// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespaceWithLeaderAndFollower;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.KafkaRowMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.sql.Timestamp;
import java.time.Period;
import java.util.*;
import java.util.concurrent.Callable;

/**
 *
 */
public class JDBCExtractionNamespaceCacheFactoryWithLeaderAndFollower
        extends JDBCExtractionNamespaceCacheFactory {
    private static final Logger LOG = new Logger(JDBCExtractionNamespaceCacheFactoryWithLeaderAndFollower.class);
    private static final String COMMA_SEPARATOR = ",";
    private boolean cancelled = false;


    private Producer<String, byte[]> kafkaProducer;
    private Consumer<String, byte[]> kafkaConsumer;

    private Properties kafkaProperties;

    @Inject
    LookupService lookupService;
    @Inject
    ServiceEmitter emitter;


    /**
     * Populate cache or write to Kafka topic.  Validates Kafka and protobuf.
     * @param id
     * @param extractionNamespace
     * @param lastVersion
     * @param cache
     * @return
     */
    @Override
    public Callable<String> getCachePopulator(
            final String id,
            final JDBCExtractionNamespace extractionNamespace,
            final String lastVersion,
            final Map<String, List<String>> cache
    ) {
        LOG.info("Calling Leader or Follower populator with variables: " +
                "id=" + id + ", namespace=" + extractionNamespace.toString() + ", lastVers=" + lastVersion);

        return getCachePopulator(id, (JDBCExtractionNamespaceWithLeaderAndFollower)extractionNamespace, lastVersion, cache);
    }

    /**
     * Populate cache or write to Kafka topic.
     * @param id
     * @param extractionNamespace
     * @param lastVersion
     * @param cache
     * @return
     */
    public Callable<String> getCachePopulator(
            final String id,
            final JDBCExtractionNamespaceWithLeaderAndFollower extractionNamespace,
            final String lastVersion,
            final Map<String, List<String>> cache
    ) {
        kafkaProperties = extractionNamespace.getKafkaProperties();

        Objects.requireNonNull(kafkaProperties, "Must first define kafkaProperties to create a JDBC -> Kafka link.");
        final long lastCheck = lastVersion == null ? Long.MIN_VALUE / 2 : Long.parseLong(lastVersion);
        if (!extractionNamespace.isCacheEnabled()) {
            return nonCacheEnabledCall(lastCheck);
        }
        final Timestamp lastDBUpdate = lastUpdates(id, extractionNamespace, !extractionNamespace.getIsLeader());
        if (Objects.nonNull(lastDBUpdate) && lastDBUpdate.getTime() <= lastCheck) {
            return new Callable<String>() {
                @Override
                public String call() {
                    extractionNamespace.setPreviousLastUpdateTimestamp(lastDBUpdate);
                    return lastVersion;
                }
            };
        }
        if(extractionNamespace.isFirstTimeCaching()) {
            return super.getCachePopulator(id, extractionNamespace, lastVersion, cache);
        }else if(extractionNamespace.getIsLeader()) {
            return doLeaderOperations(id, extractionNamespace, cache, kafkaProperties, lastDBUpdate);
        } else {
            return doFollowerOperations(id, extractionNamespace, cache, kafkaProperties);
        }
    }

    /**
     * Use the active JDBC to populate a rowList & send it to the open Kafka topic.
     * @param id
     * @param extractionNamespace
     * @param cache
     * @param kafkaProperties
     * @param lastDBUpdate
     * @return
     */
    public Callable<String> doLeaderOperations(final String id,
                                               final JDBCExtractionNamespaceWithLeaderAndFollower extractionNamespace,
                                               final Map<String, List<String>> cache,
                                               final Properties kafkaProperties,
                                               final Timestamp lastDBUpdate) {
        LOG.info("Running Kafka Leader - Producer actions on %s.", id);
        kafkaProducer = ensureKafkaProducer(kafkaProperties);

        return new Callable<String>() {
            @Override
            public String call() {
                final DBI dbi = ensureDBI(id, extractionNamespace);

                LOG.debug("Updating [%s]", id);

                KafkaRowMapper mapper = new KafkaRowMapper(extractionNamespace, cache, kafkaProducer);
                //Call Oracle through JDBC connection
                dbi.withHandle(
                        new HandleCallback<Void>() {
                            @Override
                            public Void withHandle(Handle handle) {
                                String query =
                                        String.format(
                                                "SELECT %s FROM %s",
                                                String.join(COMMA_SEPARATOR, extractionNamespace.getColumnList()),
                                                extractionNamespace.getTable()
                                        );

                                populateRowListFromJDBC(extractionNamespace, query, lastDBUpdate, handle, mapper);
                                return null;
                            }
                        }
                );

                LOG.info("Leader finished loading %d values giving final cache size of [%d] for extractionNamespace [%s]", mapper.getNumRecordsReturned(), cache.size(), id);
                extractionNamespace.setPreviousLastUpdateTimestamp(lastDBUpdate);
                return String.format("%d", lastDBUpdate.getTime());
            }
        };


    }

    /**
     * Poll the Kafka topic & populate the local cache.
     * @param id
     * @param extractionNamespace
     * @param cache
     * @param kafkaProperties
     * @return
     */
    public Callable<String> doFollowerOperations(final String id,
                                                 final JDBCExtractionNamespaceWithLeaderAndFollower extractionNamespace,
                                                 final Map<String, List<String>> cache,
                                                 final Properties kafkaProperties) {
        return new Callable<String>() {
            @Override
            public String call() {
                LOG.info("Running Kafka Follower - Consumer actions on %s.", id);
                String kafkaProducerTopic = extractionNamespace.getKafkaTopic();
                kafkaConsumer = ensureKafkaConsumer(kafkaProperties);
                        kafkaConsumer.subscribe(Collections.singletonList(kafkaProducerTopic));
                long consumerPollPeriod = extractionNamespace.getPollMs();

                Tuple2<Integer, Timestamp> runRowsWithTS = pollKafkaTopicForUpdates(kafkaConsumer, consumerPollPeriod, kafkaProducerTopic, extractionNamespace, cache);
                Integer totalNumRowsUpdated = runRowsWithTS._1;
                Timestamp polledLastUpdatedTS = runRowsWithTS._2;

                populateLastUpdatedTime(polledLastUpdatedTS, extractionNamespace);

                LOG.info("Follower operation num records returned [%d] with final cache size of [%d]: ", totalNumRowsUpdated, cache.size());
                long lastUpdatedTS = Objects.nonNull(extractionNamespace.getPreviousLastUpdateTimestamp()) ? extractionNamespace.getPreviousLastUpdateTimestamp().getTime() : 0L;
                return String.format("%d", lastUpdatedTS);
            }
        };

    }

    private Tuple2<Integer, Timestamp> pollKafkaTopicForUpdates(Consumer<String, byte[]> kafkaConsumer,
                                                               long consumerPollPeriod,
                                                               String kafkaProducerTopic,
                                                               final JDBCExtractionNamespaceWithLeaderAndFollower extractionNamespace,
                                                               final Map<String, List<String>> cache
                                                               ) {
        long halfPollPeriod = consumerPollPeriod/2;
        long tenPercentPollPeriod = consumerPollPeriod/10;

        long endTime = new DateTime().getMillis() + (halfPollPeriod);

        int i = 0;
        Timestamp latestTSFromRows = new Timestamp(0L);

        while(new DateTime().getMillis() < endTime && !cancelled) {
            ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(tenPercentPollPeriod);
            for (ConsumerRecord<String, byte[]> record : records) {
                final String key = record.key();
                final byte[] message = record.value();

                if (key == null || message == null) {
                    LOG.error("Bad key/message from topic [%s], skipping record.", kafkaProducerTopic);
                    continue;
                }

                //Return the latest updated TS to load back into the namespace.
                Timestamp singleRowUpdateTS = updateLocalCache(extractionNamespace, cache, message);

                if (singleRowUpdateTS.after(latestTSFromRows))
                    latestTSFromRows = singleRowUpdateTS;

                ++i;
            }
        }

        return new Tuple2<>(i, latestTSFromRows);
    }

    /**
     *
     * @param lastUpdatedTS
     * @param extractionNamespace
     */
    public void populateLastUpdatedTime(Timestamp lastUpdatedTS,
                                        JDBCExtractionNamespace extractionNamespace) {
        if (!Objects.nonNull(extractionNamespace.getPreviousLastUpdateTimestamp())) {
            LOG.info("Setting last updated TS as current value.");
            extractionNamespace.setPreviousLastUpdateTimestamp(lastUpdatedTS);
        } else if (Objects.nonNull(extractionNamespace.getPreviousLastUpdateTimestamp()) &&
                extractionNamespace.getPreviousLastUpdateTimestamp().before(lastUpdatedTS)) {
            extractionNamespace.setPreviousLastUpdateTimestamp(lastUpdatedTS);
        } else {
            //No-Op, current TS in the namespace is newer than the current record.
        }
    }

    /**
     * Parse the received message into the local cache.
     * Return the current row's last updated TS.
     * Cache is only updated if the record is new (in parent)
     * @param extractionNamespace
     * @param cache
     * @param value
     */
    public Timestamp updateLocalCache(final JDBCExtractionNamespace extractionNamespace, Map<String, List<String>> cache,
                            final byte[] value) {

        try {
            String keyColname = extractionNamespace.getPrimaryKeyColumn();
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(value));
            Map<String, Object> allColumnsMap = (Map<String, Object>)ois.readObject();
            String pkValue = allColumnsMap.getOrDefault(keyColname, null).toString();

            List<String> columnsInOrder = new ArrayList<>();
            Long rowTS = 0L;
            for(String str: extractionNamespace.getColumnList()) {
                Object retVal = allColumnsMap.getOrDefault(str, "");
                columnsInOrder.add(String.valueOf(retVal));
                boolean isTS = Objects.nonNull(retVal) && str.equals(extractionNamespace.getTsColumn());

                if(isTS) {
                    rowTS = Timestamp.valueOf(retVal.toString()).getTime();
                }
            }

            if(Objects.nonNull(pkValue) && !Objects.nonNull(cache.get(pkValue))) {
                cache.put(pkValue, columnsInOrder);
            } else {
                List<String> cachedRow = cache.get(pkValue);
                Long cachedLastUpdateTS = Timestamp.valueOf((cachedRow.get(cachedRow.size()-1))).getTime();
                if(cachedLastUpdateTS < rowTS) {
                    cache.put(pkValue, columnsInOrder);
                } else {
                    LOG.error("No Valid Primary Key parsed for column (or old record passed).  Refusing to update.  Failed row is: %s", columnsInOrder);
                }
            }

            return new Timestamp(rowTS);

        } catch (Exception e) {
            LOG.error("Updating cache caused exception (Check column names): " + e.toString() + "\n", e);
            return new Timestamp(0L);
        }

    }

    /**
     * If the follower is cache-disabled, don't update it.
     * @param lastCheck
     * @return
     */
    private Callable<String> nonCacheEnabledCall(long lastCheck) {
        return () -> String.valueOf(lastCheck);
    }

    /**
     * Safe KafkaProducer create/call.
     * @param kafkaProperties
     * @return
     */
    synchronized Producer<String, byte[]> ensureKafkaProducer(Properties kafkaProperties) {

        if(kafkaProducer == null) {
            kafkaProducer = new KafkaProducer<>(kafkaProperties, new StringSerializer(), new ByteArraySerializer());
        }
        return kafkaProducer;
    }

    /**
     * Safe KafkaConsumer create/call.
     * @param kafkaProperties
     * @return
     */
    synchronized Consumer<String, byte[]> ensureKafkaConsumer(Properties kafkaProperties) {
        if(kafkaConsumer == null) {
            kafkaConsumer = new KafkaConsumer<>(kafkaProperties, new StringDeserializer(), new ByteArrayDeserializer());
        }
        return kafkaConsumer;
    }

    /**
     * End leader || follower actions on the current node.
     */
    public void stop() {
        if(kafkaProducer != null) {
            kafkaProducer.flush();
            kafkaProducer.close();
        }

        if(kafkaConsumer != null) {
            kafkaConsumer.unsubscribe();
            kafkaConsumer.close();
        }

        cancelled = true;
    }
}
