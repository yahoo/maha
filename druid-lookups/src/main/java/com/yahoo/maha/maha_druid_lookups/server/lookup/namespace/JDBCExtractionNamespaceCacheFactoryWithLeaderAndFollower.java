// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.DefaultMapper;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.Callable;

/**
 *
 */
public class JDBCExtractionNamespaceCacheFactoryWithLeaderAndFollower
        extends JDBCExtractionNamespaceCacheFactory {
    private static final Logger LOG = new Logger(JDBCExtractionNamespaceCacheFactoryWithLeaderAndFollower.class);
    private static final String COMMA_SEPARATOR = ",";


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
        LOG.info("Calling Leader/Follower populator with variables: " +
                "id=" + id + ", namespace=" + extractionNamespace.toString() + ", lastVers=" + lastVersion + ", cache=" + (Objects.nonNull(cache) ? cache : "NULL"));

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
        if (!extractionNamespace.isCacheEnabled() && !extractionNamespace.getIsLeader()) {
            return nonCacheEnabledCall(lastCheck);
        }
        final Timestamp lastDBUpdate = lastUpdates(id, extractionNamespace);
        if (Objects.nonNull(lastDBUpdate) && lastDBUpdate.getTime() <= lastCheck) {
            return new Callable<String>() {
                @Override
                public String call() {
                    extractionNamespace.setPreviousLastUpdateTimestamp(lastDBUpdate);
                    return lastVersion;
                }
            };
        }
        if(extractionNamespace.getIsLeader()) {
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

        return new Callable<String>() {
            @Override
            public String call() {
                final DBI dbi = ensureDBI(id, extractionNamespace);

                LOG.debug("Updating [%s]", id);

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

                                populateRowListFromJDBC(extractionNamespace, query, lastDBUpdate, handle, new KafkaRowMapper(extractionNamespace, cache));
                                return null;
                            }
                        }
                );

                LOG.info("Leader finished loading %d values for extractionNamespace [%s]", cache.size(), id);
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
        ConsumerRecords<String, byte[]> records =  kafkaConsumer.poll(10000);

        int i = 0;
        for(ConsumerRecord<String, byte[]> record : records) {
            final String key = record.key();
            final byte[] message = record.value();

            if (key == null || message == null) {
                LOG.error("Bad key/message from topic [%s], skipping record.", kafkaProducerTopic);
                continue;
            }

            updateLocalCache(extractionNamespace, cache, message);
            ++i;
        }

        LOG.error("Follower operation num records returned [%d]: " + i);
        return null;
            }
        };

    }

    /**
     * Parse the received message into the local cache.
     * @param extractionNamespace
     * @param cache
     * @param value
     */
    public void updateLocalCache(final JDBCExtractionNamespace extractionNamespace, Map<String, List<String>> cache,
                            final byte[] value) {

        try {
            String keyColumn = "";
            String allColumnsString = new String(value);
            String[] allColumnArray = allColumnsString.split(", ");
            List<String> newValue = new ArrayList<>();
            for(String str: allColumnArray) {
                String[] columnKvPair = str.split("=");
                boolean isInvalidKVPair = columnKvPair.length < 2;
                String columnName = columnKvPair[0];
                String columnValue = "";
                boolean isTSColumn = columnName.equals(extractionNamespace.getTsColumn());
                boolean isPkColumn = columnName.equals(extractionNamespace.getPrimaryKeyColumn());

                if(isInvalidKVPair) {
                    LOG.error("Record passed in null value for column " + columnKvPair);
                    newValue.add(columnValue);
                }
                else {
                    columnValue = columnKvPair[1];
                    newValue.add(columnValue);
                }

                if(isTSColumn && Timestamp.valueOf(columnValue).after(extractionNamespace.getPreviousLastUpdateTimestamp())) {
                    LOG.debug("Updating last update TS for cache to [%s]", columnValue );
                    extractionNamespace.setPreviousLastUpdateTimestamp(Timestamp.valueOf(columnValue));
                }

                if(isPkColumn)
                    keyColumn = columnKvPair[1];

            }
            LOG.info(newValue.toString());
            cache.put(keyColumn, newValue);

        } catch (Exception e) {
            LOG.error(e.toString());
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
    }
}
