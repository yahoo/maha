// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.google.inject.Inject;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCProducerExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.ProtobufSchemaFactory;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.RowMapper;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.DefaultMapper;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.TimestampMapper;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 *
 */
public class JDBCProducerExtractionNamespaceCacheFactory
        implements ExtractionNamespaceCacheFactory<JDBCProducerExtractionNamespace, List<String>> {
    private static final Logger LOG = new Logger(JDBCProducerExtractionNamespaceCacheFactory.class);
    private static final String COMMA_SEPARATOR = ",";
    private static final String FIRST_TIME_CACHING_WHERE_CLAUSE = " WHERE LAST_UPDATED <= :lastUpdatedTimeStamp";
    private static final String SUBSEQUENT_CACHING_WHERE_CLAUSE = " WHERE LAST_UPDATED > :lastUpdatedTimeStamp";
    private static final int FETCH_SIZE = 10000;
    private final ConcurrentMap<String, DBI> dbiCache = new ConcurrentHashMap<>();

    private KafkaProducer<String, byte[]> kafkaProducer = null;
    private KafkaConsumer<String, byte[]> kafkaConsumer = null;

    private Properties kafkaProperties;

    private ProtobufSchemaFactory protobufSchemaFactory;

    @Inject
    LookupService lookupService;
    @Inject
    ServiceEmitter emitter;

    @Override
    public Callable<String> getCachePopulator(
            final String id,
            final JDBCProducerExtractionNamespace extractionNamespace,
            final String lastVersion,
            final Map<String, List<String>> cache
    ) {
        Objects.requireNonNull(kafkaProperties, "Must first define kafkaProperties to create a JDBC -> Kafka link.");
        Objects.requireNonNull(protobufSchemaFactory, "Kafka needs a Protobuf for the JDBC input.");
        return getCachePopulator(id, extractionNamespace, lastVersion, cache, kafkaProperties, protobufSchemaFactory);
    }

    public void setKafkaProperties(Properties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    public void setProtobufSchemaFactory(ProtobufSchemaFactory protobufSchemaFactory) {
        this.protobufSchemaFactory = protobufSchemaFactory;
    }

    public Callable<String> getCachePopulator(
            final String id,
            final JDBCProducerExtractionNamespace extractionNamespace,
            final String lastVersion,
            final Map<String, List<String>> cache,
            final Properties kafkaProperties,
            final ProtobufSchemaFactory protobufSchemaFactory
    ) {
        final long lastCheck = lastVersion == null ? Long.MIN_VALUE / 2 : Long.parseLong(lastVersion);
        if (!extractionNamespace.isCacheEnabled()) {
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
            return doLeaderOperations(id, extractionNamespace, lastVersion, cache, kafkaProperties, protobufSchemaFactory, lastDBUpdate);
        } else {
            return doFollowerOperations(id, extractionNamespace, lastVersion, cache, kafkaProperties, protobufSchemaFactory);
        }/*
        return new Callable<String>() {
            @Override
            public String call() {
                final DBI dbi = ensureDBI(id, extractionNamespace);

                LOG.debug("Updating [%s]", id);

                if(extractionNamespace.getIsLeader()) {
                    doLeaderOperations(id, extractionNamespace, lastVersion, cache, kafkaProperties, protobufSchemaFactory);
                } else {
                    doFollowerOperations(id, extractionNamespace, lastVersion, cache, kafkaProperties, protobufSchemaFactory);
                }

                dbi.withHandle(
                        new HandleCallback<Void>() {
                            @Override
                            public Void withHandle(Handle handle) throws Exception {

                                Descriptors.Descriptor descriptor = protobufSchemaFactory.getProtobufDescriptor(extractionNamespace.getLookupName());
                                Message.Builder messageBuilder = protobufSchemaFactory.getProtobufMessageBuilder(extractionNamespace.getLookupName());
                                Map<String, Object> map = new HashMap<>();
                                List<Map<String,  Object>> lm;
                                String query = String.format("SELECT %s FROM %s",
                                        String.join(COMMA_SEPARATOR, extractionNamespace.getColumnList()),
                                        extractionNamespace.getTable()
                                );

                                //TODO: isFirstTimeCaching cannot be set by a leader, since it never caches and only writes to a topic.
                                //TODO: non-leaders don't make any queries and only consume, but they CAN have isFirstTimeCaching.
                                if (extractionNamespace.isFirstTimeCaching()) {
                                    extractionNamespace.setFirstTimeCaching(false);
                                    query = String.format("%s %s", query, FIRST_TIME_CACHING_WHERE_CLAUSE);
                                    handle.createQuery(query).map(
                                            new RowMapper(extractionNamespace, cache))
                                            .setFetchSize(FETCH_SIZE)
                                            .bind("lastUpdatedTimeStamp", lastDBUpdate)
                                            .list();
                                    lm = handle.createQuery(query).map(
                                            new RowMapper(extractionNamespace, cache))
                                            .setFetchSize(FETCH_SIZE)
                                            .bind("lastUpdatedTimeStamp", lastDBUpdate)
                                            .map(new DefaultMapper())
                                            .list();

                                } else {
                                    query = String.format("%s %s", query, SUBSEQUENT_CACHING_WHERE_CLAUSE);
                                    handle.createQuery(query).map(
                                            new RowMapper(extractionNamespace, cache))
                                            .setFetchSize(FETCH_SIZE)
                                            .bind("lastUpdatedTimeStamp",
                                                    extractionNamespace.getPreviousLastUpdateTimestamp())
                                            .list();
                                    lm = handle.createQuery(query).map(
                                            new RowMapper(extractionNamespace, cache))
                                            .setFetchSize(FETCH_SIZE)
                                            .bind("lastUpdatedTimeStamp", extractionNamespace.getPreviousLastUpdateTimestamp())
                                            .map(new DefaultMapper())
                                            .list();
                                }

                                if(extractionNamespace.getIsLeader() && Objects.nonNull(map)) {
                                    //Execute Kafka side of the Leader

                                    for(Map<String, Object> row: lm) {
                                        descriptor.getFields()
                                                .stream()
                                                .forEach(fd -> messageBuilder.setField(fd, String.valueOf(row.get(fd.getName()))));

                                        Message message = messageBuilder.build();
                                        LOG.info("Producing key[%s] val[%s]", extractionNamespace.getTable(), message);
                                        LOG.info("Leader mode enabled on node.  Duplicating lookup record to Kafka Topic " + producerKafkaTopic);
                                        ProducerRecord<String, byte[]> producerRecord =
                                                new ProducerRecord<>(producerKafkaTopic, extractionNamespace.getTable(), message.toByteArray());
                                        kafkaProducer.send(producerRecord);
                                    }
                                } else {
                                    LOG.info("Leader disabled on node.  Using default read/write functionality.");
                                    LOG.info("This is where the node reads from the topic to write the lookup result.");
                                }

                                return null;
                            }
                        }
                );

                LOG.info("Finished loading %d values for extractionNamespace[%s]", cache.size(), id);
                extractionNamespace.setPreviousLastUpdateTimestamp(lastDBUpdate);
                return String.format("%d", lastDBUpdate.getTime());
            }
        };*/
    }

    private List<Map<String, Object>> populateRowListFromJDBC(
            JDBCProducerExtractionNamespace extractionNamespace,
            String query,
            Map<String, List<String>> cache,
            Timestamp lastDBUpdate,
            Handle handle
    ) {
        List<Map<String, Object>> rowList;
        if (extractionNamespace.isFirstTimeCaching()) {
            extractionNamespace.setFirstTimeCaching(false);
            query = String.format("%s %s", query, FIRST_TIME_CACHING_WHERE_CLAUSE);
            rowList = handle.createQuery(query).map(
                    new RowMapper(extractionNamespace, cache))
                    .setFetchSize(FETCH_SIZE)
                    .bind("lastUpdatedTimeStamp", lastDBUpdate)
                    .map(new DefaultMapper())
                    .list();

        } else {
            query = String.format("%s %s", query, SUBSEQUENT_CACHING_WHERE_CLAUSE);
            rowList = handle.createQuery(query).map(
                    new RowMapper(extractionNamespace, cache))
                    .setFetchSize(FETCH_SIZE)
                    .bind("lastUpdatedTimeStamp", extractionNamespace.getPreviousLastUpdateTimestamp())
                    .map(new DefaultMapper())
                    .list();
        }

        return rowList;
    }

    /**
     * Use the active JDBC
     * @param id
     * @param extractionNamespace
     * @param lastVersion
     * @param cache
     * @param kafkaProperties
     * @param protobufSchemaFactory
     * @param lastDBUpdate
     * @return
     */
    public Callable<String> doLeaderOperations(final String id,
                                               final JDBCProducerExtractionNamespace extractionNamespace,
                                               final String lastVersion,
                                               final Map<String, List<String>> cache,
                                               final Properties kafkaProperties,
                                               final ProtobufSchemaFactory protobufSchemaFactory,
                                               final Timestamp lastDBUpdate) {
        LOG.info("Running Kafka Leader - Producer actions on %s.", id);
        kafkaProducer = ensureKafkaProducer(kafkaProperties);
        final String producerKafkaTopic = extractionNamespace.getKafkaTopic();

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

                                Descriptors.Descriptor descriptor = protobufSchemaFactory.getProtobufDescriptor(extractionNamespace.getLookupName());
                                Message.Builder messageBuilder = protobufSchemaFactory.getProtobufMessageBuilder(extractionNamespace.getLookupName());
                                Map<String, Object> map = new HashMap<>();
                                List<Map<String,  Object>> rowList;
                                String query =
                                        String.format(
                                                "SELECT %s FROM %s",
                                                String.join(COMMA_SEPARATOR, extractionNamespace.getColumnList()),
                                                extractionNamespace.getTable()
                                        );

                                rowList = populateRowListFromJDBC(extractionNamespace, query, cache, lastDBUpdate, handle);

                                if(Objects.nonNull(rowList)) {
                                    for(Map<String, Object> row: rowList) {
                                        descriptor.getFields()
                                                .stream()
                                                .forEach(fd -> messageBuilder.setField(fd, String.valueOf(row.get(fd.getName()))));

                                        Message message = messageBuilder.build();
                                        LOG.info("Producing key [%s] val [%s]", extractionNamespace.getTable(), message);
                                        LOG.info("Leader mode enabled on node.  Sending lookup record to Kafka Topic " + producerKafkaTopic);
                                        ProducerRecord<String, byte[]> producerRecord =
                                                new ProducerRecord<>(producerKafkaTopic, extractionNamespace.getTable(), message.toByteArray());
                                        kafkaProducer.send(producerRecord);
                                    }
                                } else {
                                    LOG.info("No query results to return.");
                                }

                                return null;
                            }
                        }
                );

                LOG.info("Finished loading %d values for extractionNamespace[%s]", cache.size(), id);
                extractionNamespace.setPreviousLastUpdateTimestamp(lastDBUpdate);
                return String.format("%d", lastDBUpdate.getTime());
            }
        };


    }

    public Callable<String> doFollowerOperations(final String id,
                                                 final JDBCProducerExtractionNamespace extractionNamespace,
                                                 final String lastVersion,
                                                 final Map<String, List<String>> cache,
                                                 final Properties kafkaProperties,
                                                 final ProtobufSchemaFactory protobufSchemaFactory) {
        LOG.info("Running Kafka Follower - Consumer actions on %s.", id);
        String kafkaProducerTopic = extractionNamespace.getKafkaTopic();
        kafkaConsumer = ensureKafkaConsumer(kafkaProperties);
        kafkaConsumer.subscribe(Collections.singletonList(kafkaProducerTopic));
        kafkaConsumer.poll(10000);
        return new Callable<String>() {
            @Override
            public String call() {
                return "I like turtles.";
            }
        };

    }

    private Callable<String> nonCacheEnabledCall(long lastCheck) {
        return () -> String.valueOf(lastCheck);
    }

    synchronized KafkaProducer<String, byte[]> ensureKafkaProducer(Properties kafkaProperties) {
        if(kafkaProducer == null) {
            kafkaProducer = new KafkaProducer<>(kafkaProperties);
        }
        return kafkaProducer;
    }

    synchronized KafkaConsumer<String, byte[]> ensureKafkaConsumer(Properties kafkaProperties) {
        if(kafkaConsumer == null) {
            kafkaConsumer = new KafkaConsumer<>(kafkaProperties);
        }
        return kafkaConsumer;
    }

    public void stop() {
        if(kafkaProducer != null) {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }

    private DBI ensureDBI(String id, JDBCProducerExtractionNamespace namespace) {
        final String key = id;
        DBI dbi = null;
        if (dbiCache.containsKey(key)) {
            dbi = dbiCache.get(key);
        }
        if (dbi == null) {
            final DBI newDbi = new DBI(
                    namespace.getConnectorConfig().getConnectURI(),
                    namespace.getConnectorConfig().getUser(),
                    namespace.getConnectorConfig().getPassword()
            );
            dbiCache.putIfAbsent(key, newDbi);
            dbi = dbiCache.get(key);
        }
        return dbi;
    }

    private Timestamp lastUpdates(String id, JDBCProducerExtractionNamespace namespace) {
        final DBI dbi = ensureDBI(id, namespace);
        final String table = namespace.getTable();
        final String tsColumn = namespace.getTsColumn();
        if (tsColumn == null) {
            return null;
        }
        final Timestamp lastUpdatedTimeStamp = dbi.withHandle(
                new HandleCallback<Timestamp>() {

                    @Override
                    public Timestamp withHandle(Handle handle) throws Exception {
                        final String query = String.format(
                                "SELECT MAX(%s) FROM %s",
                                tsColumn, table
                        );
                        return handle
                                .createQuery(query)
                                .map(TimestampMapper.FIRST)
                                .first();
                    }
                }
        );
        return lastUpdatedTimeStamp;

    }

    @Override
    public void updateCache(final JDBCProducerExtractionNamespace extractionNamespace, final Map<String, List<String>> cache,
                            final String key, final byte[] value) {
        //No-Op
    }

    @Override
    public byte[] getCacheValue(final JDBCProducerExtractionNamespace extractionNamespace, final Map<String, List<String>> cache, final String key, final String valueColumn, final Optional<DecodeConfig> decodeConfigOptional) {
        if (!extractionNamespace.isCacheEnabled()) {
            byte[] value = lookupService.lookup(new LookupService.LookupData(extractionNamespace, key, valueColumn, decodeConfigOptional));
            value = (value == null) ? new byte[0] : value;
            LOG.info("Cache value [%s]", new String(value));
            return value;
        }
        List<String> cacheValue = cache.get(key);
        if (cacheValue == null) {
            return new byte[0];
        }

        if (decodeConfigOptional.isPresent()) {
            return handleDecode(extractionNamespace, cacheValue, decodeConfigOptional.get());
        }

        int index = extractionNamespace.getColumnIndex(valueColumn);
        if (index == -1) {
            LOG.error("invalid valueColumn [%s]", valueColumn);
            return new byte[0];
        }
        String value = cacheValue.get(index);
        return (value == null) ? new byte[0] : value.getBytes();
    }

    private byte[] handleDecode(JDBCProducerExtractionNamespace extractionNamespace, List<String> cacheValue, DecodeConfig decodeConfig) {

        final int columnToCheckIndex = extractionNamespace.getColumnIndex(decodeConfig.getColumnToCheck());
        if (columnToCheckIndex < 0 || columnToCheckIndex >= cacheValue.size()) {
            return new byte[0];
        }

        final String valueFromColumnToCheck = cacheValue.get(columnToCheckIndex);

        if (valueFromColumnToCheck != null && valueFromColumnToCheck.equals(decodeConfig.getValueToCheck())) {
            final int columnIfValueMatchedIndex = extractionNamespace.getColumnIndex(decodeConfig.getColumnIfValueMatched());
            if (columnIfValueMatchedIndex < 0) {
                return new byte[0];
            }
            String value = cacheValue.get(columnIfValueMatchedIndex);
            return (value == null) ? new byte[0] : value.getBytes();
        } else {
            final int columnIfValueNotMatchedIndex = extractionNamespace.getColumnIndex(decodeConfig.getColumnIfValueNotMatched());
            if (columnIfValueNotMatchedIndex < 0) {
                return new byte[0];
            }
            String value = cacheValue.get(columnIfValueNotMatchedIndex);
            return (value == null) ? new byte[0] : value.getBytes();
        }
    }

    @Override
    public String getCacheSize(final JDBCProducerExtractionNamespace extractionNamespace, final Map<String, List<String>> cache) {
        if (!extractionNamespace.isCacheEnabled()) {
            return String.valueOf(lookupService.getSize());
        }
        return String.valueOf(cache.size());
    }

    @Override
    public Long getLastUpdatedTime(final JDBCProducerExtractionNamespace extractionNamespace) {
        if (!extractionNamespace.isCacheEnabled()) {
            return lookupService.getLastUpdatedTime(new LookupService.LookupData(extractionNamespace));
        }
        return (extractionNamespace.getPreviousLastUpdateTimestamp() != null) ? extractionNamespace.getPreviousLastUpdateTimestamp().getTime() : -1L;
    }
}
