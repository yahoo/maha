// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.MongoExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.MongoStorageConnectorConfig;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.LookupBuilder;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.MongoDocumentProcessor;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.bson.Document;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 */
public class MongoExtractionNamespaceCacheFactory
        implements ExtractionNamespaceCacheFactory<MongoExtractionNamespace, List<String>> {
    private static final DateTimeFormatter ISODATE_FORMATTER =
            DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
    private static final int[] BACKOFF_MILLIS = new int[]{100, 200, 400, 800, 1600, 3200, 6400, 12800};
    private static final Logger LOG = new Logger(MongoExtractionNamespaceCacheFactory.class);
    private static final String ID_FIELD = "_id";
    private final ConcurrentMap<String, MongoClient> mongoClientCache = new ConcurrentHashMap<>();
    @Inject
    LookupService lookupService;
    @Inject
    ServiceEmitter emitter;

    @Override
    public Callable<String> getCachePopulator(
            final String id,
            final MongoExtractionNamespace extractionNamespace,
            final String lastVersion,
            final Map<String, List<String>> cache
    ) {
        final long lastCheck = lastVersion == null ? Long.MIN_VALUE / 2 : Long.parseLong(lastVersion);
        if (!extractionNamespace.isCacheEnabled()) {
            return new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return String.valueOf(lastCheck);
                }
            };
        }

        return new Callable<String>() {
            @Override
            public String call() {
                long startMillis = System.currentTimeMillis();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Updating [%s]", id);
                }
                MongoStorageConnectorConfig config = extractionNamespace.getConnectorConfig();
                int numAttempts = 0;
                MongoDocumentProcessor processor = extractionNamespace.getDocumentProcessor();
                Set<String> neededFields = new TreeSet<>();
                neededFields.add(ID_FIELD);
                neededFields.add(extractionNamespace.getTsColumn());
                neededFields.add(processor.getPrimaryKeyColumn());
                neededFields.addAll(processor.getColumnList());

                Date currentDate = new Date();
                long maxTime = -1;
                LookupBuilder lookupBuilder = new LookupBuilder(cache);
                FindIterable<Document> documents;

                MongoDatabase database = null;
                MongoCollection<Document> collection = null;
                while (numAttempts < extractionNamespace.getMongoClientRetryCount()) {
                    try {
                        final MongoClient mongoClient = ensureMongoClient(id, extractionNamespace);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Attempting to get database with config : %s"
                                    , config);
                        }
                        database = mongoClient.getDatabase(config.getDbName());
                        LOG.info("Successfully got database with hosts=%s database=%s"
                                , config.getHosts(),
                                config.getDbName());
                        collection = database.getCollection(extractionNamespace.getCollectionName());
                        LOG.info("Successfully got collection : %s"
                                , extractionNamespace.getCollectionName());

                        break;
                    } catch (Exception e) {
                        emitter.emit(ServiceMetricEvent.builder()
                                .setDimension(MonitoringConstants.MAHA_LOOKUP_NAME, extractionNamespace.getLookupName())
                                .build(MonitoringConstants.MAHA_LOOKUP_MONGO_DATABASE_OR_COLLECTION_FAILURE, 1));
                        LOG.error(e, "Failed to get database or collection, numAttempt=%s hosts=%s database=%s"
                                , numAttempts
                                , extractionNamespace.getConnectorConfig().getHosts()
                                , extractionNamespace.getConnectorConfig().getDbName()
                        );
                        backOffSleep(numAttempts);
                    }
                    numAttempts++;
                }

                if (database == null || collection == null) {
                    throw new RuntimeException(String.format("Failed to get database or collection from mongo client: hosts=%s database=%s collection=%s",
                            config.getHosts(), config.getDbName(), extractionNamespace.getCollectionName()));
                }

                if (extractionNamespace.isFirstTimeCaching()) {
                    documents = collection.find();
                } else {
                    if (extractionNamespace.isTsColumnEpochInteger()) {
                        documents = collection.find(Filters.gte(extractionNamespace.getTsColumn(), extractionNamespace.getPreviousLastUpdateTime()));
                    } else {
                        Date date = new Date(extractionNamespace.getPreviousLastUpdateTime());
                        documents = collection.find(Filters.gte(extractionNamespace.getTsColumn(), date));
                    }
                }
                documents = documents.projection(Projections.include(Lists.newArrayList(neededFields)));
                long docTime = -1;
                int count = 0;
                for (Document d : documents) {
                    try {
                        if (extractionNamespace.isTsColumnEpochInteger()) {
                            docTime = d.getInteger(extractionNamespace.getTsColumn());
                        } else {
                            docTime = d.getDate(extractionNamespace.getTsColumn()).getTime();
                        }
                        try {
                            count += processor.process(d, lookupBuilder);
                            if (maxTime < docTime) {
                                maxTime = docTime;
                            }
                        } catch (Exception e) {
                            emitter.emit(ServiceMetricEvent.builder()
                                    .setDimension(MonitoringConstants.MAHA_LOOKUP_NAME, extractionNamespace.getLookupName())
                                    .build(MonitoringConstants.MAHA_LOOKUP_MONGO_DOCUMENT_PROCESS_FAILURE, 1));
                            LOG.error(e, "collectionName=%s tsColumn=%s failed to process document document=%s"
                                    , extractionNamespace.getCollectionName()
                                    , extractionNamespace.getTsColumn()
                                    , d.toJson()
                            );
                        }

                    } catch (Exception e) {
                        emitter.emit(ServiceMetricEvent.builder()
                                .setDimension(MonitoringConstants.MAHA_LOOKUP_NAME, extractionNamespace.getLookupName())
                                .build(MonitoringConstants.MAHA_LOOKUP_MONGO_DOCUMENT_PROCESS_FAILURE, 1));
                        LOG.error(e, "collectionName=%s tsColumn=%s failed to process document document=%s"
                                , extractionNamespace.getCollectionName()
                                , extractionNamespace.getTsColumn()
                                , d.toJson()
                        );
                    }

                }

                if (extractionNamespace.isTsColumnEpochInteger()) {
                    if (maxTime < 0 || currentDate.before(new Date(maxTime * 1000))) {
                        extractionNamespace.setPreviousLastUpdateTime(currentDate.getTime() / 1000);
                    } else {
                        extractionNamespace.setPreviousLastUpdateTime(maxTime);
                    }
                } else {
                    if (maxTime < 0 || currentDate.before(new Date(maxTime))) {
                        extractionNamespace.setPreviousLastUpdateTime(currentDate.getTime());
                    } else {
                        extractionNamespace.setPreviousLastUpdateTime(maxTime);
                    }

                }

                emitter.emit(ServiceMetricEvent.builder()
                        .setDimension(MonitoringConstants.MAHA_LOOKUP_NAME, extractionNamespace.getLookupName())
                        .build(MonitoringConstants.MAHA_LOOKUP_MONGO_PROCESSING_TIME, System.currentTimeMillis() - startMillis));
                LOG.info("Finished loading %d values for extractionNamespace[%s] with %d new/updated entries", cache.size(), id, count);
                return String.format("%d", extractionNamespace.getPreviousLastUpdateTime());
            }
        };
    }

    private MongoClient ensureMongoClient(String id, MongoExtractionNamespace namespace) {
        final String key = id;
        MongoClient mongoClient = null;
        if (mongoClientCache.containsKey(key)) {
            MongoClient aMongoClient = mongoClientCache.get(key);
            synchronized (aMongoClient) {
                if (mongoClientCache.containsKey(key)) {
                    HashSet<ServerAddress> serverAddressList = new HashSet<>(aMongoClient.getServerAddressList());
                    HashSet<ServerAddress> currentAddressList = new HashSet<>(namespace.getConnectorConfig().getServerAddressList());
                    if (serverAddressList.equals(currentAddressList)) {
                        mongoClient = aMongoClient;
                        LOG.info("Using existing mongo client for namespace : %s", id);
                    } else {
                        LOG.info("Removing stale mongo client for namespace : %s", id);
                        mongoClientCache.remove(key);
                        aMongoClient.close();
                    }
                }
            }
        }
        if (mongoClient == null) {
            int numAttempts = 0;
            while (numAttempts < namespace.getMongoClientRetryCount()) {
                try {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Attempting to create mongo client with config : %s"
                                , namespace.getConnectorConfig());
                    }
                    final MongoClient newClient = namespace.getConnectorConfig().getMongoClient();
                    LOG.info("Successfully created mongo client with hosts=%s database=%s"
                            , namespace.getConnectorConfig().getHosts(),
                            namespace.getConnectorConfig().getDbName());
                    mongoClientCache.putIfAbsent(key, newClient);
                    mongoClient = mongoClientCache.get(key);
                    if (newClient != mongoClient) {
                        newClient.close();
                    }
                    break;
                } catch (Exception e) {
                    emitter.emit(ServiceMetricEvent.builder()
                            .setDimension(MonitoringConstants.MAHA_LOOKUP_NAME, namespace.getLookupName())
                            .build(MonitoringConstants.MAHA_LOOKUP_MONGO_CLIENT_FAILURE, 1));
                    LOG.error(e, "Failed to create mongo client, numAttempt=%s hosts=%s database=%s"
                            , numAttempts
                            , namespace.getConnectorConfig().getHosts()
                            , namespace.getConnectorConfig().getDbName()
                    );
                    backOffSleep(numAttempts);
                }
                numAttempts++;
            }
        }
        return mongoClient;
    }

    private void backOffSleep(int numAttempts) {
        try {
            int sleepMillis = BACKOFF_MILLIS[numAttempts % BACKOFF_MILLIS.length];
            Thread.sleep(sleepMillis);
        } catch (Exception e) {
            LOG.error(e, "Error while sleeping");
        }
    }

    @Override
    public void updateCache(final MongoExtractionNamespace extractionNamespace, final Map<String, List<String>> cache,
                            final String key, final byte[] value) {
        //No-Op
    }

    @Override
    public byte[] getCacheValue(final MongoExtractionNamespace extractionNamespace, final Map<String, List<String>> cache, final String key, final String valueColumn, final Optional<DecodeConfig> decodeConfigOptional) {
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

    private byte[] handleDecode(MongoExtractionNamespace extractionNamespace, List<String> cacheValue, DecodeConfig decodeConfig) {

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
    public String getCacheSize(final MongoExtractionNamespace extractionNamespace, final Map<String, List<String>> cache) {
        if (!extractionNamespace.isCacheEnabled()) {
            return String.valueOf(lookupService.getSize());
        }
        return String.valueOf(cache.size());
    }

    @Override
    public Long getLastUpdatedTime(final MongoExtractionNamespace extractionNamespace) {
        if (!extractionNamespace.isCacheEnabled()) {
            return lookupService.getLastUpdatedTime(new LookupService.LookupData(extractionNamespace));
        }
        return extractionNamespace.getPreviousLastUpdateTime();
    }
}
