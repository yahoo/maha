package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.query.lookup.util.LookupUtil;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.KafkaManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.MonitoringConstants;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.RocksDBManager;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.rocksdb.RocksDB;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class BaseRocksDBLookupExtractor<U> extends MahaLookupExtractor {
    private static final Logger LOG = new Logger(BaseRocksDBLookupExtractor.class);

    protected static final ObjectMapper objectMapper = JsonMapper.builder().addModule(new JodaModule()).build();
    private final Map<String, U> map;
    protected final RocksDBExtractionNamespace extractionNamespace;
    protected RocksDBManager rocksDBManager;
    protected LookupService lookupService;
    protected KafkaManager kafkaManager;
    protected ServiceEmitter serviceEmitter;
    protected Cache<String, byte[]> missingLookupCache;
    protected final byte[] extractionNamespaceAsByteArray;
    private final LookupUtil util = new LookupUtil();

    public BaseRocksDBLookupExtractor(RocksDBExtractionNamespace extractionNamespace, Map<String, U> map,
                                   LookupService lookupService, RocksDBManager rocksDBManager, KafkaManager kafkaManager,
                                   ServiceEmitter serviceEmitter) {
        this.map = Preconditions.checkNotNull(map, "map");
        this.extractionNamespace = extractionNamespace;
        this.rocksDBManager = rocksDBManager;
        this.kafkaManager = kafkaManager;
        this.lookupService = lookupService;
        this.serviceEmitter = serviceEmitter;
        this.missingLookupCache = Caffeine
                .newBuilder()
                .maximumSize(10_000)
                .expireAfterWrite(1, TimeUnit.HOURS)
                .build();
        try {
            objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            this.extractionNamespaceAsByteArray = objectMapper.writeValueAsBytes(extractionNamespace);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }

    }

    public abstract byte[] getCacheByteValue(String key, String valueColumn, Optional<DecodeConfig> decodeConfigOptional, RocksDB db);

    public Map<String, U> getMap() {
        return ImmutableMap.copyOf(map);
    }

    @Override
    public List<String> unapply(final String value) {
        return Lists.newArrayList(Maps.filterKeys(map, new Predicate<String>() {
            @Override
            public boolean apply(@Nullable String key) {
                return map.get(key).equals(Strings.nullToEmpty(value));
            }
        }).keySet());

    }

    @Override
    public byte[] getCacheKey() {
        try {
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            outputStream.write(extractionNamespace.toString().getBytes());
            outputStream.write((byte) 0xFF);
            return outputStream.toByteArray();
        } catch (IOException ex) {
            // If ByteArrayOutputStream.write has problems, that is a very bad thing
            throw Throwables.propagate(ex);
        }
    }

    @Nullable
    public String apply(@NotNull String key, @NotNull String valueColumn, DecodeConfig decodeConfig, Map<String, String> dimensionOverrideMap, Map<String, String> secondaryColOverrideMap) {
        try {
            util.overrideThrowableCheck(dimensionOverrideMap, secondaryColOverrideMap);

            if (key == null) {
                return null;
            }

            if (dimensionOverrideMap != null && dimensionOverrideMap.containsKey(key)) {
                return Strings.emptyToNull(dimensionOverrideMap.get(key));
            }

            String cacheableByteString;
            Optional<DecodeConfig> decodeConfigOptional = (decodeConfig == null) ? Optional.empty() : Optional.of(decodeConfig);
            if (!extractionNamespace.isCacheEnabled()) {
                byte[] cacheByteValue = lookupService.lookup(new LookupService.LookupData(extractionNamespace,
                        key, valueColumn, decodeConfigOptional));
                String cacheByteString = (cacheByteValue == null || cacheByteValue.length == 0) ? null : new String(cacheByteValue, UTF_8).trim();
                cacheableByteString = util.populateCacheStringFromOverride(cacheByteString, secondaryColOverrideMap);
            } else {
                final RocksDB db = rocksDBManager.getDB(extractionNamespace.getNamespace());
                if (db == null) {
                    LOG.error("RocksDB instance is null");
                    LOG.error("Failed to get lookup value from cache. Falling back to lookupService.");
                    serviceEmitter.emit(ServiceMetricEvent.builder().setMetric(MonitoringConstants.MAHA_LOOKUP_GET_CACHE_VALUE_FAILURE + "_" + extractionNamespace.getNamespace(), 1));
                    String cacheByteString = new String(lookupService.lookup(new LookupService.LookupData(extractionNamespace, key, valueColumn, decodeConfigOptional))).trim();
                    return util.populateCacheStringFromOverride(cacheByteString, secondaryColOverrideMap);
                }
                byte[] cacheByteValue = getCacheByteValue(key, valueColumn, decodeConfigOptional, db);

                if (cacheByteValue == null || cacheByteValue.length == 0) {
                    // No need to call handleMissingLookup if missing dimension is already present in missingLookupCache
                    if (extractionNamespace.getMissingLookupConfig() != null
                            && !Strings.isNullOrEmpty(extractionNamespace.getMissingLookupConfig().getMissingLookupKafkaTopic())
                            && missingLookupCache.getIfPresent(key) == null) {

                        kafkaManager.handleMissingLookup(extractionNamespaceAsByteArray,
                                extractionNamespace.getMissingLookupConfig().getMissingLookupKafkaTopic(),
                                key);
                        missingLookupCache.put(key, extractionNamespaceAsByteArray);
                        serviceEmitter.emit(ServiceMetricEvent.builder().setMetric(MonitoringConstants.MAHA_LOOKUP_PUBLISH_MISSING_LOOKUP_SUCCESS, Integer.valueOf(1)));
                    }
                    return null;
                } else {
                    String cacheByteString = new String(cacheByteValue).trim();
                    cacheableByteString = util.populateCacheStringFromOverride(cacheByteString, secondaryColOverrideMap);
                }
            }

            return cacheableByteString;

        } catch (Exception e) {
            LOG.error(e, "Caught exception while lookup");
            return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BaseRocksDBLookupExtractor that = (BaseRocksDBLookupExtractor) o;

        return map.equals(that.map);
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

}
