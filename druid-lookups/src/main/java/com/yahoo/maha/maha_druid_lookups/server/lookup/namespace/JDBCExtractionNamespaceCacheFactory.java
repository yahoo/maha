// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.CustomizedTimestampMapper;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.RowMapper;
import org.apache.derby.iapi.util.StringUtil;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.StringMapper;
import org.skife.jdbi.v2.util.TypedMapper;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class JDBCExtractionNamespaceCacheFactory
        implements ExtractionNamespaceCacheFactory<JDBCExtractionNamespace, List<String>> {
    private static final Logger LOG = new Logger(JDBCExtractionNamespaceCacheFactory.class);
    private static final String COMMA_SEPARATOR = ",";
    private static final String FIRST_TIME_CACHING_WHERE_CLAUSE = " WHERE %s <= %s";
    private static final String WHERE_CLAUSE_EXTENSION = " AND %s %s %s";
    private static final String SUBSEQUENT_CACHING_WHERE_CLAUSE = " WHERE %s > %s";
    private static final String LAST_UPDATED_TIMESTAMP = ":lastUpdatedTimeStamp";
    private static final int FETCH_SIZE = 10000;
    private final ConcurrentMap<String, DBI> dbiCache = new ConcurrentHashMap<>();
    @Inject
    LookupService lookupService;
    @Inject
    ServiceEmitter emitter;

    @Override
    public Callable<String> getCachePopulator(
            final String id,
            final JDBCExtractionNamespace extractionNamespace,
            final String lastVersion,
            final Map<String, List<String>> cache
    ) {
        final long lastCheck = lastVersion == null ? Long.MIN_VALUE / 2 : Long.parseLong(lastVersion);
        if (!extractionNamespace.isCacheEnabled()) {
            return () -> String.valueOf(lastCheck);
        }
        final Timestamp lastDBUpdate = lastUpdates(id, extractionNamespace, false);
        if (lastDBUpdate != null && lastDBUpdate.getTime() <= lastCheck) {
            return () -> {
                extractionNamespace.setPreviousLastUpdateTimestamp(lastDBUpdate);
                return lastVersion;
            };
        }
        final String whereClauseExtension = getWhereClauseExtension(id, extractionNamespace);
        return () -> {
            final DBI dbi = ensureDBI(id, extractionNamespace);

            LOG.debug("Updating [%s]", id);
            dbi.withHandle(
                    (HandleCallback<Void>) handle -> {
                        String query = String.format("SELECT %s FROM %s",
                                String.join(COMMA_SEPARATOR, extractionNamespace.getColumnList()),
                                extractionNamespace.getTable()
                        );

                        populateRowListFromJDBC(extractionNamespace, query, lastDBUpdate, handle, new RowMapper(extractionNamespace, cache), whereClauseExtension);
                        return null;
                    }
            );

            LOG.info("Finished loading %d values for extractionNamespace[%s]", cache.size(), id);
            extractionNamespace.setPreviousLastUpdateTimestamp(lastDBUpdate);
            return String.format("%d", lastDBUpdate.getTime());
        };
    }

    Void populateRowListFromJDBC(
            JDBCExtractionNamespace extractionNamespace,
            String query,
            Timestamp lastDBUpdate,
            Handle handle,
            RowMapper rm
    ) {
        return populateRowListFromJDBC(extractionNamespace, query, lastDBUpdate, handle, rm, "");
    }

    Void populateRowListFromJDBC(
            JDBCExtractionNamespace extractionNamespace,
            String query,
            Timestamp lastDBUpdate,
            Handle handle,
            RowMapper rm,
            String whereClauseExtension
    ) {
        Timestamp updateTS;
        if (extractionNamespace.isFirstTimeCaching()) {
            extractionNamespace.setFirstTimeCaching(false);
            query = String.format("%s %s %s",
                    query,
                    getBaseWhereClause(FIRST_TIME_CACHING_WHERE_CLAUSE, extractionNamespace),
                    whereClauseExtension
            );
            updateTS = lastDBUpdate;

        } else {
            query = String.format("%s %s",
                    query,
                    getBaseWhereClause(SUBSEQUENT_CACHING_WHERE_CLAUSE, extractionNamespace),
                    whereClauseExtension
            );
            updateTS = extractionNamespace.getPreviousLastUpdateTimestamp();
        }

        List<Void> p = handle.createQuery(query).map(rm)
                .setFetchSize(FETCH_SIZE)
                .bind("lastUpdatedTimeStamp",
                        getTsValue(extractionNamespace, updateTS))
                .list();

        return null;
    }

    protected String getWhereClauseExtension(String id, JDBCExtractionNamespace extractionNamespace) {
        String whereClauseExtension = "";
        if (extractionNamespace.hasTsColumnConfig() && extractionNamespace.getTsColumnConfig().hasSecondaryTsColumn()) {
            String maxVal = (String) getMaxValFromColumn(id, extractionNamespace, StringMapper.FIRST, extractionNamespace.getTsColumnConfig().getSecondaryTsColumn(), extractionNamespace.getTable());
            whereClauseExtension = String.format(WHERE_CLAUSE_EXTENSION,
                    extractionNamespace.getTsColumnConfig().getSecondaryTsColumn(),
                    extractionNamespace.getTsColumnConfig().getSecondaryTsColumnCondition(),
                    StringUtil.quoteStringLiteral(maxVal)
            );
        }
        return whereClauseExtension;
    }

    private Object getTsValue(JDBCExtractionNamespace extractionNamespace, Timestamp updateTS) {
        Object tsValue = updateTS;
        if (extractionNamespace.hasTsColumnConfig()) {
            if (extractionNamespace.getTsColumnConfig().isVarchar()) {
                tsValue = new SimpleDateFormat(extractionNamespace.getTsColumnConfig().getFormat()).format(updateTS);
            } else if (extractionNamespace.getTsColumnConfig().isBigint()) {
                tsValue = updateTS.getTime();
            }
        }
        return tsValue;
    }

    protected String getBaseWhereClause(String baseClause, JDBCExtractionNamespace namespace) {
        return String.format(
                baseClause,
                namespace.getTsColumn(),
                namespace.hasTsColumnConfig()?
                        (namespace.getTsColumnConfig().isVarchar() ?
                                StringUtil.quoteStringLiteral(LAST_UPDATED_TIMESTAMP)
                                : LAST_UPDATED_TIMESTAMP)
                        : LAST_UPDATED_TIMESTAMP
        );
    }

    protected DBI ensureDBI(String id, JDBCExtractionNamespace namespace) {
        final String key = id;
        DBI dbi = null;

        if (dbiCache.containsKey(key)) {
            dbi = dbiCache.get(key);
        }

        if (dbi == null) {
            if (!namespace.hasKerberosProperties()) {
                LOG.info("Connecting %s using user and password", namespace.getConnectorConfig().getConnectURI());
                dbi = new DBI(
                        namespace.getConnectorConfig().getConnectURI(),
                        namespace.getConnectorConfig().getUser(),
                        namespace.getConnectorConfig().getPassword()
                );
            } else {
                LOG.info("Connecting %s using Kerberos", namespace.getConnectorConfig().getConnectURI());
                dbi = new DBI(
                        namespace.getConnectorConfig().getConnectURI(),
                        namespace.getKerberosProperties()
                );
            }
            dbiCache.putIfAbsent(key, dbi);
        }
        return dbi;
    }

    protected Timestamp lastUpdates(String id, JDBCExtractionNamespace namespace, Boolean isFollower) {
        final String table = namespace.getTable();
        final String tsColumn = namespace.getTsColumn();

        if (tsColumn == null) {
            return null;
        }

        if (!namespace.isFirstTimeCaching() && isFollower)
            return namespace.getPreviousLastUpdateTimestamp();

        final Timestamp lastUpdatedTimeStamp = (Timestamp) getMaxValFromColumn(id, namespace, CustomizedTimestampMapper.FIRST, tsColumn, table);
        return lastUpdatedTimeStamp;

    }

    protected Object getMaxValFromColumn(String id, JDBCExtractionNamespace namespace, TypedMapper mapper, String col, String table) {
        final DBI dbi = ensureDBI(id, namespace);
        return getMaxValFromColumn(dbi, mapper, col, table);
    }

    protected Object getMaxValFromColumn(DBI dbi, TypedMapper mapper, String col, String table) {
        final Object maxVal = dbi.withHandle(
                handle -> {
                    final String query = String.format(
                            "SELECT MAX(%s) FROM %s",
                            col, table
                    );
                    return handle
                            .createQuery(query)
                            .map(mapper)
                            .first();
                }
        );
        return maxVal;
    }

    @Override
    public void updateCache(final JDBCExtractionNamespace extractionNamespace, final Map<String, List<String>> cache,
                            final String key, final byte[] value) {
        //No-op
    }

    @Override
    public byte[] getCacheValue(final JDBCExtractionNamespace extractionNamespace, final Map<String, List<String>> cache, final String key, final String valueColumn, final Optional<DecodeConfig> decodeConfigOptional) {
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

    private byte[] handleDecode(JDBCExtractionNamespace extractionNamespace, List<String> cacheValue, DecodeConfig decodeConfig) {

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
    public String getCacheSize(final JDBCExtractionNamespace extractionNamespace, final Map<String, List<String>> cache) {
        if (!extractionNamespace.isCacheEnabled()) {
            return String.valueOf(lookupService.getSize());
        }
        return String.valueOf(cache.size());
    }

    @Override
    public Long getLastUpdatedTime(final JDBCExtractionNamespace extractionNamespace) {
        if (!extractionNamespace.isCacheEnabled()) {
            return lookupService.getLastUpdatedTime(new LookupService.LookupData(extractionNamespace));
        }
        return (extractionNamespace.getPreviousLastUpdateTimestamp() != null) ? extractionNamespace.getPreviousLastUpdateTimestamp().getTime() : -1L;
    }
}
