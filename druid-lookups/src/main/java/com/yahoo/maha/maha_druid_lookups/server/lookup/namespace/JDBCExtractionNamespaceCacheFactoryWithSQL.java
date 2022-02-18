package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespaceWithSQL;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.RowMapper;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class JDBCExtractionNamespaceCacheFactoryWithSQL extends JDBCExtractionNamespaceCacheFactory{
    private static final Logger LOG = new Logger(JDBCExtractionNamespaceCacheFactoryWithSQL.class);

    @Override
    public Callable<String> getCachePopulator(String id, JDBCExtractionNamespace extractionNamespace, String lastVersion, Map<String, List<String>> cache) {

        LOG.info("Calling JDBCExtractionNamespaceWithSQL: " +
                "id=" + id + ", namespace=" + extractionNamespace.toString() + ", lastVers=" + lastVersion);

        return getCachePopulator(id, (JDBCExtractionNamespaceWithSQL) extractionNamespace, lastVersion, cache);
    }

    public Callable<String> getCachePopulator(String id, JDBCExtractionNamespaceWithSQL extractionNamespace, String lastVersion, Map<String, List<String>> cache) {
        final long lastCheck = lastVersion == null ? Long.MIN_VALUE / 2 : Long.parseLong(lastVersion);

        if (!extractionNamespace.isCacheEnabled()) {
            return () -> String.valueOf(lastCheck);
        }

        //Always set to current time for SQL-based extraction
        //reason: we don't care what is the max ts value in table, we only keep polling data every specified pollPeriod
        final Timestamp currentPollPeriodTime = new Timestamp(DateTime.now().getMillis());

        return () -> {
            final DBI dbi = ensureDBI(id, extractionNamespace);

            LOG.info("Updating [%s]", id);
            dbi.withHandle(
                    (HandleCallback<Void>) handle -> {
                        String query = extractionNamespace.getColumnExtractionSQL();
                        try {
                            if (query != null) {
                                handle.createQuery(query).map(new RowMapper(extractionNamespace, cache)).setFetchSize(super.getFetchSize()).list();
                            } else {
                                throw new Exception("query string is null");
                            }
                        } catch (Throwable t) {
                            LOG.error(t, "Failed to populate RowList From JDBC [s%]", id);
                            throw t;
                        }
                        return null;
                    }
            );

            LOG.info("Finished loading %d values for extractionNamespace[%s]", cache.size(), id);
            extractionNamespace.setPreviousLastUpdateTimestamp(currentPollPeriodTime);
            // return cache version for this run
            return String.format("%d", currentPollPeriodTime.getTime());
        };
    }
}
