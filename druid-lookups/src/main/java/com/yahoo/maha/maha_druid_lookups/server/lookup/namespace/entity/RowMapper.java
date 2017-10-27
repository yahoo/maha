// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity;

import com.metamx.common.logger.Logger;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class RowMapper implements ResultSetMapper<Void> {

    private static final Logger LOG = new Logger(RowMapper.class);
    private static final String CONTROL_A_SEPARATOR = "\u0001";
    private JDBCExtractionNamespace extractionNamespace;
    private Map<String, String> cache;

    public RowMapper(JDBCExtractionNamespace extractionNamespace, Map<String, String> cache) {
        this.extractionNamespace = extractionNamespace;
        this.cache = cache;
    }

    @Override
    public Void map(int i, ResultSet resultSet, StatementContext statementContext) throws SQLException {

        StringBuilder sb = new StringBuilder();
        for(String columnName: this.extractionNamespace.getColumnList()) {
            sb.append(resultSet.getString(columnName)).append(CONTROL_A_SEPARATOR);
        }
        cache.put(resultSet.getString(extractionNamespace.getPrimaryKeyColumn()), sb.toString());
        return null;
    }
}
