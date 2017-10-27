// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest;

import java.sql.ResultSet;
import java.sql.SQLException;

public class RowMapperUtil {

    public static Long getLong(ResultSet rs, String fieldName) throws SQLException {
        Long field = rs.getLong(fieldName);
        if (rs.wasNull()) {
            field = null;
        }
        return field;
    }

    public static Double getDouble(ResultSet rs, String fieldName) throws SQLException {
        Double field = rs.getDouble(fieldName);
        if (rs.wasNull()) {
            field = null;
        }
        return field;
    }

    public static String getString(ResultSet rs, String fieldName) throws SQLException {
        String field = rs.getString(fieldName);
        if (rs.wasNull()) {
            field = null;
        }
        return field;
    }

    public static Integer getInteger(ResultSet rs, String fieldName) throws SQLException {
        Integer field = rs.getInt(fieldName);
        if (rs.wasNull()) {
            field = null;
        }
        return field;
    }
}
