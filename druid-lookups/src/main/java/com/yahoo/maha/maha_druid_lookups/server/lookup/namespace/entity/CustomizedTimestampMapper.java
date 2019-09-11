package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity;

import org.skife.jdbi.v2.util.TimestampMapper;

import java.util.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class CustomizedTimestampMapper extends TimestampMapper {

    private static final String ERROR_MSG_PRE = "Expected column to be a timestamp type but is ";
    private static final String VARCHAR_ERROR_MSG = ERROR_MSG_PRE + "varchar";
    private static final String BIGINT_ERROR_MSG = ERROR_MSG_PRE + "BIGINT";

    private static final String DEFAULT_TIMESTAMP_FORMAT = "yyyyMMddhhmm";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat(DEFAULT_TIMESTAMP_FORMAT);

    public static final TimestampMapper FIRST = new CustomizedTimestampMapper(1);

    public CustomizedTimestampMapper(int index) {
        super(index);
    }

    @Override
    protected Timestamp extractByIndex(ResultSet r, int index) throws SQLException {
        Timestamp result;

        try {
            result = r.getTimestamp(index);
        } catch (Exception e) {
            if (e.getMessage().contains(BIGINT_ERROR_MSG)) {
                result = new Timestamp(r.getLong(index));
            } else if (e.getMessage().contains(VARCHAR_ERROR_MSG)) {
                result = getTimestampFromString(r, index);
            } else {
                throw e;
            }
        }

        return result;
    }

    protected Timestamp getTimestampFromString(ResultSet r, int index) throws SQLException {
        Timestamp result;
        Date parsedDate;
        String date = r.getString(index);

        try {
            parsedDate = dateFormat.parse(date);
        } catch (Exception e) {
            throw new IllegalArgumentException(e.getMessage());
        }

        result = new Timestamp(parsedDate.getTime());
        return result;
    }
}