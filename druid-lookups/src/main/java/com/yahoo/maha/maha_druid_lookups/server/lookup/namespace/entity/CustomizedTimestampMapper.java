package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity;

import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.URIExtractionNamespace;
import org.skife.jdbi.v2.util.TimestampMapper;

import java.text.ParseException;
import java.util.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class CustomizedTimestampMapper extends TimestampMapper {

    private static final String DEFAULT_TIMESTAMP_FORMAT = "yyyyMMddhhmm";
    private static final SimpleDateFormat DEFAULT_DATE_FORMAT = new SimpleDateFormat(DEFAULT_TIMESTAMP_FORMAT);

    public static final TimestampMapper FIRST = new CustomizedTimestampMapper(1);

    private SimpleDateFormat dateFormat;

    public CustomizedTimestampMapper(int index) {
        super(index);
        this.dateFormat = DEFAULT_DATE_FORMAT;
    }

    public CustomizedTimestampMapper(int index, String dateFormat) {
        super(index);
        this.dateFormat = new SimpleDateFormat(dateFormat);
    }

    public static TimestampMapper getInstance(JDBCExtractionNamespace namespace) {
        if (namespace.hasTsColumnConfig() && namespace.getTsColumnConfig().getFormat() != null)
            return new CustomizedTimestampMapper(1, namespace.getTsColumnConfig().getFormat());
        else
            return CustomizedTimestampMapper.FIRST;
    }

    public static TimestampMapper getInstance(URIExtractionNamespace namespace) {
        if (namespace.hasTsColumnConfig() && namespace.getTsColumnConfig().getFormat() != null)
            return new CustomizedTimestampMapper(1, namespace.getTsColumnConfig().getFormat());
        else
            return CustomizedTimestampMapper.FIRST;
    }

    @Override
    protected Timestamp extractByIndex(ResultSet r, int index) throws SQLException {
        Timestamp result;
        Object timestamp = r.getObject(index);

        if (timestamp instanceof Long) {
            result = new Timestamp(r.getLong(index));
        } else if (timestamp instanceof String) {
            result = getTimestampFromString(r, index, dateFormat);
        } else { // Timestamp by default
            result = r.getTimestamp(index);
        }

        return result;
    }

    protected Timestamp getTimestampFromString(ResultSet r, int index, SimpleDateFormat dateFormat) throws SQLException {
        Timestamp result;
        Date parsedDate;
        String date = r.getString(index);

        try {
            parsedDate = dateFormat.parse(date);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e); //ParseException must be handled
        }

        result = new Timestamp(parsedDate.getTime());
        return result;
    }
}