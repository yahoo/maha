package com.yahoo.maha.maha_druid_lookups.query.lookup.parser;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.parsers.Parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DelegateParser implements Parser<String, List<String>>
{
    private final Parser<String, Object> delegate;
    private final String key;
    private final String nullReplacement;

    public DelegateParser(
            Parser<String, Object> delegate,
            String key,
            String nullReplacement
    )
    {
        this.delegate = delegate;
        this.key = key;
        this.nullReplacement = nullReplacement;
    }

    @Override
    public Map<String, List<String>> parseToMap(String input)
    {
        final Map<String, Object> inner = delegate.parseToMap(input);
        if (null == inner) {
            // Skip null or missing values, treat them as if there were no row at all.
            return ImmutableMap.of();
        }

        final String k = inner.get(key) == null ? nullReplacement : inner.get(key).toString();

        return ImmutableMap.of(k, new ArrayList(inner.values()));
    }

    @Override
    public void setFieldNames(Iterable<String> fieldNames)
    {
        delegate.setFieldNames(fieldNames);
    }

    @Override
    public List<String> getFieldNames()
    {
        return delegate.getFieldNames();
    }
}