package com.yahoo.maha.maha_druid_lookups.query.lookup.parser;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.druid.java.util.common.parsers.CSVParser;
import org.apache.druid.java.util.common.parsers.DelimitedParser;
import org.apache.druid.java.util.common.parsers.Parser;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@JsonTypeName("csv")
public class TSVFlatDataParser implements FlatDataParser
{
    private final Parser<String, List<String>> parser;
    private final List<String> columns;
    private final String keyColumn;
    private final String nullReplacement;
    private final String delimiter;
    private final String listDelimiter;

    @JsonCreator
    public TSVFlatDataParser(
            @JsonProperty("columns") List<String> columns,
            @JsonProperty("keyColumn") final String keyColumn,
            @JsonProperty("hasHeaderRow") boolean hasHeaderRow,
            @JsonProperty("skipHeaderRows") int skipHeaderRows,
            @JsonProperty("nullReplacement") String nullReplacement,
            @JsonProperty("delimiter") String delimiter,
            @JsonProperty("listDelimiter") String listDelimiter
    )
    {
        Preconditions.checkArgument(
                Preconditions.checkNotNull(columns, "`columns` list required").size() > 1,
                "Must specify more than one column to have a key value pair"
        );

        this.columns = columns;
        this.keyColumn = Strings.isNullOrEmpty(keyColumn) ? columns.get(0) : keyColumn;
        this.nullReplacement = nullReplacement == null ? "" : nullReplacement;
        this.delimiter = delimiter;
        this.listDelimiter = listDelimiter;
        Preconditions.checkArgument(
                columns.contains(this.keyColumn),
                "Column [%s] not found int columns: %s",
                this.keyColumn,
                Arrays.toString(columns.toArray())
        );
        final DelimitedParser delegate = new DelimitedParser(
                Strings.emptyToNull(delimiter),
                Strings.emptyToNull(listDelimiter),
                hasHeaderRow,
                skipHeaderRows
        );
        delegate.startFileFromBeginning();
        this.parser = new DelegateParser(
                delegate,
                this.keyColumn,
                this.nullReplacement
        );
        parser.setFieldNames(columns);
    }

    @JsonProperty
    public List<String> getColumns() { return columns; }

    @JsonProperty
    public String getKey() { return keyColumn; }

    @JsonProperty
    public String getNullReplacement() { return nullReplacement; }

    @VisibleForTesting
    public TSVFlatDataParser(
            List<String> columns,
            String keyColumn,
            String nullReplacement,
            String delimiter,
            String listDelimiter
    ) {
        this(columns, keyColumn, true, 0, nullReplacement, delimiter, listDelimiter);
    }

    @VisibleForTesting
    public TSVFlatDataParser(
            List<String> columns,
            String keyColumn,
            String nullReplacement
    )
    {
        this(columns, keyColumn, true, 0, nullReplacement, null, null);
    }

    @VisibleForTesting
    public TSVFlatDataParser(
            List<String> columns,
            String keyColumn
    )
    {
        this(columns, keyColumn, true, 0, "", null, null);
    }

    @JsonProperty
    public String getKeyColumn()
    {
        return this.keyColumn;
    }

    @Override
    public Parser<String, List<String>> getParser()
    {
        return parser;
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TSVFlatDataParser that = (TSVFlatDataParser) o;
        return Objects.equals(columns, that.columns) &&
                Objects.equals(keyColumn, that.keyColumn);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columns, keyColumn);
    }

    @Override
    public String toString()
    {
        return "CSVFlatDataParser{" +
                "columns=" + columns +
                ", keyColumn='" + keyColumn + '\'' +
                '}';
    }
}
