package com.yahoo.maha.maha_druid_lookups.query.lookup.parser;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.URIExtractionNamespace;
import org.apache.druid.java.util.common.parsers.CSVParser;
import org.apache.druid.java.util.common.parsers.Parser;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@JsonTypeName("csv")
public class CSVFlatDataParser implements FlatDataParser
{
    private final Parser<String, List<String>> parser;
    private final List<String> columns;
    private final String keyColumn;
    private final String nullReplacement;

    @JsonCreator
    public CSVFlatDataParser(
            @JsonProperty("columns") List<String> columns,
            @JsonProperty("keyColumn") final String keyColumn,
            @JsonProperty("hasHeaderRow") boolean hasHeaderRow,
            @JsonProperty("skipHeaderRows") int skipHeaderRows,
            @JsonProperty("nullReplacement") String nullReplacement
    )
    {
        Preconditions.checkArgument(
                Preconditions.checkNotNull(columns, "`columns` list required").size() > 1,
                "Must specify more than one column to have a key value pair"
        );

        this.columns = columns;
        this.keyColumn = Strings.isNullOrEmpty(keyColumn) ? columns.get(0) : keyColumn;
        this.nullReplacement = nullReplacement == null ? "" : nullReplacement;
        Preconditions.checkArgument(
                columns.contains(this.keyColumn),
                "Column [%s] not found int columns: %s",
                this.keyColumn,
                Arrays.toString(columns.toArray())
        );
        CSVParser csvParser = new CSVParser(null, columns, hasHeaderRow, skipHeaderRows);
        csvParser.startFileFromBeginning();
        this.parser = new DelegateParser(
                csvParser,
                this.keyColumn,
                this.nullReplacement
        );
    }

    @JsonProperty
    public List<String> getColumns() { return columns; }

    @JsonProperty
    public String getKey() { return keyColumn; }

    @JsonProperty
    public String getNullReplacement() { return nullReplacement; }

    @VisibleForTesting
    public CSVFlatDataParser(
            List<String> columns,
            String keyColumn,
            String nullReplacement
    )
    {
        this(columns, keyColumn, false, 0, nullReplacement);
    }

    @VisibleForTesting
    public CSVFlatDataParser(
            List<String> columns,
            String keyColumn
    )
    {
        this(columns, keyColumn, false, 0, "");
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
        final CSVFlatDataParser that = (CSVFlatDataParser) o;
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
