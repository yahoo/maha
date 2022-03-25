package com.yahoo.maha.maha_druid_lookups.query.lookup.parser;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.URIExtractionNamespace;
import org.apache.druid.java.util.common.parsers.Parser;

import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "format")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = "csv", value = CSVFlatDataParser.class),
        @JsonSubTypes.Type(name = "tsv", value = TSVFlatDataParser.class)

})
public interface FlatDataParser {
    Parser<String, List<String>> getParser();
    List<String> getColumns();
    String getKey();
    String getNullReplacement();
}
