package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.guice.annotations.ExtensionPoint;

import java.util.List;

import org.bson.Document;

@ExtensionPoint
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = FlatMultiValueDocumentProcessor.class)
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = "flatmultivalue", value = FlatMultiValueDocumentProcessor.class),
})
public interface MongoDocumentProcessor {
    int process(Document document, LookupBuilder lookupBuilder);

    List<String> getColumnList();

    String getPrimaryKeyColumn();

}
