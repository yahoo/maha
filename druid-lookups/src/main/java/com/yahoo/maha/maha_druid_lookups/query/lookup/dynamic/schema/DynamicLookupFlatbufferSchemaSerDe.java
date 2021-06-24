package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema;


import com.fasterxml.jackson.databind.JsonNode;
import com.yahoo.maha.maha_druid_lookups.query.lookup.*;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.*;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.*;

import java.util.*;

public class DynamicLookupFlatbufferSchemaSerDe implements  DynamicLookupCoreSchema {

    //WIP
    private JsonNode coreSchema;

    public DynamicLookupFlatbufferSchemaSerDe(JsonNode coreSchema){
        this.coreSchema = coreSchema;
    }

    public ExtractionNameSpaceSchemaType getSchemaType(){
        return ExtractionNameSpaceSchemaType.FLAT_BUFFER;
    }

    public String getSchemaPath() {
        return "";
    }

    @Override
    public String getValue(String fieldName, byte[] dataBytes, Optional<DecodeConfig> decodeConfigOptional, RocksDBExtractionNamespace extractionNamespace) {
        return null;
    }

    private GenericFlatBufferTable getDynamicTable(String fieldName, byte[] dataBytes, RocksDBExtractionNamespace extractionNamespace) {
        return null;
    }


    @Override
    public String toString() {
        return "DynamicLookupFlatbufferSchemaSerDe{" +
                "coreSchema=" + coreSchema +
                '}';
    }
}
