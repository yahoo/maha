package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.HashMap;
import java.util.Map;

public class DynamicLookupProtobufSchemaSerDe implements DynamicLookupCoreSchema {

    //WIP
    public DynamicLookupProtobufSchemaSerDe(JsonNode coreSchema){

    }

    public SCHEMA_TYPE getSchemaType(){
        return SCHEMA_TYPE.PROTOBUF;
    }


    public Map<String, Integer> getSchema(){
        //WIP
        return new HashMap<String, Integer>();
    }

}

