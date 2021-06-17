package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema;

import com.fasterxml.jackson.databind.JsonNode;

public class DynamicLookupProtobufSchemaSerDe implements DynamicLookupCoreSchema {

    //WIP
    public DynamicLookupProtobufSchemaSerDe(JsonNode coreSchema){ // check if the JsonNode is the best way to pass coreSchema

    }

    public SCHEMA_TYPE getSchemaType(){
        return SCHEMA_TYPE.PROTOBUF;
    }


    public String getBinarySchema(){ //Is it ok to return string here?
        //WIP
        return "";
    }

}
