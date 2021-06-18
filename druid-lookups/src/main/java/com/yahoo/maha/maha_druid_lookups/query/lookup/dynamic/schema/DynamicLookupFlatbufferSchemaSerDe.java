package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema;


import com.fasterxml.jackson.databind.JsonNode;

import java.util.HashMap;
import java.util.Map;

public class DynamicLookupFlatbufferSchemaSerDe implements  DynamicLookupCoreSchema{

    //WIP
    JsonNode coreSchema;

    public DynamicLookupFlatbufferSchemaSerDe(JsonNode coreSchema){ // check if the JsonNode is the best way to pass coreSchema
        this.coreSchema = coreSchema;
    }

    public SCHEMA_TYPE getSchemaType(){
        return SCHEMA_TYPE.FLATBUFFER;
    }


    /*
    public JSONObject toJson(){

    }
    */


    public Map<String, Integer> getSchema(){ // Is it ok to return HashMap here and what needs to be returned in Protobuf?
        //WIP


        return new HashMap<String, Integer>();
    }


}
