package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema;


import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.HashMap;
import java.util.Map;

public class DynamicLookupFlatbufferSchemaSerDe implements  DynamicLookupCoreSchema<String, String>{

    //WIP
    private JsonNode coreSchema;

    public DynamicLookupFlatbufferSchemaSerDe(JsonNode coreSchema){
        this.coreSchema = coreSchema;
    }

    public SCHEMA_TYPE getSchemaType(){
        return SCHEMA_TYPE.FLATBUFFER;
    }


    /*
    public JSONObject toJson(){

    }
    */


    public String getSchema(){



        return "";
    }


    public ImmutablePair<String ,String>  getValue(String fieldName, byte[] dataBytes, String extractionNamespace){
        return new ImmutablePair<>("","");
    }

    @Override
    public String toString(){
        return "";
    }


}
