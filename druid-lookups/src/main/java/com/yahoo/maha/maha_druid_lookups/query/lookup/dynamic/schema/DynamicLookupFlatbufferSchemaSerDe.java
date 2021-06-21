package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema;


import com.fasterxml.jackson.databind.JsonNode;

import java.util.HashMap;
import java.util.Map;

public class DynamicLookupFlatbufferSchemaSerDe implements  DynamicLookupCoreSchema{

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


    public Map<String, Integer> getSchema(){



        return new HashMap<String, Integer>();
    }

    @Override
    public String toString(){
        return "";
    }


}
