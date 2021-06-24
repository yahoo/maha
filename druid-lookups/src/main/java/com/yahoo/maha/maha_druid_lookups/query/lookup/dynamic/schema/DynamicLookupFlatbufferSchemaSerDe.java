package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema;


import com.fasterxml.jackson.databind.JsonNode;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DynamicLookupFlatbufferSchemaSerDe implements  DynamicLookupCoreSchema<String,Optional<Long>>{

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


    public ImmutablePair<String ,Optional<Long>>  getValue(String fieldName, byte[] dataBytes, RocksDBExtractionNamespace extractionNamespace){
        return new ImmutablePair<>("",Optional.empty());
    }

    @Override
    public String toString(){
        return "";
    }


}
