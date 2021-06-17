package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.druid.java.util.common.logger.Logger;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.IOException;

/*
<gridPath>/schema.json
{
 type: “flatbuffer”,
“version”: “YYYYMMddhhmm” // use load time for this version string
 name : “product_ad_fb_lookup”,
    coreSchema: {  //contains field name to offset mapping
        “favoriteColumn1”: 4,
        “favoriteColumn2”: 6,
        “favoriteColumn3”: 8,
        “favoriteColumn4”: 10,
    }
}
*/

@JsonDeserialize(builder = DynamicLookupSchema.Builder.class) // not sure how to use these as we have coreSchema
public class DynamicLookupSchema {
    private static final Logger LOG = new Logger(DynamicLookupSchema.class);

    private final SCHEMA_TYPE type ;
    private final String version;
    private final String name;
    private final DynamicLookupCoreSchema dynamicLookupCoreSchema;

    private DynamicLookupSchema(Builder builder){
        this.type = builder.type;
        this.version = builder.version;
        this.name = builder.name;
        this.dynamicLookupCoreSchema = builder.dynamicLookupCoreSchema;
    }


    @Override
    public String toString(){
        return "DynamicLookupSchema{" +
                "name = " + name +
                ", type = " + type.toString() +
                ", version = " + version +
                ", coreSchema = " + dynamicLookupCoreSchema.toString() + // is printing dynamicLookupCoreSchema required
                " }";
    }


    public String getName(){
        return name;
    }

    public String getVersion(){
        return version;
    }

    public SCHEMA_TYPE getSchemaType(){
        return type;
    }


    public JSONObject toJson(){
        return new JSONObject();
    } // will get back to serialization later

    @JsonPOJOBuilder
    public static class Builder {
        protected SCHEMA_TYPE type;
        protected String version;
        protected String schemaFilePath;
        protected String name;
        protected DynamicLookupCoreSchema dynamicLookupCoreSchema;

        private void buildType(SCHEMA_TYPE type) {
            this.type = type;
        }

        private void buildVersion(String  version) {
            this.version = version;
        }

        private void buildName(String  name) {
            this.name = name;
        }


        private void buildDynamicLookupCoreSchema(SCHEMA_TYPE type,JsonNode coreSchema){
            this.dynamicLookupCoreSchema = new DynamicLookupCoreSchemaFactory().buildSchema(type, coreSchema);
        }
        public Builder setSchemaFilePath(String schemaFilePath) {
            this.schemaFilePath = schemaFilePath;
            parseJson(schemaFilePath);
            return this;
        }

        private void parseJson(String schemaFilePath) {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode json = null;

            try {
                File file = new File(schemaFilePath); // check if this is local path or hdfs path

                 json = objectMapper.readTree(file); // check if readTree is the most appropriate one
            } catch (IOException ex){
                LOG.error("Cannot Read schema file for path " + schemaFilePath );

            }

            String version = json.has("version") ? json.get("version").toString() : ""; //add error checking when version is empty
            String name = json.has("name") ? json.get("name").toString() : ""; //add error checking when name is empty
            SCHEMA_TYPE type = json.has("type") ? SCHEMA_TYPE.valueOf(json.get("type").toString()) : SCHEMA_TYPE.PROTOBUF; //add error checking when type is empty

            if(json.has("coreSchema")){
                buildDynamicLookupCoreSchema(type,json.get("coreSchema"));
            }
            buildVersion(version);
            buildName(name);
            buildType(type);

        }


        public DynamicLookupSchema build(){
            return new DynamicLookupSchema(this);

        }

    }
}
