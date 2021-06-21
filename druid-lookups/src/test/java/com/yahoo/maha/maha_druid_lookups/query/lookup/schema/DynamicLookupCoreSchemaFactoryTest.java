package com.yahoo.maha.maha_druid_lookups.query.lookup.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.*;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

public class DynamicLookupCoreSchemaFactoryTest {

    ObjectMapper objectMapper ;
    JsonNode jsonNode ;
    @BeforeClass
    public void setUp() throws JsonProcessingException {
        objectMapper = new ObjectMapper();
        jsonNode = objectMapper.readTree("");
    }

    @Test
    public void DynamicLookupCoreSchemaFactoryTestProtobuf() throws IOException {
        DynamicLookupCoreSchema dynamicLookupCoreSchema = DynamicLookupCoreSchemaFactory.buildSchema(SCHEMA_TYPE.PROTOBUF, jsonNode);
        Assert.assertTrue(dynamicLookupCoreSchema instanceof DynamicLookupProtobufSchemaSerDe);
    }

    @Test
    public void DynamicLookupCoreSchemaFactoryTestFlatbuffer() throws IOException {
        DynamicLookupCoreSchema dynamicLookupCoreSchema = DynamicLookupCoreSchemaFactory.buildSchema(SCHEMA_TYPE.FLATBUFFER, jsonNode);
        Assert.assertTrue(dynamicLookupCoreSchema instanceof DynamicLookupFlatbufferSchemaSerDe);
    }
}
