package com.yahoo.maha.maha_druid_lookups.query.lookup.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors;
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
        String path = Thread.currentThread().getContextClassLoader()
                .getResource("./dynamic/schema/Ad.desc").getPath();
        String coreSchema = "{\"descFilePath\" : \""  + path+ "\" }"; // works in local
        objectMapper = new ObjectMapper();
        jsonNode = objectMapper.readTree(coreSchema);
    }

    @Test
    public void DynamicLookupCoreSchemaFactoryTestProtobuf() throws IOException, Descriptors.DescriptorValidationException {
        DynamicLookupCoreSchema dynamicLookupCoreSchema = DynamicLookupCoreSchemaFactory.buildSchema(SCHEMA_TYPE.PROTOBUF, jsonNode);
        Assert.assertTrue(dynamicLookupCoreSchema instanceof DynamicLookupProtobufSchemaSerDe);
    }

    @Test
    public void DynamicLookupCoreSchemaFactoryTestFlatbuffer() throws IOException, Descriptors.DescriptorValidationException {
        DynamicLookupCoreSchema dynamicLookupCoreSchema = DynamicLookupCoreSchemaFactory.buildSchema(SCHEMA_TYPE.FLATBUFFER, jsonNode);
        Assert.assertTrue(dynamicLookupCoreSchema instanceof DynamicLookupFlatbufferSchemaSerDe);
    }
}
