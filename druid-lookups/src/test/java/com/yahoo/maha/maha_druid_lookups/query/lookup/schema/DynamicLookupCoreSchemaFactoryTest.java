package com.yahoo.maha.maha_druid_lookups.query.lookup.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupCoreSchema;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupCoreSchemaFactory;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupProtobufSchemaSerDe;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.SCHEMA_TYPE;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

public class DynamicLookupCoreSchemaFactoryTest {

    @Test
    public void DynamicLookupCoreSchemaFactoryTest() throws IOException {

        ObjectMapper objectMapper = new ObjectMapper();
        File file = new File(Thread.currentThread().getContextClassLoader().getResource("./dynamic/schema/dynamic_lookup_core_schema_only.json").getFile());
        JsonNode jsonNode = objectMapper.readTree(file);
        DynamicLookupCoreSchema dynamicLookupCoreSchema = DynamicLookupCoreSchemaFactory.buildSchema(SCHEMA_TYPE.PROTOBUF, jsonNode);
        Assert.assertTrue(dynamicLookupCoreSchema instanceof DynamicLookupProtobufSchemaSerDe);

    }
}
