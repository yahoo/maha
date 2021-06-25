package com.yahoo.maha.maha_druid_lookups.query.lookup.schema;


import com.google.protobuf.Descriptors;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.*;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.*;
import java.util.*;

public class DynamicLookupCoreSchemaFactoryTest {

    DynamicLookupSchema dynamicLookupSchema;

    private final String dir = "./dynamic/schema/";

    @BeforeClass
    public void setUp() {
        File schemaFile = new File(this.getClass().getClassLoader().getResource(dir + "dynamic_lookup_pb_schema.json").getPath());
        Optional<DynamicLookupSchema> dynamicLookupSchemaOptional = DynamicLookupSchema.parseFrom(schemaFile);
        Assert.assertTrue(dynamicLookupSchemaOptional.isPresent());
        dynamicLookupSchema = dynamicLookupSchemaOptional.get();
    }

    @Test
    public void DynamicLookupCoreSchemaFactoryTestProtobuf() throws IOException, Descriptors.DescriptorValidationException {
        DynamicLookupCoreSchema dynamicLookupCoreSchema = DynamicLookupCoreSchemaFactory.buildSchema(dynamicLookupSchema);
        Assert.assertTrue(dynamicLookupCoreSchema instanceof DynamicLookupProtobufSchemaSerDe);
    }

/*
    @Test
    public void DynamicLookupCoreSchemaFactoryTestFlatbuffer() throws IOException, Descriptors.DescriptorValidationException {
        DynamicLookupCoreSchema dynamicLookupCoreSchema = DynamicLookupCoreSchemaFactory.buildSchema(ExtractionNameSpaceSchemaType.FLAT_BUFFER, jsonNode);
        Assert.assertTrue(dynamicLookupCoreSchema instanceof DynamicLookupFlatbufferSchemaSerDe);
    }
*/
}
