package com.yahoo.maha.maha_druid_lookups.query.lookup.schema;


import com.google.protobuf.Descriptors;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.*;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.*;
import java.util.*;

public class DynamicLookupCoreSchemaFactoryTest {

    DynamicLookupSchema dynamicLookupSchemaPb;
    DynamicLookupSchema dynamicLookupSchemaFb;

    private final String dir = "./dynamic/schema/";

    @BeforeClass
    public void setUp() {
        File schemaFilePb = new File(this.getClass().getClassLoader().getResource(dir + "dynamic_lookup_pb_schema.json").getPath());
        Optional<DynamicLookupSchema> dynamicLookupSchemaOptionalPb = DynamicLookupSchema.parseFrom(schemaFilePb);
        Assert.assertTrue(dynamicLookupSchemaOptionalPb.isPresent());
        dynamicLookupSchemaPb = dynamicLookupSchemaOptionalPb.get();

        File schemaFileFb = new File(this.getClass().getClassLoader().getResource(dir + "dynamic_lookup_fb_schema.json").getPath());
        Optional<DynamicLookupSchema> dynamicLookupSchemaOptionalFb = DynamicLookupSchema.parseFrom(schemaFileFb);
        Assert.assertTrue(dynamicLookupSchemaOptionalFb.isPresent());
        dynamicLookupSchemaFb = dynamicLookupSchemaOptionalFb.get();
    }

    @Test
    public void DynamicLookupCoreSchemaFactoryTestProtobuf() throws  Descriptors.DescriptorValidationException {
        DynamicLookupCoreSchema dynamicLookupCoreSchema = DynamicLookupCoreSchemaFactory.buildSchema(dynamicLookupSchemaPb);
        System.out.println(dynamicLookupCoreSchema.toString());
        Assert.assertTrue(dynamicLookupCoreSchema instanceof DynamicLookupProtobufSchemaSerDe);
    }

    @Test
    public void DynamicLookupCoreSchemaFactoryTestFlatbuffer() throws  Descriptors.DescriptorValidationException{
        DynamicLookupCoreSchema dynamicLookupCoreSchema = DynamicLookupCoreSchemaFactory.buildSchema(dynamicLookupSchemaFb);
        Assert.assertTrue(dynamicLookupCoreSchema instanceof DynamicLookupFlatbufferSchemaSerDe);
    }
}
