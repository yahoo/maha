package com.yahoo.maha.maha_druid_lookups.query.lookup.schema;


import com.google.protobuf.Descriptors;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.*;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.*;
import java.util.*;

public class DynamicLookupCoreSchemaFactoryTest {

    DynamicLookupSchema dynamicLookupSchemaProtobuf;
    DynamicLookupSchema dynamicLookupSchemaFlatbuffer;

    private final String dir = "./dynamic/schema/";

    @BeforeClass
    public void setUp() {
        File schemaFilePb = new File(this.getClass().getClassLoader().getResource(dir + "dynamic_lookup_pb_schema.json").getPath());
        Optional<DynamicLookupSchema> dynamicLookupSchemaProtobufOptional = DynamicLookupSchema.parseFrom(schemaFilePb);
        Assert.assertTrue(dynamicLookupSchemaProtobufOptional.isPresent());
        dynamicLookupSchemaProtobuf = dynamicLookupSchemaProtobufOptional.get();

        File schemaFileFb = new File(this.getClass().getClassLoader().getResource(dir + "dynamic_lookup_fb_schema.json").getPath());
        Optional<DynamicLookupSchema> dynamicLookupSchemaFlatbufferOptional = DynamicLookupSchema.parseFrom(schemaFileFb);
        Assert.assertTrue(dynamicLookupSchemaFlatbufferOptional.isPresent());
        dynamicLookupSchemaFlatbuffer = dynamicLookupSchemaFlatbufferOptional.get();
    }

    @Test
    public void DynamicLookupCoreSchemaFactoryTestProtobuf() throws IOException, Descriptors.DescriptorValidationException {
        DynamicLookupCoreSchema dynamicLookupCoreSchema = DynamicLookupCoreSchemaFactory.buildSchema(dynamicLookupSchemaProtobuf);
        Assert.assertTrue(dynamicLookupCoreSchema instanceof DynamicLookupProtobufSchemaSerDe);
    }

    @Test
    public void DynamicLookupCoreSchemaFactoryTestFlatbuffer() throws IOException, Descriptors.DescriptorValidationException {
        DynamicLookupCoreSchema dynamicLookupCoreSchema = DynamicLookupCoreSchemaFactory.buildSchema(dynamicLookupSchemaFlatbuffer);
        Assert.assertTrue(dynamicLookupCoreSchema instanceof DynamicLookupFlatbufferSchemaSerDe);
    }

}
