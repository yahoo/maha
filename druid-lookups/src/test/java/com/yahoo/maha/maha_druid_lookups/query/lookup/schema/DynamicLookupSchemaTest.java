package com.yahoo.maha.maha_druid_lookups.query.lookup.schema;

import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupSchema;

import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.*;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.zeroturnaround.zip.commons.*;

import java.io.*;
import java.util.*;

public class DynamicLookupSchemaTest {

    private final String dir = "./dynamic/schema/";
    private  ClassLoader classLoader ;


    @BeforeClass
    public void setup() throws Exception {
        classLoader = Thread.currentThread().getContextClassLoader();
    }


    @Test
    public void DynamicLookupSchemaBuilderTestPB() throws IOException {

        File schemaFile = new File(classLoader.getResource(dir + "dynamic_lookup_pb_schema.json").getPath());
        Optional<DynamicLookupSchema> dynamicLookupSchemaOptional = DynamicLookupSchema.parseFrom(schemaFile);
        Assert.assertTrue(dynamicLookupSchemaOptional.isPresent());
        DynamicLookupSchema dynamicLookupSchema = dynamicLookupSchemaOptional.get();
        Assert.assertEquals(dynamicLookupSchema.getName() , "product_ad_pb_dym_lookup");
        Assert.assertEquals(dynamicLookupSchema.getVersion(), "2021061800");
        Assert.assertEquals(dynamicLookupSchema.getType(), ExtractionNameSpaceSchemaType.PROTOBUF);
        Assert.assertEquals(dynamicLookupSchema.getSchemaFieldList().size(), 2);
    }

    /*

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void DynamicLookupSchemaBuilderMissingVersion() throws IOException {
        dynamicLookupSchemaBuilder.setSchemaFilePath(classLoader.getResource(dir + "dynamic_lookup_missing_version.json").getPath());
    }


    @Test (expectedExceptions = IllegalArgumentException.class)
    public void DynamicLookupSchemaBuilderMissingName() throws IOException {
        dynamicLookupSchemaBuilder.setSchemaFilePath(classLoader.getResource(dir + "dynamic_lookup_missing_name.json").getPath());
    }


    @Test (expectedExceptions = IllegalArgumentException.class)
    public void DynamicLookupSchemaBuilderMissingType() throws IOException {
        dynamicLookupSchemaBuilder.setSchemaFilePath(classLoader.getResource(dir + "dynamic_lookup_missing_type.json").getPath());
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void DynamicLookupSchemaBuilderMissingCoreSchema() throws IOException {
        dynamicLookupSchemaBuilder.setSchemaFilePath(classLoader.getResource(dir + "dynamic_lookup_missing_core_schema.json").getPath());
    }
    @Test (expectedExceptions = IllegalArgumentException.class)
    public void DynamicLookupSchemaBuilderBadCoreSchema() throws IOException {
        dynamicLookupSchemaBuilder.setSchemaFilePath(classLoader.getResource(dir + "dynamic_lookup_unknown_schema_type.json").getPath());
    }

    @Test
    public void DynamicLookupSchemaBuilderTestFB() throws IOException {
        DynamicLookupSchema dynamicLookupSchema = dynamicLookupSchemaBuilder
                .setSchemaFilePath(classLoader.getResource(dir + "dynamic_lookup_fb_schema.json").getPath())
                .build();
        Assert.assertEquals(dynamicLookupSchema.getName() , "product_ad_dym_lookup");
        Assert.assertEquals(dynamicLookupSchema.getVersion(), "2021061800");
        Assert.assertEquals(dynamicLookupSchema.getSchemaType(), ExtractionNameSpaceSchemaType.FLAT_BUFFER);
    }
    */

}

