package com.yahoo.maha.maha_druid_lookups.query.lookup.schema;

import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupSchema;

import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.SCHEMA_TYPE;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


import java.io.IOException;



import static org.mockito.Mockito.spy;


public class DynamicLookupSchemaTest {

    private final String dir = "./dynamic/schema/";
    private  ClassLoader classLoader ;
    private  DynamicLookupSchema.Builder dynamicLookupSchemaBuilder;


    @BeforeClass
    public void setup() throws Exception {
        classLoader = Thread.currentThread().getContextClassLoader();
        dynamicLookupSchemaBuilder = new DynamicLookupSchema.Builder(); // can this be set multiple times.
    }


    @Test
    public void DynamicLookupSchemaBuilderTest() throws IOException {
        DynamicLookupSchema dynamicLookupSchema = dynamicLookupSchemaBuilder
                .setSchemaFilePath(classLoader.getResource(dir + "dynamic_lookup_schema.json").getPath())
                .build();
        Assert.assertEquals(dynamicLookupSchema.getName() , "product_ad_pb_dym_lookup");
        Assert.assertEquals(dynamicLookupSchema.getVersion(), "2021061800");
        Assert.assertEquals(dynamicLookupSchema.getSchemaType(), SCHEMA_TYPE.PROTOBUF);
    }


    @Test (expectedExceptions = NullPointerException.class)
    public void DynamicLookupSchemaBuilderMissingVersion() throws IOException {
        dynamicLookupSchemaBuilder.setSchemaFilePath(classLoader.getResource(dir + "dynamic_lookup_missing_version.json").getPath());
    }


    @Test (expectedExceptions = NullPointerException.class)
    public void DynamicLookupSchemaBuilderMissingName() throws IOException {
        dynamicLookupSchemaBuilder.setSchemaFilePath(classLoader.getResource(dir + "dynamic_lookup_missing_name.json").getPath());
    }


    @Test (expectedExceptions = NullPointerException.class)
    public void DynamicLookupSchemaBuilderMissingType() throws IOException {
        dynamicLookupSchemaBuilder.setSchemaFilePath(classLoader.getResource(dir + "dynamic_lookup_missing_type.json").getPath());
    }

    @Test (expectedExceptions = NullPointerException.class)
    public void DynamicLookupSchemaBuilderMissingCoreSchema() throws IOException {
        dynamicLookupSchemaBuilder.setSchemaFilePath(classLoader.getResource(dir + "dynamic_lookup_missing_core_schema.json").getPath());
    }
    @Test (expectedExceptions = IllegalArgumentException.class)
    public void DynamicLookupSchemaBuilderBadCoreSchema() throws IOException {
        dynamicLookupSchemaBuilder.setSchemaFilePath(classLoader.getResource(dir + "dynamic_lookup_unknown_schema_type.json").getPath());
    }
}

