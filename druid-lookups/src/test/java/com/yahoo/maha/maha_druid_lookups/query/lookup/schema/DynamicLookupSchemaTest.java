package com.yahoo.maha.maha_druid_lookups.query.lookup.schema;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.*;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupSchema;

import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.FieldDataType;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.*;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.*;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.flatbuffer.*;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.protobuf.DefaultProtobufSchemaFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.*;
import java.util.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DynamicLookupSchemaTest {

    private final String dir = "./dynamic/schema/";
    private  ClassLoader classLoader ;
    DynamicLookupSchema dynamicLookupSchema;
    RocksDBExtractionNamespace extractionNamespace;

    @BeforeClass
    public void setup() throws Exception {
        classLoader = Thread.currentThread().getContextClassLoader();
        File schemaFile = new File(classLoader.getResource(dir + "dynamic_lookup_pb_schema.json").getPath());
        Optional<DynamicLookupSchema> dynamicLookupSchemaOptional = DynamicLookupSchema.parseFrom(schemaFile);
        Assert.assertTrue(dynamicLookupSchemaOptional.isPresent());
        dynamicLookupSchema = dynamicLookupSchemaOptional.get();
        extractionNamespace = mock(RocksDBExtractionNamespace.class);
        when(extractionNamespace.getLookupName()).thenReturn("ad_pb_dym_lookup");
    }

    @AfterClass
    public void clean() throws Exception {
        File rmFile = new File(classLoader.getResource(dir + "ad_pb_dyn_lookup.json").getPath());
        rmFile.delete();

        File dynFile = new File(classLoader.getResource(dir + "product_ad_dyn_lookup.json").getPath());
        dynFile.delete();
    }

    @Test
    public void DynamicLookupSchemaBuilderTestPB() throws IOException {
        Assert.assertEquals(dynamicLookupSchema.getName() , "ad_pb_dym_lookup");
        Assert.assertEquals(dynamicLookupSchema.getVersion(), "2021061800");
        Assert.assertEquals(dynamicLookupSchema.getType(), ExtractionNameSpaceSchemaType.PROTOBUF);
        Assert.assertEquals(dynamicLookupSchema.getSchemaFieldList().size(), 4);
    }

    @Test
    public void getValueDynamicMessageTest() {
        Message msg = AdProtos.Ad.newBuilder()
                .setId("32309719080")
                .setTitle("some title")
                .setStatus("ON")
                .setLastUpdated("1470733203505")
                .build();

        String value = dynamicLookupSchema.getCoreSchema().getValue("status", msg.toByteArray(), Optional.empty(), extractionNamespace);
        Assert.assertEquals(value, "ON");
    }

    @Test
    public void testToJson() {
        DefaultProtobufSchemaFactory defaultProtobufSchemaFactory = new DefaultProtobufSchemaFactory(ImmutableMap.<String, GeneratedMessageV3>of("Ad", AdProtos.Ad.getDefaultInstance()));
        String resourcePath = classLoader.getResource(dir).getPath();
        String outputName = "ad_pb_dyn_lookup";
        DynamicLookupSchema.toJson("Ad", defaultProtobufSchemaFactory, "ad_pb_dyn_lookup", resourcePath);

        File output = new File(resourcePath + outputName + ".json");
        Assert.assertTrue(output.exists());

        Optional<DynamicLookupSchema> dynamicLookupSchemaOptional = DynamicLookupSchema.parseFrom(output);
        Assert.assertTrue(dynamicLookupSchemaOptional.isPresent());
        DynamicLookupSchema resSchema = dynamicLookupSchemaOptional.get();
        Assert.assertEquals(resSchema.getName() , "ad_pb_dyn_lookup");
        Assert.assertEquals(resSchema.getType(), ExtractionNameSpaceSchemaType.PROTOBUF);
        Assert.assertEquals(resSchema.getSchemaFieldList().size(), 4);
        Assert.assertEquals(resSchema.getSchemaFieldList().get(0).getField(), "id");
        Assert.assertEquals(resSchema.getSchemaFieldList().get(0).getIndex(), 1);
        Assert.assertEquals(resSchema.getSchemaFieldList().get(0).getDataType(), FieldDataType.STRING);
        Assert.assertEquals(resSchema.getSchemaFieldList().get(3).getField(), "last_updated");
        Assert.assertEquals(resSchema.getSchemaFieldList().get(3).getIndex(), 4);
        Assert.assertEquals(resSchema.getSchemaFieldList().get(3).getDataType(), FieldDataType.STRING);
    }


    @Test
    public void testToJsonFlatBuffer() {
        FlatBufferSchemaFactory flatBufferSchemaFactory = new TestFlatBufferSchemaFactory();
        String resourcePath = classLoader.getResource(dir).getPath();
        String outputName = "product_ad_dyn_lookup";
        DynamicLookupSchema.toJson("product_ad_fb_lookup", flatBufferSchemaFactory, outputName, resourcePath);

        File output = new File(resourcePath + outputName + ".json");
        Assert.assertTrue(output.exists());

        Optional<DynamicLookupSchema> dynamicLookupSchemaOptional = DynamicLookupSchema.parseFrom(output);
        Assert.assertTrue(dynamicLookupSchemaOptional.isPresent());
        DynamicLookupSchema resSchema = dynamicLookupSchemaOptional.get();
        Assert.assertEquals(resSchema.getName() , "product_ad_dyn_lookup");
        Assert.assertEquals(resSchema.getType(), ExtractionNameSpaceSchemaType.FLAT_BUFFER);
        Assert.assertEquals(resSchema.getSchemaFieldList().size(), 8);
        Assert.assertEquals(resSchema.getSchemaFieldList().get(0).getField(), "id");
        Assert.assertEquals(resSchema.getSchemaFieldList().get(0).getIndex(), 4);
        Assert.assertEquals(resSchema.getSchemaFieldList().get(0).getDataType(), FieldDataType.STRING);
        Assert.assertEquals(resSchema.getSchemaFieldList().get(7).getField(), "last_updated");
        Assert.assertEquals(resSchema.getSchemaFieldList().get(7).getIndex(), 18);
        Assert.assertEquals(resSchema.getSchemaFieldList().get(7).getDataType(), FieldDataType.STRING);
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

