package com.yahoo.maha.maha_druid_lookups.query.lookup.schema;


import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupProtobufSchemaSerDe;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupSchema;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.FieldDataType;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.SchemaField;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNameSpaceSchemaType;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.AdProtos;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DynamicLookupProtobufSchemaSerDeTest {

    DynamicLookupProtobufSchemaSerDe dynamicLookupProtobufSchemaSerDe;
    DynamicLookupSchema dynamicLookupSchema;
    RocksDBExtractionNamespace extractionNamespace;
    Optional<DecodeConfig> decodeConfigOptional;
    private final String dir = "./dynamic/schema/";
    private  ClassLoader classLoader ;

    @BeforeClass
    public void setup() throws Exception {
        decodeConfigOptional = Optional.of(mock(DecodeConfig.class));
        classLoader = Thread.currentThread().getContextClassLoader();
        File schemaFile = new File(classLoader.getResource(dir + "dynamic_lookup_pb_schema.json").getPath());
        Optional<DynamicLookupSchema> dynamicLookupSchemaOptional = DynamicLookupSchema.parseFrom(schemaFile);
        Assert.assertTrue(dynamicLookupSchemaOptional.isPresent());
        decodeConfigOptional = Optional.of(mock(DecodeConfig.class));

        dynamicLookupProtobufSchemaSerDe = new DynamicLookupProtobufSchemaSerDe(dynamicLookupSchemaOptional.get());
        extractionNamespace = mock(RocksDBExtractionNamespace.class);

    }

    @Test
    public void DynamicLookupProtobufSchemaSerDeTestSuccess(){
        Message adMessage = AdProtos.Ad.newBuilder().setTitle("some title").setStatus("ON").setLastUpdated("2021062220").build();
        byte[] byteArr = adMessage.toByteArray();

        String str = dynamicLookupProtobufSchemaSerDe.getValue("title", byteArr, Optional.empty(), extractionNamespace);
        Assert.assertEquals("some title", str);

    }


    @Test
    public void DynamicLookupProtobufSchemaSerDeTestFailure(){
        Message adMessage = AdProtos.Ad.newBuilder().setTitle("some title").setStatus("ON").setLastUpdated("2021062220").build();
        byte[] byteArr = adMessage.toByteArray();

        String str = dynamicLookupProtobufSchemaSerDe.getValue("random", byteArr, Optional.empty(), extractionNamespace);
        Assert.assertEquals("", str);

    }


    @Test
    public void DynamicLookupProtobufSchemaSerDeTestDecodeConfig(){
        Message adMessage = AdProtos.Ad.newBuilder().setTitle("sometitle").setStatus("ON").setLastUpdated("2021062220").build();

        byte[] byteArr = adMessage.toByteArray();
        when(decodeConfigOptional.get().getColumnToCheck()).thenReturn("title"); //if getColumn to check is "title"
        when(decodeConfigOptional.get().getValueToCheck()).thenReturn("sometitle"); // if value of getColumn to check is "some title"
        when(decodeConfigOptional.get().getColumnIfValueMatched()).thenReturn("status"); // then return "status" column value
        String str = dynamicLookupProtobufSchemaSerDe.getValue("title", byteArr, decodeConfigOptional, extractionNamespace);
        Assert.assertEquals("ON", str);
    }


    @Test
    public void DynamicLookupProtobufSchemaSerDeTestDecodeConfigValueNotMatched(){
        Message adMessage = AdProtos.Ad.newBuilder().setTitle("sometitle").setStatus("ON").setLastUpdated("2021062220").build();

        byte[] byteArr = adMessage.toByteArray();
        when(decodeConfigOptional.get().getColumnToCheck()).thenReturn("title"); //if getColumn to check is "title"
        when(decodeConfigOptional.get().getValueToCheck()).thenReturn("sometitle"); // if value of getColumn to check is "some title"
        when(decodeConfigOptional.get().getColumnIfValueMatched()).thenReturn("status"); // then return "status" column value
        String str = dynamicLookupProtobufSchemaSerDe.getValue("title", byteArr, decodeConfigOptional, extractionNamespace);
        Assert.assertEquals("ON", str);

    }

    @Test
    public void DynamicLookupProtobufSchemaSerDeTestDecodeConfigColumnNotpresent(){
        Message adMessage = AdProtos.Ad.newBuilder().setTitle("sometitle").setStatus("ON").setLastUpdated("2021062220").build();

        byte[] byteArr = adMessage.toByteArray();
        when(decodeConfigOptional.get().getColumnToCheck()).thenReturn("title"); //if getColumn to check is "title"
        when(decodeConfigOptional.get().getValueToCheck()).thenReturn("sometitle"); // if value of getColumn to check is "some title"
        when(decodeConfigOptional.get().getColumnIfValueMatched()).thenReturn("status"); // then return "status" column value
        String str = dynamicLookupProtobufSchemaSerDe.getValue("title", byteArr, decodeConfigOptional, extractionNamespace);
        Assert.assertEquals("ON", str);

    }

    @Test
    public void DynamicLookupProtobufSchemaSerDeTestDecodeConfigValueToCheckIsEmpty(){
        Message adMessage = AdProtos.Ad.newBuilder().setTitle("sometitle").setStatus("ON").setLastUpdated("2021062220").build();

        byte[] byteArr = adMessage.toByteArray();
        when(decodeConfigOptional.get().getColumnToCheck()).thenReturn("title"); //if getColumn to check is "title"
        when(decodeConfigOptional.get().getValueToCheck()).thenReturn("sometitle"); // if value of getColumn to check is "some title"
        when(decodeConfigOptional.get().getColumnIfValueMatched()).thenReturn("status"); // then return "status" column value
        String str = dynamicLookupProtobufSchemaSerDe.getValue("title", byteArr, decodeConfigOptional, extractionNamespace);
        Assert.assertEquals("ON", str);
    }

    @Test
    public void DynamicLookupProtobufSchemaSerDeDataTypeTestSuccess() throws Exception{
        List<SchemaField> schemaFieldList = new ArrayList<>();
        schemaFieldList.add(new SchemaField("id", FieldDataType.INT32, 1));
        schemaFieldList.add(new SchemaField("budget", FieldDataType.DOUBLE, 2));
        schemaFieldList.add(new SchemaField("status", FieldDataType.BOOL, 3));
        schemaFieldList.add(new SchemaField("last_updated", FieldDataType.INT64, 4));
        DynamicLookupSchema dynamicSchema = new DynamicLookupSchema(ExtractionNameSpaceSchemaType.PROTOBUF, "2022021000", "budget_schedule_pb_lookup", schemaFieldList);

        DynamicLookupProtobufSchemaSerDe serDe = new DynamicLookupProtobufSchemaSerDe(dynamicSchema);
        Descriptors.Descriptor descriptor = serDe.getProtobufMessageDescriptor();
        Assert.assertEquals(descriptor.findFieldByName("id").getType().toString(), FieldDataType.INT32.toString());
        Assert.assertEquals(descriptor.findFieldByName("budget").getType().toString(), FieldDataType.DOUBLE.toString());
        Assert.assertEquals(descriptor.findFieldByName("status").getType().toString(), FieldDataType.BOOL.toString());
        Assert.assertEquals(descriptor.findFieldByName("last_updated").getType().toString(), FieldDataType.INT64.toString());
    }

}
