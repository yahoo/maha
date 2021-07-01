package com.yahoo.maha.maha_druid_lookups.query.lookup.schema;

import com.google.flatbuffers.FlatBufferBuilder;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupFlatbufferSchemaSerDe;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupSchema;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.FieldDataType;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.SchemaField;

import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.flatbuffer.FlatBufferValue;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.flatbuffer.ProductAdWrapper;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.mockito.Mockito.*;

public class DynamicLookupFlatbufferSchemaSerDeTestRandom {

    DynamicLookupFlatbufferSchemaSerDe dynamicLookupFlatbufferSchemaSerDe;
    DynamicLookupSchema dynamicLookupSchema;
    Optional<DecodeConfig> decodeConfigOptional ;
    RocksDBExtractionNamespace extractionNamespace;

    ByteBuffer buf;
    byte[] bytes;

    @BeforeClass
    public void setup() throws Exception {
        decodeConfigOptional = Optional.of(mock(DecodeConfig.class));
        dynamicLookupSchema = mock(DynamicLookupSchema.class);
        dynamicLookupFlatbufferSchemaSerDe = new DynamicLookupFlatbufferSchemaSerDe(dynamicLookupSchema);
        extractionNamespace = mock(RocksDBExtractionNamespace.class);

        ProductAdWrapper productAdWrapper = new ProductAdWrapper();

        Map<String, FlatBufferValue> map = new HashMap();
        map.put("id",  FlatBufferValue.of("32309719080"));
        map.put("title",  FlatBufferValue.of("some title"));
        map.put("status",  FlatBufferValue.of("ON"));
        map.put("description",  FlatBufferValue.of("test desc"));
        map.put("last_updated", FlatBufferValue.of("1480733203505"));
        map.put("image_url_hq" , FlatBufferValue.of("image_hq"));
        map.put("image_url_large", FlatBufferValue.of("image_large"));
        map.put("source_id", FlatBufferValue.of("1234"));

        FlatBufferBuilder flatBufferBuilder = productAdWrapper.createFlatBuffer(map);

        bytes = productAdWrapper.toByteArr(flatBufferBuilder.dataBuffer());

        List<SchemaField> fieldList = new ArrayList<SchemaField>();
        SchemaField schemaField = new SchemaField();
        schemaField.setIndex(4); // flatbuffer index starts at 4 and increments by 2
        schemaField.setField("id");
        schemaField.setDataType(FieldDataType.STRING);
        fieldList.add(schemaField);
        SchemaField schemaField1 = new SchemaField();
        schemaField1.setIndex(6);
        schemaField1.setField("description");
        schemaField1.setDataType(FieldDataType.STRING);
        fieldList.add(schemaField1);
        SchemaField schemaField2 = new SchemaField();
        schemaField2.setIndex(8);
        schemaField2.setField("status");
        schemaField2.setDataType(FieldDataType.STRING);
        fieldList.add(schemaField2);
        SchemaField schemaField3 = new SchemaField();
        schemaField3.setIndex(16);
        schemaField3.setField("title");
        schemaField3.setDataType(FieldDataType.STRING);
        fieldList.add(schemaField3);
        when(dynamicLookupSchema.getSchemaFieldList()).thenReturn(fieldList);
    }


    @Test
    public void DynamicLookupFlatbufferSchemaSerDeTestSuccess(){
        String str = dynamicLookupFlatbufferSchemaSerDe.getValue("id", bytes, Optional.empty(), extractionNamespace);
        System.out.println(" str " + str);
        Assert.assertEquals("32309719080", str);

        String str1 = dynamicLookupFlatbufferSchemaSerDe.getValue("title", bytes, Optional.empty(), extractionNamespace);
        System.out.println(" str1 " + str1);
        Assert.assertEquals("some title", str1);
    }


    @Test
    public void DynamicLookupFlatbufferSchemaSerDeTestFailure(){ // check for a non defined column
        String str = dynamicLookupFlatbufferSchemaSerDe.getValue("random", bytes, Optional.empty(), extractionNamespace);
        Assert.assertEquals("", str);
    }

    @Test
    public void DynamicLookupFlatbufferSchemaSerDeTestDecodeConfigValueMatched(){
        when(decodeConfigOptional.get().getColumnToCheck()).thenReturn("title"); //if getColumn to check is "title"
        when(decodeConfigOptional.get().getValueToCheck()).thenReturn("some title"); // if value of getColumn to check is "some title"
        when(decodeConfigOptional.get().getColumnIfValueMatched()).thenReturn("status"); // then return "status" column value
        String str = dynamicLookupFlatbufferSchemaSerDe.getValue("title", bytes,decodeConfigOptional, extractionNamespace);
        Assert.assertEquals("ON", str);
    }

    @Test
    public void DynamicLookupFlatbufferSchemaSerDeTestDecodeConfigValueNotMatched(){
        when(decodeConfigOptional.get().getColumnToCheck()).thenReturn("title"); //if getColumn to check is "title"
        when(decodeConfigOptional.get().getValueToCheck()).thenReturn("title"); // if value of getColumn to check is not  "some title"
        when(decodeConfigOptional.get().getColumnIfValueNotMatched()).thenReturn("description"); // then return "description" column value
        String str = dynamicLookupFlatbufferSchemaSerDe.getValue("title", bytes,decodeConfigOptional, extractionNamespace);
        Assert.assertEquals("test desc", str);
    }

    @Test
    public void DynamicLookupFlatbufferSchemaSerDeTestDecodeConfigColumnNotpresent(){
        when(decodeConfigOptional.get().getColumnToCheck()).thenReturn("random"); //if getColumn to check is not valid
        when(decodeConfigOptional.get().getValueToCheck()).thenReturn("title"); // if value of getColumn to check is not  "some title"
        when(decodeConfigOptional.get().getColumnIfValueNotMatched()).thenReturn("description"); // then return "description" column value
        String str = dynamicLookupFlatbufferSchemaSerDe.getValue("title", bytes,decodeConfigOptional, extractionNamespace);
        Assert.assertEquals("test desc", str);
    }

    @Test
    public void DynamicLookupFlatbufferSchemaSerDeTestDecodeConfigGetValueToCheckIsEmpty(){
        when(decodeConfigOptional.get().getColumnToCheck()).thenReturn("title"); //if getColumn to check is "title
        when(decodeConfigOptional.get().getValueToCheck()).thenReturn(""); // if value of getColumn to check is not  "some title"
        when(decodeConfigOptional.get().getColumnIfValueNotMatched()).thenReturn("description"); // then return "description" column value
        String str = dynamicLookupFlatbufferSchemaSerDe.getValue("title", bytes,decodeConfigOptional, extractionNamespace);
        Assert.assertEquals("test desc", str);
    }
}
