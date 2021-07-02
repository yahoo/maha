package com.yahoo.maha.maha_druid_lookups.query.lookup.schema;

import com.google.flatbuffers.FlatBufferBuilder;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupFlatbufferSchemaSerDe;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupSchema;

import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.flatbuffer.FlatBufferValue;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.flatbuffer.ProductAdWrapper;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;

import static org.mockito.Mockito.*;

public class DynamicLookupFlatbufferSchemaSerDeTest {

    DynamicLookupFlatbufferSchemaSerDe dynamicLookupFlatbufferSchemaSerDe;
    DynamicLookupSchema dynamicLookupSchema;
    Optional<DecodeConfig> decodeConfigOptional ;
    RocksDBExtractionNamespace extractionNamespace;
    private final String dir = "./dynamic/schema/";
    private  ClassLoader classLoader ;

    ByteBuffer buf;
    byte[] bytes;

    @BeforeClass
    public void setup() throws Exception {
        decodeConfigOptional = Optional.of(mock(DecodeConfig.class));
        classLoader = Thread.currentThread().getContextClassLoader();
        File schemaFile = new File(classLoader.getResource(dir + "dynamic_lookup_fb_schema.json").getPath());
        Optional<DynamicLookupSchema> dynamicLookupSchemaOptional = DynamicLookupSchema.parseFrom(schemaFile);
        Assert.assertTrue(dynamicLookupSchemaOptional.isPresent());
        dynamicLookupSchema = dynamicLookupSchemaOptional.get();
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

    }


    @Test
    public void DynamicLookupFlatbufferSchemaSerDeTestSuccess(){
        String str = dynamicLookupFlatbufferSchemaSerDe.getValue("id", bytes, Optional.empty(), extractionNamespace);
        Assert.assertEquals("32309719080", str);

        String str1 = dynamicLookupFlatbufferSchemaSerDe.getValue("title", bytes, Optional.empty(), extractionNamespace);
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
        when(decodeConfigOptional.get().getColumnIfValueNotMatched()).thenReturn("last_updated"); // then return "last_updated" column value
        String str = dynamicLookupFlatbufferSchemaSerDe.getValue("title", bytes,decodeConfigOptional, extractionNamespace);
        Assert.assertEquals("1480733203505", str);
    }

    @Test
    public void DynamicLookupFlatbufferSchemaSerDeTestDecodeConfigColumnNotpresent(){
        when(decodeConfigOptional.get().getColumnToCheck()).thenReturn("random"); //if getColumn to check is not valid
        when(decodeConfigOptional.get().getValueToCheck()).thenReturn("title"); // if value of getColumn to check is not  "some title"
        when(decodeConfigOptional.get().getColumnIfValueNotMatched()).thenReturn("last_updated"); // then return "last_updated" column value
        String str = dynamicLookupFlatbufferSchemaSerDe.getValue("title", bytes,decodeConfigOptional, extractionNamespace);
        Assert.assertEquals("1480733203505", str);
    }

    @Test
    public void DynamicLookupFlatbufferSchemaSerDeTestDecodeConfigGetValueToCheckIsEmpty(){
        when(decodeConfigOptional.get().getColumnToCheck()).thenReturn("title"); //if getColumn to check is "title
        when(decodeConfigOptional.get().getValueToCheck()).thenReturn(""); // if value of getColumn to check is not  "some title"
        when(decodeConfigOptional.get().getColumnIfValueNotMatched()).thenReturn("last_updated"); // then return "last_updated" column value
        String str = dynamicLookupFlatbufferSchemaSerDe.getValue("title", bytes,decodeConfigOptional, extractionNamespace);
        Assert.assertEquals("1480733203505", str);
    }
}
