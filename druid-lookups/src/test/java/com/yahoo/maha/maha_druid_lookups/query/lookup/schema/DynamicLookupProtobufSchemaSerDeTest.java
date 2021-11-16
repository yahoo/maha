package com.yahoo.maha.maha_druid_lookups.query.lookup.schema;


import com.google.protobuf.Message;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupProtobufSchemaSerDe;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupSchema;
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

}
