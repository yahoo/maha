package com.yahoo.maha.maha_druid_lookups.query.lookup.schema;
/* 
Created by lgadde on 7/1/21 
*/

import com.google.protobuf.Message;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupProtobufSchemaSerDe;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupSchema;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.FieldDataType;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.SchemaField;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.AdProtos;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DynamicLookupProtobufSchemaSerDeTestRandom {

    DynamicLookupProtobufSchemaSerDe dynamicLookupProtobufSchemaSerDe;
    DynamicLookupSchema dynamicLookupSchema;
    RocksDBExtractionNamespace extractionNamespace;
    Optional<DecodeConfig> decodeConfigOptional;

    @BeforeClass
    public void setup() throws Exception {
        decodeConfigOptional = Optional.of(mock(DecodeConfig.class));
        dynamicLookupSchema = mock(DynamicLookupSchema.class);
        when(dynamicLookupSchema.getName()).thenReturn("ad_lookup");

        extractionNamespace = mock(RocksDBExtractionNamespace.class);

        List<SchemaField> fieldList = new ArrayList<SchemaField>();
        SchemaField schemaField = new SchemaField();
        schemaField.setIndex(1); // protobuf index starts at 1 and increments by 1
        schemaField.setField("id");
        schemaField.setDataType(FieldDataType.STRING);
        fieldList.add(schemaField);
        SchemaField schemaField1 = new SchemaField();
        schemaField1.setIndex(2);
        schemaField1.setField("title");
        schemaField1.setDataType(FieldDataType.STRING);
        fieldList.add(schemaField1);
        SchemaField schemaField2 = new SchemaField();
        schemaField2.setIndex(3);
        schemaField2.setField("status");
        schemaField2.setDataType(FieldDataType.STRING);
        fieldList.add(schemaField2);
        SchemaField schemaField3 = new SchemaField();
        schemaField3.setIndex(4);
        schemaField3.setField("last_updated");
        schemaField3.setDataType(FieldDataType.STRING);
        fieldList.add(schemaField3);
        when(dynamicLookupSchema.getSchemaFieldList()).thenReturn(fieldList);
        dynamicLookupProtobufSchemaSerDe = new DynamicLookupProtobufSchemaSerDe(dynamicLookupSchema);

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
        String str = dynamicLookupProtobufSchemaSerDe.getValue("title", byteArr, Optional.empty(), extractionNamespace);
        Assert.assertEquals("ON", str);

    }


}
