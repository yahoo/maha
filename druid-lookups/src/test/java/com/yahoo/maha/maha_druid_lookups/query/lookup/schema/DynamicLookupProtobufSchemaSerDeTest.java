package com.yahoo.maha.maha_druid_lookups.query.lookup.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupProtobufSchemaSerDe;

import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.AdProtos;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.protobuf.*;

import java.io.FileNotFoundException;
import java.util.Optional;

import static org.mockito.Mockito.*;

public class DynamicLookupProtobufSchemaSerDeTest {
    private DynamicLookupProtobufSchemaSerDe dynamicLookupProtobufSchemaSerDe;
    RocksDBExtractionNamespace rocksDBExtractionNamespace;

    @BeforeClass
    public void setup() throws Exception {
        String path = Thread.currentThread().getContextClassLoader()
                .getResource("./dynamic/schema/Ad.desc").getPath();
        String coreSchema = "{\"descFilePath\" : \"" + path + "\" }"; // works in local
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.readTree(coreSchema);
        dynamicLookupProtobufSchemaSerDe = spy(new DynamicLookupProtobufSchemaSerDe(node));

        rocksDBExtractionNamespace = mock(RocksDBExtractionNamespace.class);
        when(rocksDBExtractionNamespace.getTsColumn()).thenReturn("last_updated");
        when(rocksDBExtractionNamespace.getLookupName()).thenReturn("ad_lookup");
    }

    @Test
    public void DynamicLookupProtobufSchemaSerDeTestgetValue() throws FileNotFoundException {

        Message adMessage = AdProtos.Ad.newBuilder().setStatus("ON").setLastUpdated("2021062220").build();

        byte[] byteArr = adMessage.toByteArray();
        String statusValue = dynamicLookupProtobufSchemaSerDe.getValue("status", byteArr, Optional.empty(), rocksDBExtractionNamespace);
        Assert.assertEquals(statusValue, "ON");

        String last_updatedValue = dynamicLookupProtobufSchemaSerDe.getValue("last_updated", byteArr, Optional.empty(), rocksDBExtractionNamespace);
        Assert.assertEquals(last_updatedValue, "2021062220");

    }


    @Test
    public void DynamicLookupProtobufSchemaSerDeTestgetValueFailed() {

        Message adMessage = AdProtos.Ad.newBuilder().setStatus("OFF").build();

        byte[] byteArr = adMessage.toByteArray();

        String titleValue = dynamicLookupProtobufSchemaSerDe.getValue("title", byteArr, Optional.empty(), rocksDBExtractionNamespace);

        Assert.assertEquals(titleValue, "");
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void DynamicLookupProtobufSchemaSerDeTestgetValueFailedNoField() {
        Message adMessage = AdProtos.Ad.newBuilder().setStatus("OFF").build();
        byte[] byteArr = adMessage.toByteArray();
        dynamicLookupProtobufSchemaSerDe.getValue("random", byteArr, Optional.empty(), rocksDBExtractionNamespace);
    }
}
