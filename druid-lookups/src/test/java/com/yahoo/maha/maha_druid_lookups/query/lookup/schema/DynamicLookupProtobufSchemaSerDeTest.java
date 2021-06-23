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
        String coreSchema = "{\"descFilePath\" : \""  + path+ "\" }"; // works in local
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.readTree(coreSchema);
        dynamicLookupProtobufSchemaSerDe = spy(new DynamicLookupProtobufSchemaSerDe(node));

        rocksDBExtractionNamespace  = mock(RocksDBExtractionNamespace.class);
        when(rocksDBExtractionNamespace.getTsColumn()).thenReturn("last_updated");
    }

    @Test
    public void DynamicLookupProtobufSchemaSerDeTestgetValue() throws FileNotFoundException {

        Message adMessage = AdProtos.Ad.newBuilder().setStatus("ON").setLastUpdated("2021062220").build();

        byte[] byteArr =  adMessage.toByteArray();
        ImmutablePair<String, Optional<Long>>  pair = dynamicLookupProtobufSchemaSerDe.getValue("status", byteArr,rocksDBExtractionNamespace);

        Assert.assertEquals(pair.getLeft(), "ON");
        Assert.assertEquals(pair.getRight(),Optional.of(2021062220L));
    }


    @Test
    public void DynamicLookupProtobufSchemaSerDeTestgetValueFailed(){

        Message adMessage = AdProtos.Ad.newBuilder().setStatus("OFF").build();

        byte[] byteArr =  adMessage.toByteArray();

        ImmutablePair<String, Optional<Long>>  pair = dynamicLookupProtobufSchemaSerDe.getValue("title", byteArr,rocksDBExtractionNamespace);

        Assert.assertNotEquals(pair.getLeft(), "ON");
        Assert.assertEquals(pair.getRight(),Optional.of(0L));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void DynamicLookupProtobufSchemaSerDeTestgetValueFailedNoField(){

        Message adMessage = AdProtos.Ad.newBuilder().setStatus("OFF").build();

        byte[] byteArr =  adMessage.toByteArray();
        ImmutablePair<String, Optional<Long>>  pair = dynamicLookupProtobufSchemaSerDe.getValue("random", byteArr,rocksDBExtractionNamespace);

        Assert.assertNotEquals(pair.getLeft(), "ON");
        Assert.assertEquals(pair.getRight(),Optional.of(0L));
    }
}
