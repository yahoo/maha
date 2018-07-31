package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.metamx.emitter.service.ServiceEmitter;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace;
import io.druid.metadata.MetadataStorageConnectorConfig;
import org.joda.time.Period;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.*;

public class JDBCExtractionNamespaceCacheFactoryTest {

    @InjectMocks
    JDBCExtractionNamespaceCacheFactory obj = new JDBCExtractionNamespaceCacheFactory();

    @Mock
    ServiceEmitter serviceEmitter;

    @Mock
    LookupService lookupService;

    @BeforeTest
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        obj.emitter = serviceEmitter;
        obj.lookupService = lookupService;
    }

    @Test
    public void testGetCacheValueWhenKeyPresent() throws Exception{
        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(
                        metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id","name","currency","status")),
                        "id", "", new Period(), true, "advertiser_lookup");
        Map<String, List<String>> map = new HashMap<>();
        map.put("12345", Arrays.asList("12345", "my name", "USD", "ON"));
        Assert.assertEquals(obj.getCacheValue(extractionNamespace, map, "12345", "name", Optional.empty()), "my name".getBytes());
    }

    @Test
    public void testGetCacheValueWhenKeyNotPresent() throws Exception{
        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(
                        metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id","name","currency","status")),
                        "id", "", new Period(), true, "advertiser_lookup");
        Map<String, List<String>> map = new HashMap<>();
        map.put("12345", Arrays.asList("12345", "my name", "USD", "ON"));
        Assert.assertEquals(obj.getCacheValue(extractionNamespace, map, "6789", "name", Optional.empty()), "".getBytes());
    }

    @Test
    public void testGetCacheValueWhenKeyPresentButValueColumnNotPresent() throws Exception{
        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(
                        metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id","name","currency","status")),
                        "id", "", new Period(), true, "advertiser_lookup");
        Map<String, List<String>> map = new HashMap<>();
        map.put("12345", Arrays.asList("12345", "my name", "USD", "ON"));
        Assert.assertEquals(obj.getCacheValue(extractionNamespace, map, "6789", "blah", Optional.empty()), "".getBytes());
    }

    @Test
    public void testGetCacheValueWithDecodeConfig() throws Exception{
        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(
                        metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id","name","currency","status")),
                        "id", "", new Period(), true, "advertiser_lookup");
        Map<String, List<String>> map = new HashMap<>();
        map.put("12345", Arrays.asList("12345", "my name", "USD", "ON"));
        DecodeConfig decodeConfig1 = new DecodeConfig();
        decodeConfig1.setColumnToCheck("name");
        decodeConfig1.setValueToCheck("my name");
        decodeConfig1.setColumnIfValueMatched("currency");
        decodeConfig1.setColumnIfValueNotMatched("status");
        Assert.assertEquals(obj.getCacheValue(extractionNamespace, map, "12345", "name", Optional.of(decodeConfig1)), "USD".getBytes());

        DecodeConfig decodeConfig2 = new DecodeConfig();
        decodeConfig2.setColumnToCheck("name");
        decodeConfig2.setValueToCheck("my unknown name");
        decodeConfig2.setColumnIfValueMatched("currency");
        decodeConfig2.setColumnIfValueNotMatched("status");
        Assert.assertEquals(obj.getCacheValue(extractionNamespace, map, "12345", "name", Optional.of(decodeConfig2)), "ON".getBytes());
    }
}
