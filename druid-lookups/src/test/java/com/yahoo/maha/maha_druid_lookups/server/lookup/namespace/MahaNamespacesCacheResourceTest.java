// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.emitter.service.ServiceEmitter;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache.OnHeapMahaNamespaceExtractionCacheManager;
import io.druid.metadata.MetadataStorageConnectorConfig;
import org.joda.time.Period;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MahaNamespacesCacheResourceTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testGetCacheValue() throws Exception {
        OnHeapMahaNamespaceExtractionCacheManager cacheManager = mock(OnHeapMahaNamespaceExtractionCacheManager.class);
        ServiceEmitter serviceEmitter = mock(ServiceEmitter.class);
        HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
        MahaNamespacesCacheResource resource = new MahaNamespacesCacheResource(cacheManager, serviceEmitter);

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(
                        metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id","name","currency","status")),
                        "id", "", null, null, new Period(), true, false, "advertiser_lookup");

        when(cacheManager.getExtractionNamespace(anyString())).thenReturn(Optional.of(extractionNamespace));
        when(cacheManager.getExtractionNamespaceFunctionFactory(any())).thenReturn(new JDBCExtractionNamespaceCacheFactory());

        ConcurrentMap<String, List<String>> map = new ConcurrentHashMap<>();
        map.put("12345", Arrays.asList("12345", "my name", "USD", "ON"));

        when(cacheManager.getCacheMap(anyString())).thenReturn(map);
        Response response = resource.getCacheValue("advertiser_lookup",
                "com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace",
                "12345",
                "name",
                null, false, httpServletRequest);

        Assert.assertEquals(response.getStatus(), 200);

        byte[] byteArray = (byte[])response.getEntity();
        Assert.assertEquals(byteArray, "my name".getBytes());
    }

    @Test
    public void testGetCacheValueWhenDecodeConfigPresent() throws Exception {
        OnHeapMahaNamespaceExtractionCacheManager cacheManager = mock(OnHeapMahaNamespaceExtractionCacheManager.class);
        ServiceEmitter serviceEmitter = mock(ServiceEmitter.class);
        HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
        MahaNamespacesCacheResource resource = new MahaNamespacesCacheResource(cacheManager, serviceEmitter);

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(
                        metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id","name","currency","status")),
                        "id", "", null, null, new Period(), true, false, "advertiser_lookup");

        when(cacheManager.getExtractionNamespace(anyString())).thenReturn(Optional.of(extractionNamespace));
        when(cacheManager.getExtractionNamespaceFunctionFactory(any())).thenReturn(new JDBCExtractionNamespaceCacheFactory());

        ConcurrentMap<String, List<String>> map = new ConcurrentHashMap<>();
        map.put("12345", Arrays.asList("12345", "my name", "USD", "ON"));

        when(cacheManager.getCacheMap(anyString())).thenReturn(map);

        DecodeConfig decodeConfig1 = new DecodeConfig();
        decodeConfig1.setColumnToCheck("name");
        decodeConfig1.setValueToCheck("my name");
        decodeConfig1.setColumnIfValueMatched("currency");
        decodeConfig1.setColumnIfValueNotMatched("status");

        Response response = resource.getCacheValue("advertiser_lookup",
                "com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace",
                "12345",
                "name",
                URLEncoder.encode(objectMapper.writeValueAsString(decodeConfig1), "utf-8"), false, httpServletRequest);

        Assert.assertEquals(response.getStatus(), 200);

        byte[] byteArray = (byte[])response.getEntity();
        Assert.assertEquals(byteArray, "USD".getBytes());

        DecodeConfig decodeConfig2 = new DecodeConfig();
        decodeConfig2.setColumnToCheck("name");
        decodeConfig2.setValueToCheck("my unknown name");
        decodeConfig2.setColumnIfValueMatched("currency");
        decodeConfig2.setColumnIfValueNotMatched("status");

        response = resource.getCacheValue("advertiser_lookup",
                "com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace",
                "12345",
                "name",
                URLEncoder.encode(objectMapper.writeValueAsString(decodeConfig2), "utf-8"), false, httpServletRequest);

        Assert.assertEquals(response.getStatus(), 200);

        byteArray = (byte[])response.getEntity();
        Assert.assertEquals(byteArray, "ON".getBytes());
    }
}
