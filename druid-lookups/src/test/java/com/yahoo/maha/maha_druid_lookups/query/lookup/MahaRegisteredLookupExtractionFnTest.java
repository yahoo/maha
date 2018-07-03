// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.query.lookup.LookupExtractorFactory;
import io.druid.query.lookup.LookupExtractorFactoryContainer;
import io.druid.query.lookup.LookupReferencesManager;
import org.joda.time.Period;
import org.mockito.Mock;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class MahaRegisteredLookupExtractionFnTest {

    ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    LookupService lookupService;

    @Test
    public void testWhenCacheValueIsEmpty() {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id","name","currency","status")),
                        "id", "", new Period(), true, "advertiser_lookup");

        Map<String, List<String>> map = new HashMap<>();
        map.put("123", Arrays.asList("123", "some name", "USD", "ON"));
        JDBCLookupExtractor jdbcLookupExtractor = new JDBCLookupExtractor(extractionNamespace, map, lookupService);

        LookupExtractorFactory lef = mock(LookupExtractorFactory.class);
        when(lef.get()).thenReturn(jdbcLookupExtractor);

        LookupExtractorFactoryContainer lefc = mock(LookupExtractorFactoryContainer.class);
        when(lefc.getLookupExtractorFactory()).thenReturn(lef);
        LookupReferencesManager lrm = mock(LookupReferencesManager.class);
        when(lrm.get(anyString())).thenReturn(lefc);

        MahaRegisteredLookupExtractionFn fn = spy(new MahaRegisteredLookupExtractionFn(lrm, objectMapper, "advertiser_lookup", false, "", false, false, "status", null, null, true));
        fn.apply("123");
        Assert.assertEquals(fn.cache.getIfPresent("123"), "{\"dimension\":\"123\",\"valueColumn\":\"status\",\"decodeConfig\":null,\"dimensionOverrideMap\":null}");
        verify(fn, times(1)).getSerializedLookupQueryElement(anyString());
    }

    @Test
    public void testWhenCacheValueIsPresent() {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id","name","currency","status")),
                        "id", "", new Period(), true, "advertiser_lookup");

        Map<String, List<String>> map = new HashMap<>();
        map.put("123", Arrays.asList("123", "some name", "USD", "ON"));
        JDBCLookupExtractor jdbcLookupExtractor = new JDBCLookupExtractor(extractionNamespace, map, lookupService);

        LookupExtractorFactory lef = mock(LookupExtractorFactory.class);
        when(lef.get()).thenReturn(jdbcLookupExtractor);

        LookupExtractorFactoryContainer lefc = mock(LookupExtractorFactoryContainer.class);
        when(lefc.getLookupExtractorFactory()).thenReturn(lef);
        LookupReferencesManager lrm = mock(LookupReferencesManager.class);
        when(lrm.get(anyString())).thenReturn(lefc);

        MahaRegisteredLookupExtractionFn fn = spy(new MahaRegisteredLookupExtractionFn(lrm, objectMapper, "advertiser_lookup", false, "", false, false, "status", null, null, true));

        fn.cache.put("123", "{\"dimension\":\"123\",\"valueColumn\":\"status\",\"decodeConfig\":null,\"dimensionOverrideMap\":null}");

        fn.apply("123");
        Assert.assertEquals(fn.cache.getIfPresent("123"), "{\"dimension\":\"123\",\"valueColumn\":\"status\",\"decodeConfig\":null,\"dimensionOverrideMap\":null}");
        verify(fn, times(0)).getSerializedLookupQueryElement(anyString());
    }

    @Test
    public void testWhenUseQueryLevelCacheIsFalse() {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id","name","currency","status")),
                        "id", "", new Period(), true, "advertiser_lookup");

        Map<String, List<String>> map = new HashMap<>();
        map.put("123", Arrays.asList("123", "some name", "USD", "ON"));
        JDBCLookupExtractor<List<String>> jdbcLookupExtractor = new JDBCLookupExtractor(extractionNamespace, map, lookupService);

        LookupExtractorFactory lef = mock(LookupExtractorFactory.class);
        when(lef.get()).thenReturn(jdbcLookupExtractor);

        LookupExtractorFactoryContainer lefc = mock(LookupExtractorFactoryContainer.class);
        when(lefc.getLookupExtractorFactory()).thenReturn(lef);
        LookupReferencesManager lrm = mock(LookupReferencesManager.class);
        when(lrm.get(anyString())).thenReturn(lefc);

        MahaRegisteredLookupExtractionFn fn = spy(new MahaRegisteredLookupExtractionFn(lrm, objectMapper, "advertiser_lookup", false, "", false, false, "status", null, null, false));

        fn.apply("123");
        Assert.assertNull(fn.cache);
        verify(fn, times(1)).getSerializedLookupQueryElement(anyString());
    }
}
