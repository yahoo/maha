// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespaceWithLeaderAndFollower;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.query.lookup.LookupExtractorFactory;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupReferencesManager;
import org.joda.time.Period;
import org.mockito.Mock;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.*;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class MahaRegisteredLookupExtractionFnTest {

    ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Mock
    LookupService lookupService;

    @BeforeTest
    public void before() {
        NullHandling.initializeForTests();
    }

    @Test
    public void testWhenCacheValueIsEmpty() {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id", "name", "currency", "status")),
                        "id", "", new Period(), true, "advertiser_lookup");

        Map<String, List<String>> map = new HashMap<>();
        map.put("123", Arrays.asList("123", "some name", "USD", "ON"));
        JDBCLookupExtractor jdbcLookupExtractor = new JDBCLookupExtractor(extractionNamespace, map, lookupService);

        LookupExtractorFactory lef = mock(LookupExtractorFactory.class);
        when(lef.get()).thenReturn(jdbcLookupExtractor);

        LookupExtractorFactoryContainer lefc = mock(LookupExtractorFactoryContainer.class);
        Optional<LookupExtractorFactoryContainer> lefcOptional = Optional.of(lefc);
        when(lefc.getLookupExtractorFactory()).thenReturn(lef);
        LookupReferencesManager lrm = mock(LookupReferencesManager.class);
        when(lrm.get(anyString())).thenReturn(lefcOptional);

        MahaRegisteredLookupExtractionFn fn = spy(new MahaRegisteredLookupExtractionFn(lrm, "advertiser_lookup", false, "", false, false, "status", null, null, null, true));
        Assert.assertNull(fn.cache);
        Assert.assertEquals(fn.apply("123"), "ON");
        Assert.assertEquals(fn.cache.getIfPresent("123"), "ON");
    }

    @Test
    public void testWhenCacheValueIsPresent() {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id", "name", "currency", "status")),
                        "id", "", new Period(), true, "advertiser_lookup");

        Map<String, List<String>> map = new HashMap<>();
        map.put("123", Arrays.asList("123", "some name", "USD", "ON"));
        JDBCLookupExtractor jdbcLookupExtractor = new JDBCLookupExtractor(extractionNamespace, map, lookupService);

        LookupExtractorFactory lef = mock(LookupExtractorFactory.class);
        when(lef.get()).thenReturn(jdbcLookupExtractor);

        LookupExtractorFactoryContainer lefc = mock(LookupExtractorFactoryContainer.class);
        Optional<LookupExtractorFactoryContainer> lefcOptional = Optional.of(lefc);
        when(lefc.getLookupExtractorFactory()).thenReturn(lef);
        LookupReferencesManager lrm = mock(LookupReferencesManager.class);
        when(lrm.get(anyString())).thenReturn(lefcOptional);

        MahaRegisteredLookupExtractionFn fn = spy(new MahaRegisteredLookupExtractionFn(lrm, "advertiser_lookup", false, "", false, false, "status", null, null, null, true));

        fn.ensureCache().put("123", "hola");

        Assert.assertEquals(fn.apply("123"), "hola");
        Assert.assertEquals(fn.cache.getIfPresent("123"), "hola");
    }

    @Test
    public void testWhenUseQueryLevelCacheIsFalse() {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id", "name", "currency", "status")),
                        "id", "", new Period(), true, "advertiser_lookup");

        Map<String, List<String>> map = new HashMap<>();
        map.put("123", Arrays.asList("123", "some name", "USD", "ON"));
        JDBCLookupExtractor<List<String>> jdbcLookupExtractor = new JDBCLookupExtractor(extractionNamespace, map, lookupService);

        LookupExtractorFactory lef = mock(LookupExtractorFactory.class);
        when(lef.get()).thenReturn(jdbcLookupExtractor);

        LookupExtractorFactoryContainer lefc = mock(LookupExtractorFactoryContainer.class);
        Optional<LookupExtractorFactoryContainer> lefcOptional = Optional.of(lefc);
        when(lefc.getLookupExtractorFactory()).thenReturn(lef);
        LookupReferencesManager lrm = mock(LookupReferencesManager.class);
        when(lrm.get(anyString())).thenReturn(lefcOptional);

        MahaRegisteredLookupExtractionFn fn = spy(new MahaRegisteredLookupExtractionFn(lrm, "advertiser_lookup", false, "", false, false, "status", null, null, null, false));

        Assert.assertNull(fn.cache);
        Assert.assertEquals(fn.apply("123"), "ON");
        Assert.assertNull(fn.cache);
    }

    @Test
    public void testWhenNullValueIsPresent() {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id", "name", "currency", "status")),
                        "id", "", new Period(), true, "advertiser_lookup");

        Map<String, List<String>> map = new HashMap<>();
        map.put("123", Arrays.asList("123", "some name", "USD", "ON"));
        JDBCLookupExtractor jdbcLookupExtractor = new JDBCLookupExtractor(extractionNamespace, map, lookupService);

        LookupExtractorFactory lef = mock(LookupExtractorFactory.class);
        when(lef.get()).thenReturn(jdbcLookupExtractor);

        LookupExtractorFactoryContainer lefc = mock(LookupExtractorFactoryContainer.class);
        Optional<LookupExtractorFactoryContainer> lefcOptional = Optional.of(lefc);
        when(lefc.getLookupExtractorFactory()).thenReturn(lef);
        LookupReferencesManager lrm = mock(LookupReferencesManager.class);
        when(lrm.get(anyString())).thenReturn(lefcOptional);

        MahaRegisteredLookupExtractionFn fn = spy(new MahaRegisteredLookupExtractionFn(lrm, "advertiser_lookup", false, "", false, false, "status", null, null, null, true));

        Assert.assertNull(fn.apply(null));
    }
}
