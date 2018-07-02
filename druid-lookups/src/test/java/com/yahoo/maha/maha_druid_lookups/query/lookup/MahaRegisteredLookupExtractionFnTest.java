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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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

        Map<String, String> map = new HashMap<>();
        JDBCLookupExtractor jdbcLookupExtractor = new JDBCLookupExtractor(extractionNamespace, map, lookupService);

        LookupExtractorFactory lef = mock(LookupExtractorFactory.class);
        when(lef.get()).thenReturn(jdbcLookupExtractor);

        LookupExtractorFactoryContainer lefc = mock(LookupExtractorFactoryContainer.class);
        when(lefc.getLookupExtractorFactory()).thenReturn(lef);
        LookupReferencesManager lrm = mock(LookupReferencesManager.class);
        when(lrm.get(anyString())).thenReturn(lefc);

        MahaRegisteredLookupExtractionFn fn = spy(new MahaRegisteredLookupExtractionFn(lrm, objectMapper, "advertiser_lookup", false, "", false, false, "status", null, null));
        fn.apply("123");
        Assert.assertEquals(fn.cache.getIfPresent("123"), "{\"dimension\":\"123\",\"valueColumn\":\"status\",\"decodeConfig\":null,\"dimensionOverrideMap\":null}");
        verify(fn, times(1)).populateCacheAndGetSerializedElement(anyString());
    }

    @Test
    public void testWhenCacheValueIsPresent() {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id","name","currency","status")),
                        "id", "", new Period(), true, "advertiser_lookup");

        Map<String, String> map = new HashMap<>();
        JDBCLookupExtractor jdbcLookupExtractor = new JDBCLookupExtractor(extractionNamespace, map, lookupService);

        LookupExtractorFactory lef = mock(LookupExtractorFactory.class);
        when(lef.get()).thenReturn(jdbcLookupExtractor);

        LookupExtractorFactoryContainer lefc = mock(LookupExtractorFactoryContainer.class);
        when(lefc.getLookupExtractorFactory()).thenReturn(lef);
        LookupReferencesManager lrm = mock(LookupReferencesManager.class);
        when(lrm.get(anyString())).thenReturn(lefc);

        MahaRegisteredLookupExtractionFn fn = spy(new MahaRegisteredLookupExtractionFn(lrm, objectMapper, "advertiser_lookup", false, "", false, false, "status", null, null));

        fn.cache.put("123", "{\"dimension\":\"123\",\"valueColumn\":\"status\",\"decodeConfig\":null,\"dimensionOverrideMap\":null}");

        fn.apply("123");
        Assert.assertEquals(fn.cache.getIfPresent("123"), "{\"dimension\":\"123\",\"valueColumn\":\"status\",\"decodeConfig\":null,\"dimensionOverrideMap\":null}");
        verify(fn, times(0)).populateCacheAndGetSerializedElement(anyString());
    }
}
