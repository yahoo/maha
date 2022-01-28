// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.google.common.collect.ImmutableMap;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.TsColumnConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.URIExtractionNamespace;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.SearchableVersionedDataFinder;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.segment.loading.LocalFileTimestampVersionFinder;
import org.joda.time.Period;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.Callable;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;

public class URIExtractionNamespaceCacheFactoryTest {
    private Lifecycle lifecycle;
    private static final String FAKE_SCHEME = "wabblywoo";
    private static final Map<String, SearchableVersionedDataFinder> FINDERS = ImmutableMap.of(
            "file",
            new LocalFileTimestampVersionFinder(),
            FAKE_SCHEME,
            new LocalFileTimestampVersionFinder()
            {
                URI fixURI(URI uri)
                {
                    final URI newURI;
                    try {
                        newURI = new URI(
                                "file",
                                uri.getUserInfo(),
                                uri.getHost(),
                                uri.getPort(),
                                uri.getPath(),
                                uri.getQuery(),
                                uri.getFragment()
                        );
                    }
                    catch (URISyntaxException e) {
                        throw new RuntimeException(e);
                    }
                    return newURI;
                }

                @Override
                public String getVersion(URI uri)
                {
                    return super.getVersion(fixURI(uri));
                }

                @Override
                public InputStream getInputStream(URI uri) throws IOException
                {
                    return super.getInputStream(fixURI(uri));
                }
            }
    );

    private File tmpFile;
    private File tmpFileParent;
    private final String suffix = ".txt";

    @Spy
    @InjectMocks
    URIExtractionNamespaceCacheFactory obj = new URIExtractionNamespaceCacheFactory(FINDERS);

    URIExtractionNamespace namespace;

    @Mock
    ServiceEmitter serviceEmitter;

    @Mock
    LookupService lookupService;

    @BeforeTest
    public void setUp() throws Exception {
        Path path = Files.createTempDirectory("druidTest");
        this.lifecycle = new Lifecycle();
        lifecycle.start();
        MockitoAnnotations.initMocks(this);
        obj.emitter = serviceEmitter;
        obj.lookupService = lookupService;
        NullHandling.initializeForTests();
        File newFolder = path.toFile();

        tmpFileParent = new File(newFolder, "tmp");
        //tmpFile = Files.createTempFile(tmpFileParent.toPath(), "druidTestURIExtractionNS", suffix).toFile();
        namespace = new URIExtractionNamespace(tmpFileParent.toURI(), null, null,
                new URIExtractionNamespace.CSVFlatDataParser(Arrays.asList("id", "gpa", "date"), "id", "gpa", false, 0),
                null, null, 10L, "student_lookup", "date", true, "id",
                new ArrayList<String>(Arrays.asList("id", "gpa", "date")), null);
    }

    @AfterTest
    public void tearDown() throws Exception {
        lifecycle.stop();
    }

    @Test
    public void testGetCacheValueWhenKeyPresent() throws Exception{
        Callable<String> versionedCache = obj.getCachePopulator("blah",
                namespace, "500", new HashMap<String, String>(){{put("id", "123");put("gpa", "4.5");put("date", "20220101");}});

        System.err.println(versionedCache.call());
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
    }

}
