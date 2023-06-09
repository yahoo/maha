
// Copyright 2022, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.google.common.collect.ImmutableMap;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.URIExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.query.lookup.parser.CSVFlatDataParser;
import com.yahoo.maha.maha_druid_lookups.query.lookup.parser.TSVFlatDataParser;
import org.apache.commons.io.FileUtils;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.SearchableVersionedDataFinder;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.segment.loading.LocalFileTimestampVersionFinder;
import org.apache.druid.storage.hdfs.HdfsFileTimestampVersionFinder;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.Period;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.Optional;
import java.util.zip.GZIPOutputStream;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;

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
            },
            "hdfs",
            new HdfsFileTimestampVersionFinder(new Configuration(true))
    );

    private File tmpFileParent;
    private File tmpFileParent2;

    @Spy
    @InjectMocks
    URIExtractionNamespaceCacheFactory obj = new URIExtractionNamespaceCacheFactory(FINDERS);

    URIExtractionNamespace namespace;

    @Mock
    ServiceEmitter serviceEmitter;

    @Mock
    LookupService lookupService;

    @BeforeMethod
    public void setUp() throws Exception {
        Path path = Files.createTempDirectory("druidTest");
        this.lifecycle = new Lifecycle();
        lifecycle.start();
        MockitoAnnotations.initMocks(this);
        obj.emitter = serviceEmitter;
        obj.lookupService = lookupService;
        NullHandling.initializeForTests();
        ExpressionProcessing.initializeForTests();
        ExpressionProcessing.initializeForStrictBooleansTests(false);

        File newFolder = path.toFile();
        tmpFileParent = new File(newFolder, "tmp.txt");
        tmpFileParent.createNewFile();
        tmpFileParent2 = new File(newFolder, "tmp2.txt");
        tmpFileParent2.createNewFile();

    }

    @AfterMethod
    public void tearDown() throws Exception {
        tmpFileParent.delete();
        tmpFileParent2.delete();
        lifecycle.stop();
    }

    @Test
    public void testGetCacheValueWhenKeyPresent() throws Exception{
        namespace = new URIExtractionNamespace(tmpFileParent.toURI(), null, null,
                new CSVFlatDataParser(Arrays.asList("id", "gpa", "date"), "id", false, 0, null),
                null, null, 10L, "student_lookup", "date", true, null, null);

        tmpFileParent.setWritable(true);
        FileUtils.writeStringToFile(tmpFileParent, "543,0.2,20220102\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "111,0.3,22220103\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "222,3.9,20220104\n", true);
        tmpFileParent.setLastModified(8675309123L);
        Map<String, List<String>> cache = new HashMap<String, List<String>>(){{put("123", Arrays.asList("123", "4.5", "20220101"));}};
        Callable<String> versionedCache = obj.getCachePopulator("blah",
                namespace, "500", cache);

        Assert.assertEquals(versionedCache.call(), "8675309000", versionedCache.call());
        Assert.assertEquals(cache.size() , 3);
        Assert.assertTrue(cache.containsKey("222"));
        Assert.assertTrue(cache.containsValue(Arrays.asList("111", "0.3", "22220103")));
        Assert.assertTrue(!cache.containsKey("123"));
    }

    @Test
    public void testGetRegexCacheOnFileRegex() throws Exception{
        namespace = new URIExtractionNamespace(null, tmpFileParent.getParentFile().toURI(), ".*tmp.*txt", //want the regex to match all tmp*txt files
                new CSVFlatDataParser(Arrays.asList("name","country","subcountry","geonameid"), "name", true, 0, null),
                null, null, 10L, "cities_lookup", "date", true, null, null);

        Map<String, List<String>> cache = new HashMap<String, List<String>>();
        Callable<String> versionedCache = obj.getCachePopulator("blah",
                namespace, "500", cache);

        tmpFileParent.setWritable(true);
        FileUtils.writeStringToFile(tmpFileParent, "name,country,subcountry,geonameid\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "les Escaldes,Andorra,Escaldes-Engordany,3040051\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "Andorra la Vella,Andorra,Andorra la Vella,3041563\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "Umm al Qaywayn,United Arab Emirates,Umm al Qaywayn,290594\n", true);
        tmpFileParent.setLastModified(8675309123L);


        tmpFileParent2.setWritable(true);
        FileUtils.writeStringToFile(tmpFileParent2, "name,country,subcountry,geonameid\n", true);
        FileUtils.writeStringToFile(tmpFileParent2, "les Escaldes,Andorra123,Escaldes-Engordany,3040051\n", true);
        FileUtils.writeStringToFile(tmpFileParent2, "Andorra la Vella,Andorra,Andorra la Vella123,3041563\n", true);
        FileUtils.writeStringToFile(tmpFileParent2, "Umm al Qaywayn,United Arab Emirates123,Umm al Qaywayn,290594\n", true);
        tmpFileParent.setLastModified(8675310321L);

        System.err.println(versionedCache.call());
        assert(cache.containsKey("Andorra la Vella"));
        assert(cache.get("Andorra la Vella").get(2).contains("Andorra la Vella123")); //should be the contents of the second file created
    }

    @Test
    public void testDebugHdfsInput() throws Exception{
        namespace = new URIExtractionNamespace(tmpFileParent.toURI(), null, null,
                new CSVFlatDataParser(Arrays.asList("name","country","subcountry","geonameid"), "name", true, 0, null),
                null, null, 10L, "cities_lookup", "date", true, null, null);

        Map<String, List<String>> cache = new HashMap<String, List<String>>();
        Callable<String> versionedCache = obj.getCachePopulator("blah",
                namespace, "500", cache);

        tmpFileParent.setWritable(true);
        FileUtils.writeStringToFile(tmpFileParent, "name,country,subcountry,geonameid\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "les Escaldes,Andorra,Escaldes-Engordany,3040051\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "Andorra la Vella,Andorra,Andorra la Vella,3041563\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "Umm al Qaywayn,United Arab Emirates,Umm al Qaywayn,290594\n", true);
        tmpFileParent.setLastModified(8675309123L);

        System.err.println(versionedCache.call());
        assert(cache.containsKey("Andorra la Vella"));

    }

    @Test
    public void testNewGzFileHandle() throws Exception{
        tmpFileParent.setWritable(true);
        FileUtils.writeStringToFile(tmpFileParent, "name,country,subcountry,geonameid\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "les Escaldes,Andorra,Escaldes-Engordany,3040051\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "Andorra la Vella,Andorra,Andorra la Vella,3041563\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "Umm al Qaywayn,United Arab Emirates,Umm al Qaywayn,290594\n", true);
        tmpFileParent.setLastModified(8675309123L);
        String parent = tmpFileParent.getParent();

        FileInputStream fis = new FileInputStream(tmpFileParent.getAbsolutePath());
        FileOutputStream fos = new FileOutputStream(parent + "/tmp.gz");
        GZIPOutputStream gzos = new GZIPOutputStream(fos);
        byte[] buffer = new byte[2048];
        int length;
        while ((length = fis.read(buffer)) > 0) {
            gzos.write(buffer, 0, length);
        }
        gzos.finish();
        URI testUri = new URI("file:" + parent + "/tmp.gz");

        namespace = new URIExtractionNamespace(testUri, null, null,
                new CSVFlatDataParser(Arrays.asList("name","country","subcountry","geonameid"), "name", true, 0, null),
                null, null, 10L, "cities_lookup", "date", true, null, null);

        Map<String, List<String>> cache = new HashMap<String, List<String>>();
        Callable<String> versionedCache = obj.getCachePopulator("blah",
                namespace, "500", cache);


        System.err.println(versionedCache.call());
        assert(cache.containsKey("Andorra la Vella"));

    }

    @Test
    public void testNullKeyJoining() throws Exception{
        tmpFileParent.setWritable(true);
        FileUtils.writeStringToFile(tmpFileParent, "name,country,subcountry,geonameid\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "les Escaldes,Andorra,Escaldes-Engordany,3040051\n", true);
        FileUtils.writeStringToFile(tmpFileParent, ",Andorra,Andorra la Vella,3041563\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "Umm al Qaywayn,United Arab Emirates,Umm al Qaywayn,290594\n", true);
        tmpFileParent.setLastModified(8675309123L);
        String parent = tmpFileParent.getParent();

        FileInputStream fis = new FileInputStream(tmpFileParent.getAbsolutePath());
        FileOutputStream fos = new FileOutputStream(parent + "/tmp.gz");
        GZIPOutputStream gzos = new GZIPOutputStream(fos);
        byte[] buffer = new byte[2048];
        int length;
        while ((length = fis.read(buffer)) > 0) {
            gzos.write(buffer, 0, length);
        }
        gzos.finish();
        URI testUri = new URI("file:" + parent + "/tmp.gz");

        namespace = new URIExtractionNamespace(testUri, null, null,
                new CSVFlatDataParser(Arrays.asList("name","country","subcountry","geonameid"), "name", true, 0, "nuller"),
                null, null, 10L, "cities_lookup", "date", true, null, null);

        Map<String, List<String>> cache = new HashMap<String, List<String>>();
        Callable<String> versionedCache = obj.getCachePopulator("blah",
                namespace, "500", cache);


        System.err.println(versionedCache.call());
        assert(cache.containsKey("nuller"));

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

    @Test
    public void testUseTsvParser() throws Exception{
        namespace = new URIExtractionNamespace(tmpFileParent.toURI(), null, null,
                new TSVFlatDataParser(Arrays.asList("name","country","subcountry","geonameid"), "name", "NA", "\t", null),
                null, null, 10L, "cities_lookup", "date", true, null, null);

        Map<String, List<String>> cache = new HashMap<String, List<String>>();
        Callable<String> versionedCache = obj.getCachePopulator("blah",
                namespace, "500", cache);

        tmpFileParent.setWritable(true);
        FileUtils.writeStringToFile(tmpFileParent, "name\tcountry\tsubcountry\tgeonameid\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "les Escaldes\tAndorra\tEscaldes-Engordany\t3040051\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "Andorra la Vella\tAndorra\tAndorra la Vella\t3041563\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "Umm al Qaywayn\tUnited Arab Emirates\tUmm al Qaywayn\t290594\n", true);
        tmpFileParent.setLastModified(8675309123L);

        System.err.println(versionedCache.call());
        assert(cache.containsKey("Andorra la Vella"));

    }

    @Test
    public void testUseTsvParserWithoutHeaders() throws Exception{
        namespace = new URIExtractionNamespace(tmpFileParent.toURI(), null, null,
                new TSVFlatDataParser(Arrays.asList("name","country","subcountry","geonameid"), "name", true, 0, "NA", "\t", null),
                null, null, 10L, "cities_lookup", "date", true, null, null);

        Map<String, List<String>> cache = new HashMap<String, List<String>>();
        Callable<String> versionedCache = obj.getCachePopulator("blah",
                namespace, "500", cache);

        tmpFileParent.setWritable(true);
        FileUtils.writeStringToFile(tmpFileParent, "name\tcountry\tsubcountry\tgeonameid\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "les Escaldes\tAndorra\tEscaldes-Engordany\t3040051\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "Andorra la Vella\tAndorra\tAndorra la Vella\t3041563\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "Umm al Qaywayn\tUnited Arab Emirates\tUmm al Qaywayn\t290594\n", true);
        tmpFileParent.setLastModified(8675309123L);

        System.err.println(versionedCache.call());
        assert(cache.containsKey("Andorra la Vella"));
        assert(cache.size() == 3);

    }

    @Test
    public void testTsvWithSkipRows() throws Exception{
        namespace = new URIExtractionNamespace(tmpFileParent.toURI(), null, null,
                new TSVFlatDataParser(Arrays.asList("name","country","subcountry","geonameid"), "name", true, 1, "NA", "\t", null),
                null, null, 10L, "cities_lookup", "date", true, null, null);

        Map<String, List<String>> cache = new HashMap<String, List<String>>();
        Callable<String> versionedCache = obj.getCachePopulator("blah",
                namespace, "500", cache);

        tmpFileParent.setWritable(true);
        FileUtils.writeStringToFile(tmpFileParent, "name\tcountry\tsubcountry\tgeonameid\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "les Escaldes\tAndorra\tEscaldes-Engordany\t3040051\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "Andorra la Vella\tAndorra\tAndorra la Vella\t3041563\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "Umm al Qaywayn\tUnited Arab Emirates\tUmm al Qaywayn\t290594\n", true);
        tmpFileParent.setLastModified(8675309123L);

        System.err.println(versionedCache.call());
        assert(cache.containsKey("Andorra la Vella"));
        assert(cache.size() == 2);

    }

    @Test
    public void testTsvNoHeaderSkipRows() throws Exception{
        namespace = new URIExtractionNamespace(tmpFileParent.toURI(), null, null,
                new TSVFlatDataParser(Arrays.asList("name","country","subcountry","geonameid"), "name", false, 1, "NA", "\t", null),
                null, null, 10L, "cities_lookup", "date", true, null, null);

        Map<String, List<String>> cache = new HashMap<String, List<String>>();
        Callable<String> versionedCache = obj.getCachePopulator("blah",
                namespace, "500", cache);

        tmpFileParent.setWritable(true);
        FileUtils.writeStringToFile(tmpFileParent, "name\tcountry\tsubcountry\tgeonameid\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "les Escaldes\tAndorra\tEscaldes-Engordany\t3040051\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "Andorra la Vella\tAndorra\tAndorra la Vella\t3041563\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "Umm al Qaywayn\tUnited Arab Emirates\tUmm al Qaywayn\t290594\n", true);
        tmpFileParent.setLastModified(8675309123L);

        System.err.println(versionedCache.call());
        assert(cache.containsKey("Andorra la Vella"));
        assert(cache.size() == 3);

    }

    @Test
    public void testTsvArbitraryDelimiter() throws Exception{
        namespace = new URIExtractionNamespace(tmpFileParent.toURI(), null, null,
                new TSVFlatDataParser(Arrays.asList("name","country","subcountry","geonameid"), "name", false, 1, "NA", ":", null),
                null, null, 10L, "cities_lookup", "date", true, null, null);

        Map<String, List<String>> cache = new HashMap<String, List<String>>();
        Callable<String> versionedCache = obj.getCachePopulator("blah",
                namespace, "500", cache);

        tmpFileParent.setWritable(true);
        FileUtils.writeStringToFile(tmpFileParent, "name:country:subcountry:geonameid\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "les Escaldes:Andorra:Escaldes-Engordany:3040051\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "Andorra la Vella:Andorra:Andorra la Vella:3041563\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "Umm al Qaywayn:United Arab Emirates:Umm al Qaywayn:290594\n", true);
        tmpFileParent.setLastModified(8675309123L);

        System.err.println(versionedCache.call());
        assert(cache.containsKey("Andorra la Vella") && cache.containsValue(Arrays.asList("Umm al Qaywayn", "United Arab Emirates", "Umm al Qaywayn", "290594")));
        assert(cache.size() == 3);

    }

    @Test
    public void testGetCacheValueWhenDisabled() throws Exception {
        namespace = new URIExtractionNamespace(tmpFileParent.toURI(), null, null,
                new TSVFlatDataParser(Arrays.asList("name","country","subcountry","geonameid"), "name", false, 1, "NA", ":", null),
                null, null, 10L, "cities_lookup", "date", false, null, null);

        Map<String, List<String>> cache = new HashMap<String, List<String>>();
        Callable<String> versionedCache = obj.getCachePopulator("blah",
                namespace, "500", cache);


        tmpFileParent.setWritable(true);
        FileUtils.writeStringToFile(tmpFileParent, "name:country:subcountry:geonameid\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "les Escaldes:Andorra:Escaldes-Engordany:3040051\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "Andorra la Vella:Andorra:Andorra la Vella:3041563\n", true);
        FileUtils.writeStringToFile(tmpFileParent, "Umm al Qaywayn:United Arab Emirates:Umm al Qaywayn:290594\n", true);
        tmpFileParent.setLastModified(8675309123L);

        System.err.println(versionedCache.call());
        assert(cache.isEmpty());

        Mockito.when(lookupService.lookup(Mockito.any())).thenReturn("Something".getBytes());

        String cacheValue = new String(obj.getCacheValue(namespace, cache, "Andorra", "country", Optional.empty()));

        assert(cacheValue.equals("Something"));
    }

}
