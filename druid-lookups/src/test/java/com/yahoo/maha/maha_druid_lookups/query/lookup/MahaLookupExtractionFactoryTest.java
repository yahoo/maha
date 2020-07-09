package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import com.google.inject.*;
import com.yahoo.maha.maha_druid_lookups.TestMongoServer;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.MongoExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.MahaNamespaceExtractionModule;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache.MahaNamespaceExtractionCacheManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache.OnHeapMahaNamespaceExtractionCacheManager;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactory;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupReferencesManager;
import org.apache.druid.server.DruidNode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class MahaLookupExtractionFactoryTest extends TestMongoServer {

    Provider<LookupReferencesManager> provider = new Provider<LookupReferencesManager>() {
        @Override
        public LookupReferencesManager get() {
            return getLookupReferencesManager();
        }
    };

    Injector injector = createInjector();

    ObjectMapper objectMapper = injector.getInstance(ObjectMapper.class);

    InetSocketAddress mongoSocketAddress;

    LookupReferencesManager lookupReferencesManager;

    LookupReferencesManager getLookupReferencesManager() {
        return lookupReferencesManager;
    }

    private Injector createInjector() {
        Injector injector = GuiceInjectors.makeStartupInjector();
        final Properties properties = injector.getInstance(Properties.class);
        properties.put(MahaNamespaceExtractionModule.TYPE_PREFIX, "onHeap");
        properties.put(String.format("%s.authHeaderFactory", MahaNamespaceExtractionModule.PREFIX), "com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.NoopAuthHeaderFactory");
        properties.put(String.format("%s.schemaFactory", MahaNamespaceExtractionModule.PREFIX), "com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.protobuf.NoopProtobufSchemaFactory");
        properties.put(String.format("%s.lookupService.service_scheme", MahaNamespaceExtractionModule.PREFIX), "http");
        properties.put(String.format("%s.lookupService.service_port", MahaNamespaceExtractionModule.PREFIX), "4080");
        properties.put(String.format("%s.lookupService.service_nodes", MahaNamespaceExtractionModule.PREFIX), "hist1,hist2,hist3");
        properties.put(String.format("%s.rocksdb.localStorageDirectory", MahaNamespaceExtractionModule.PREFIX), "/home/y/tmp/maha-lookups");
        properties.put(String.format("%s.rocksdb.blockCacheSize", MahaNamespaceExtractionModule.PREFIX), "2147483648");

        injector = Initialization.makeInjectorWithModules(
                injector,
                ImmutableList.of(
                        binder -> {
                            JsonConfigProvider.bindInstance(
                                    binder,
                                    Key.get(DruidNode.class, Self.class),
                                    new DruidNode("test-inject", null, false, null, null, true, false)
                            );
                            binder.bind(LookupReferencesManager.class).toProvider(provider);
                        }
                )
        );

        return injector;
    }

    @BeforeClass
    public void setup() throws Exception {
        mongoSocketAddress = setupMongoServer(objectMapper);
    }

    @AfterClass
    public void cleanup() {
        cleanupMongoServer();
    }

    @Test
    public void successfullyDeserializeFullNamespaceFromJSON() throws Exception {
        InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream("maha_lookup_extraction_factory.json");
        String json = String.format(CharStreams.toString(new InputStreamReader(input, StandardCharsets.UTF_8))
                , mongoSocketAddress.getHostString(), mongoSocketAddress.getPort());
        LookupExtractorFactoryContainer container = objectMapper
                .readValue(json
                        , LookupExtractorFactoryContainer.class);
        assertEquals(container.getVersion(), "v1");
        assertNotNull(container.getLookupExtractorFactory());
        LookupExtractorFactory factory = container.getLookupExtractorFactory();
        assertTrue(factory instanceof MahaLookupExtractorFactory);
        MahaLookupExtractorFactory mahaFactory = (MahaLookupExtractorFactory) factory;
        assertEquals(mahaFactory.getExtractionNamespace().getLookupName(), "advertiser_lookup");
        assertEquals(mahaFactory.getExtractionNamespace().getPollMs(), 30000L);
        assertEquals(mahaFactory.getExtractionNamespace().getTsColumn(), "updated_at");
        assertTrue(mahaFactory.getExtractionNamespace().isCacheEnabled());
        assertTrue(mahaFactory.getExtractionNamespace() instanceof MongoExtractionNamespace);
        MongoExtractionNamespace mongoExtractionNamespace = (MongoExtractionNamespace) mahaFactory.getExtractionNamespace();

        createTestData("mongo_advertiser.json", "advertiser", objectMapper, mongoExtractionNamespace.getConnectorConfig());

        assertTrue(factory.start());

        MahaNamespaceExtractionCacheManager manager = injector.getInstance(MahaNamespaceExtractionCacheManager.class);
        assertEquals(OnHeapMahaNamespaceExtractionCacheManager.class, manager.getClass());
        OnHeapMahaNamespaceExtractionCacheManager onHeapManager = (OnHeapMahaNamespaceExtractionCacheManager) manager;
        LookupExtractor extractor = onHeapManager.getLookupExtractor("advertiser_lookup");
        assertTrue(extractor instanceof MahaLookupExtractor);
        MahaLookupExtractor mahaExtractor = (MahaLookupExtractor) extractor;
        assertEquals(mahaExtractor.apply("5ad10906fc7b6ecac8d41081", "name", null, null), "advertiser1");

        lookupReferencesManager = mock(LookupReferencesManager.class);
        when(lookupReferencesManager.get(any())).thenReturn(container);

        MahaRegisteredLookupExtractionFn fn = objectMapper
                .readValue(Thread.currentThread()
                                .getContextClassLoader()
                                .getResourceAsStream("maha_registered_lookup_extraction_fn.json")
                        , MahaRegisteredLookupExtractionFn.class);

        assertEquals(fn.getLookup(), "advertiser_lookup");
        assertEquals(fn.getValueColumn(), "name");
        assertEquals(fn.getExtractionType(), ExtractionFn.ExtractionType.ONE_TO_ONE);
        assertEquals(fn.getReplaceMissingValueWith(), "Unknown");

        assertEquals(fn.apply("5ad10906fc7b6ecac8d41080"), "123");
        assertEquals(fn.apply("5ad10906fc7b6ecac8d41083"), "Unknown");

        manager.shutdown();
    }

}
