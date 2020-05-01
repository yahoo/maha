package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache.MahaNamespaceExtractionCacheManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache.OnHeapMahaNamespaceExtractionCacheManager;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.server.DruidNode;
import org.testng.Assert;
import org.testng.annotations.Test;


import java.util.Properties;

public class MahaNamespaceExtractionModuleTest {
    @Test
    public void testInjection()
    {
        Injector injector = GuiceInjectors.makeStartupInjector();
        final Properties properties = injector.getInstance(Properties.class);
        properties.put(MahaNamespaceExtractionModule.TYPE_PREFIX, "onHeap");
        properties.put(String.format("%s.authHeaderFactory", MahaNamespaceExtractionModule.PREFIX), "com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.NoopAuthHeaderFactory");
        properties.put(String.format("%s.schemaFactory", MahaNamespaceExtractionModule.PREFIX), "com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.TestProtobufSchemaFactory");
        properties.put(String.format("%s.lookupService.service_scheme", MahaNamespaceExtractionModule.PREFIX), "http");
        properties.put(String.format("%s.lookupService.service_port", MahaNamespaceExtractionModule.PREFIX), "4080");
        properties.put(String.format("%s.lookupService.service_nodes", MahaNamespaceExtractionModule.PREFIX), "hist1,hist2,hist3");
        properties.put(String.format("%s.rocksdb.localStorageDirectory", MahaNamespaceExtractionModule.PREFIX), "/home/y/tmp/maha-lookups");
        properties.put(String.format("%s.rocksdb.blockCacheSize", MahaNamespaceExtractionModule.PREFIX), "2147483648");

        injector = Initialization.makeInjectorWithModules(
                injector,
                ImmutableList.of(
                        binder -> JsonConfigProvider.bindInstance(
                                binder,
                                Key.get(DruidNode.class, Self.class),
                                new DruidNode("test-inject", "host", false, null, null, true, false)
                        )
                )
        );
        final MahaNamespaceExtractionCacheManager manager = injector.getInstance(MahaNamespaceExtractionCacheManager.class);
        Assert.assertEquals(OnHeapMahaNamespaceExtractionCacheManager.class, manager.getClass());
    }
}
