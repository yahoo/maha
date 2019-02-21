package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache.MahaNamespaceExtractionCacheManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache.OnHeapMahaNamespaceExtractionCacheManager;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Self;
import io.druid.initialization.Initialization;
import io.druid.server.DruidNode;
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
        properties.put(String.format("%s.schemaFactory", MahaNamespaceExtractionModule.PREFIX), "com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.DefaultProtobufSchemaFactory");

        injector = Initialization.makeInjectorWithModules(
                injector,
                ImmutableList.of(
                        new Module()
                        {
                            @Override
                            public void configure(Binder binder)
                            {
                                JsonConfigProvider.bindInstance(
                                        binder,
                                        Key.get(DruidNode.class, Self.class),
                                        new DruidNode("test-inject", null, null, null, true, false)
                                );
                            }
                        }
                )
        );
        final MahaNamespaceExtractionCacheManager manager = injector.getInstance(MahaNamespaceExtractionCacheManager.class);
        Assert.assertEquals(OnHeapMahaNamespaceExtractionCacheManager.class, manager.getClass());
    }
}
