// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Self;
import io.druid.initialization.Initialization;
import io.druid.server.DruidNode;
import org.junit.Assert;

import java.util.Properties;

public class OnHeapMahaExtractionCacheManagerTest
{
    //@Test
    public void testInjection()
    {
        final Injector injector = Initialization.makeInjectorWithModules(
                GuiceInjectors.makeStartupInjector(),
                ImmutableList.of(
                        new Module()
                        {
                            @Override
                            public void configure(Binder binder)
                            {
                                JsonConfigProvider.bindInstance(
                                        binder, Key.get(DruidNode.class, Self.class), new DruidNode("test-inject", null, null)
                                );
                            }
                        }
                )
        );
        final Properties properties = injector.getInstance(Properties.class);
        properties.clear();
        final MahaExtractionCacheManager manager = injector.getInstance(MahaExtractionCacheManager.class);
        Assert.assertEquals(OnHeapMahaExtractionCacheManager.class, manager.getClass());
    }
}
