// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.yahoo.maha.maha_druid_lookups.query.lookup.MahaLookupExtractorFactory;
import com.yahoo.maha.maha_druid_lookups.query.lookup.MahaRegisteredLookupExtractionFn;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache.MahaNamespaceExtractionCacheManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache.OnHeapMahaNamespaceExtractionCacheManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.ProtobufSchemaFactory;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.PolyBind;
import io.druid.initialization.DruidModule;

import java.util.List;

/**
 *
 */
public class MahaNamespaceExtractionModule implements DruidModule
{
    public static final String PREFIX = "druid.lookup.maha.namespace";
    public static final String TYPE_PREFIX = "druid.lookup.maha.namespace.cache.type";

    @Override
    public List<? extends Module> getJacksonModules()
    {
        return ImmutableList.<Module>of(
                new SimpleModule("DruidNamespacedCachedExtractionModule")
                        .registerSubtypes(MahaLookupExtractorFactory.class)
                        .registerSubtypes(MahaRegisteredLookupExtractionFn.class)
        );
    }

    public static MapBinder<Class<? extends ExtractionNamespace>, ExtractionNamespaceCacheFactory<?,?>> getNamespaceFactoryMapBinder(
            final Binder binder
    )
    {
        return MapBinder.newMapBinder(
                binder,
                new TypeLiteral<Class<? extends ExtractionNamespace>>()
                {
                },
                new TypeLiteral<ExtractionNamespaceCacheFactory<?,?>>()
                {
                }
        );
    }

    @Override
    public void configure(Binder binder)
    {
        JsonConfigProvider.bind(binder, PREFIX, MahaNamespaceExtractionConfig.class);

        PolyBind
                .createChoiceWithDefault(binder, TYPE_PREFIX, Key.get(MahaNamespaceExtractionCacheManager.class), "onHeap")
                .in(LazySingleton.class);

        PolyBind
                .optionBinder(binder, Key.get(MahaNamespaceExtractionCacheManager.class))
                .addBinding("onHeap")
                .to(OnHeapMahaNamespaceExtractionCacheManager.class)
                .in(LazySingleton.class);


        binder.bind(ProtobufSchemaFactory.class).toProvider(ProtobufSchemaFactoryProvider.class);

        binder.bind(AuthHeaderFactory.class).toProvider(AuthHeaderFactoryProvider.class);

        getNamespaceFactoryMapBinder(binder)
                .addBinding(JDBCExtractionNamespace.class)
                .to(JDBCExtractionNamespaceCacheFactory.class)
                .in(LazySingleton.class);
        getNamespaceFactoryMapBinder(binder)
                .addBinding(RocksDBExtractionNamespace.class)
                .to(RocksDBExtractionNamespaceCacheFactory.class)
                .in(LazySingleton.class);

        LifecycleModule.register(binder, RocksDBManager.class);
        LifecycleModule.register(binder, KafkaManager.class);
        LifecycleModule.register(binder, LookupService.class);
        Jerseys.addResource(binder, MahaNamespacesCacheResource.class);
    }
}
