// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.yahoo.maha.maha_druid_lookups.query.lookup.MahaLookupExtractorFactory;
import com.yahoo.maha.maha_druid_lookups.query.lookup.MahaRegisteredLookupExtractionFn;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.*;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache.MahaNamespaceExtractionCacheManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache.OnHeapMahaNamespaceExtractionCacheManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.flatbuffer.FlatBufferSchemaFactory;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.flatbuffer.FlatBufferSchemaFactoryProvider;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.protobuf.ProtobufSchemaFactory;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.protobuf.ProtobufSchemaFactoryProvider;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.initialization.DruidModule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 *
 */
public class MahaNamespaceExtractionModule implements DruidModule
{
    public static final String PREFIX = "druid.lookup.maha.namespace";
    public static final String TYPE_PREFIX = "druid.lookup.namespace.cache.type";
    private static final String PROPERTIES_KEY = "druid.query.rename.kafka.properties";
    private static final String ROCKSDB_PROPERTIES_KEY = "druid.query.extraction.namespace.rocksdb.properties";
    private static final String LOOKUP_SERVICE_PROPERTY_KEY = "druid.query.extraction.namespace.lookupservice.properties";

    @Override
    public List<? extends Module> getJacksonModules()
    {
        return ImmutableList.<Module>of(
                new SimpleModule("DruidNamespacedCachedExtractionModule")
                        .registerSubtypes(MahaLookupExtractorFactory.class)
                        .registerSubtypes(MahaRegisteredLookupExtractionFn.class)
        );
    }

    private static MapBinder<Class<? extends ExtractionNamespace>, ExtractionNamespaceCacheFactory<?,?>> getNamespaceFactoryMapBinder(
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

    private Properties getTypedProperties(
            ObjectMapper mapper,
            Properties systemProperties,
            String key
    )
    {
        String val = systemProperties.getProperty(key);
        if (val == null) {
            return new Properties();
        }
        try {
            final Properties properties = new Properties();
            properties.putAll(
                    mapper.<Map<String, String>>readValue(
                            val, new TypeReference<Map<String, String>>()
                            {
                            }
                    )
            );
            return properties;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Provides
    @Named("kafkaProperties")
    @LazySingleton
    public Properties getProperties(
            @Json ObjectMapper mapper,
            Properties systemProperties
    )
    {
        return getTypedProperties(mapper, systemProperties, PROPERTIES_KEY);
    }

    @Provides
    @Named("rocksdbProperties")
    @LazySingleton
    public Properties getRocksDBProperties(
            @Json ObjectMapper mapper,
            Properties systemProperties
    )
    {
        return getTypedProperties(mapper, systemProperties, ROCKSDB_PROPERTIES_KEY);
    }

    @Provides
    @Named("lookupServiceProperties")
    @LazySingleton
    public Properties getLookupServiceProperties(
            @Json ObjectMapper mapper,
            Properties systemProperties
    ) {
        return getTypedProperties(mapper, systemProperties, LOOKUP_SERVICE_PROPERTY_KEY);
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

        binder.bind(FlatBufferSchemaFactory.class).toProvider(FlatBufferSchemaFactoryProvider.class);

        binder.bind(AuthHeaderFactory.class).toProvider(AuthHeaderFactoryProvider.class);

        getNamespaceFactoryMapBinder(binder)
                .addBinding(JDBCExtractionNamespace.class)
                .to(JDBCExtractionNamespaceCacheFactory.class)
                .in(LazySingleton.class);
        getNamespaceFactoryMapBinder(binder)
                .addBinding(RocksDBExtractionNamespace.class)
                .to(RocksDBExtractionNamespaceCacheFactory.class)
                .in(LazySingleton.class);
        getNamespaceFactoryMapBinder(binder)
                .addBinding(MongoExtractionNamespace.class)
                .to(MongoExtractionNamespaceCacheFactory.class)
                .in(LazySingleton.class);
        getNamespaceFactoryMapBinder(binder)
                .addBinding(JDBCExtractionNamespaceWithLeaderAndFollower.class)
                .to(JDBCExtractionNamespaceCacheFactoryWithLeaderAndFollower.class)
                .in(LazySingleton.class);

        LifecycleModule.register(binder, RocksDBManager.class);
        LifecycleModule.register(binder, KafkaManager.class);
        LifecycleModule.register(binder, LookupService.class);
        Jerseys.addResource(binder, MahaNamespacesCacheResource.class);
    }
}
