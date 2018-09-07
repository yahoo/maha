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
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import com.yahoo.maha.maha_druid_lookups.query.lookup.MahaLookupExtractorFactory;
import com.yahoo.maha.maha_druid_lookups.query.lookup.MahaRegisteredLookupExtractionFn;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.InMemoryDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache.MahaExtractionCacheManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache.OnHeapMahaExtractionCacheManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.ProtobufSchemaFactory;
import io.druid.guice.Jerseys;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.PolyBind;
import io.druid.guice.annotations.Json;
import io.druid.initialization.DruidModule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 *
 */
public class NamespaceExtractionModule implements DruidModule
{
    public static final String TYPE_PREFIX = "druid.lookup.namespace.cache.type";
    private static final String PROPERTIES_KEY = "druid.query.rename.kafka.properties";
    private static final String ROCKSDB_PROPERTIES_KEY = "druid.query.extraction.namespace.rocksdb.properties";
    public static final String LOOKUP_SERVICE_PROPERTY_KEY = "druid.query.extraction.namespace.lookupservice.properties";

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

    @Provides
    @Named("kafkaProperties")
    @LazySingleton
    public Properties getProperties(
            @Json ObjectMapper mapper,
            Properties systemProperties
    )
    {
        String val = systemProperties.getProperty(PROPERTIES_KEY);
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
    @Named("rocksdbProperties")
    @LazySingleton
    public Properties getRocksDBProperties(
            @Json ObjectMapper mapper,
            Properties systemProperties
    )
    {
        String val = systemProperties.getProperty(ROCKSDB_PROPERTIES_KEY);
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
    @Named("lookupServiceProperties")
    @LazySingleton
    public Properties getLookupServiceProperties(
            @Json ObjectMapper mapper,
            Properties systemProperties
    ) {
        String val = systemProperties.getProperty(LOOKUP_SERVICE_PROPERTY_KEY);
        if (val == null) {
            return new Properties();
        }
        try {
            final Properties properties = new Properties();
            properties.putAll(
                    mapper.<Map<String, String>>readValue(
                            val, new TypeReference<Map<String, String>>() {
                            }
                    )
            );
            return properties;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void configure(Binder binder)
    {
        PolyBind
                .createChoiceWithDefault(binder, TYPE_PREFIX, Key.get(MahaExtractionCacheManager.class), "onHeap")
                .in(LazySingleton.class);

        PolyBind
                .optionBinder(binder, Key.get(MahaExtractionCacheManager.class))
                .addBinding("onHeap")
                .to(OnHeapMahaExtractionCacheManager.class)
                .in(LazySingleton.class);

        binder.bind(ProtobufSchemaFactory.class).to(getProtobufSchemaFactoryClass());

        binder.bind(AuthHeaderProvider.class).to(getAuthHeaderProviderClass());

        getNamespaceFactoryMapBinder(binder)
                .addBinding(JDBCExtractionNamespace.class)
                .to(JDBCExtractionNamespaceCacheFactory.class)
                .in(LazySingleton.class);
        getNamespaceFactoryMapBinder(binder)
                .addBinding(InMemoryDBExtractionNamespace.class)
                .to(InMemoryDBExtractionNamespaceCacheFactory.class)
                .in(LazySingleton.class);

        LifecycleModule.register(binder, RocksDBManager.class);
        LifecycleModule.register(binder, KafkaManager.class);
        LifecycleModule.register(binder, LookupService.class);
        Jerseys.addResource(binder, MahaNamespacesCacheResource.class);
    }

    private Class<? extends ProtobufSchemaFactory> getProtobufSchemaFactoryClass();
    private Class<? extends AuthHeaderProvider> getAuthHeaderProviderClass();
}
