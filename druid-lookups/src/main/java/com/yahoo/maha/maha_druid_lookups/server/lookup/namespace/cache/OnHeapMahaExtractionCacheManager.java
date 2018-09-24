// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache;

import com.google.common.primitives.Chars;
import com.google.common.util.concurrent.Striped;
import com.google.inject.Inject;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.emitter.service.ServiceMetricEvent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

/**
 *
 */
public class OnHeapMahaExtractionCacheManager<U> extends MahaExtractionCacheManager
{
    private static final Logger LOG = new Logger(OnHeapMahaExtractionCacheManager.class);
    private final ConcurrentMap<String, ConcurrentMap<String, U>> mapMap = new ConcurrentHashMap<>();
    private final Striped<Lock> nsLocks = Striped.lock(32);

    @Inject
    public OnHeapMahaExtractionCacheManager(
            final Lifecycle lifecycle,
            final ServiceEmitter emitter,
            final Map<Class<? extends ExtractionNamespace>, ExtractionNamespaceCacheFactory<?,?>> namespaceFunctionFactoryMap
    )
    {
        super(lifecycle, emitter, namespaceFunctionFactoryMap);
    }

    @Override
    protected boolean swapAndClearCache(String namespaceKey, String cacheKey)
    {
        final Lock lock = nsLocks.get(namespaceKey);
        lock.lock();
        try {
            ConcurrentMap<String, U> cacheMap = mapMap.get(cacheKey);
            if (cacheMap == null) {
                throw new IAE("Extraction Cache [%s] does not exist", cacheKey);
            }
            ConcurrentMap<String, U> prior = mapMap.put(namespaceKey, cacheMap);
            mapMap.remove(cacheKey);
            if (prior != null) {
                // Old map will get GC'd when it is not used anymore
                return true;
            } else {
                return false;
            }
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public ConcurrentMap<String, U> getCacheMap(String namespaceOrCacheKey)
    {
        ConcurrentMap<String, U> map = mapMap.get(namespaceOrCacheKey);
        if (map == null) {
            mapMap.putIfAbsent(namespaceOrCacheKey, new ConcurrentHashMap());
            map = mapMap.get(namespaceOrCacheKey);
        }
        return map;
    }

    @Override
    public boolean delete(final String namespaceKey)
    {
        // `super.delete` has a synchronization in it, don't call it in the lock.
        if (!super.delete(namespaceKey)) {
            return false;
        }
        final Lock lock = nsLocks.get(namespaceKey);
        lock.lock();
        try {
            return mapMap.remove(namespaceKey) != null;
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    protected void monitor(ServiceEmitter serviceEmitter)
    {
        long numEntries = 0;
        long size = 0;
        for (Map.Entry<String, ConcurrentMap<String, U>> entry : mapMap.entrySet()) {
            final ConcurrentMap<String, U> map = entry.getValue();
            if (map == null) {
                LOG.debug("missing cache key for reporting [%s]", entry.getKey());
                continue;
            }
            numEntries += map.size();
            for (Map.Entry<String, U> sEntry : map.entrySet()) {
                final String key = sEntry.getKey();
                size += key.length();
            }
        }
        serviceEmitter.emit(ServiceMetricEvent.builder().build("namespace/cache/numEntries", numEntries));
        serviceEmitter.emit(ServiceMetricEvent.builder().build("namespace/cache/heapSizeInBytes", size * Chars.BYTES));
    }
}
