// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache.MahaNamespaceExtractionCacheManager;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactory;
import org.apache.druid.query.lookup.LookupIntrospectHandler;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@JsonTypeName("cachedNamespace")
public class MahaLookupExtractorFactory implements LookupExtractorFactory
{
    private static final Logger LOG = new Logger(MahaLookupExtractorFactory.class);

    private static final byte[] CLASS_CACHE_KEY;

    static {
        final byte[] keyUtf8 = StringUtils.toUtf8(MahaLookupExtractorFactory.class.getCanonicalName());
        CLASS_CACHE_KEY = ByteBuffer.allocate(keyUtf8.length + 1).put(keyUtf8).put((byte) 0xFF).array();
    }

    private volatile boolean started = false;
    private final ReadWriteLock startStopSync = new ReentrantReadWriteLock();
    private final MahaNamespaceExtractionCacheManager manager;
    private final LookupIntrospectHandler lookupIntrospectHandler;
    private final ExtractionNamespace extractionNamespace;
    private final long firstCacheTimeout;
    private final boolean injective;

    private final String extractorID;

    @JsonCreator
    public MahaLookupExtractorFactory(
            @JsonProperty(value = "extractionNamespace") ExtractionNamespace extractionNamespace,
            @JsonProperty(value = "firstCacheTimeout") long firstCacheTimeout, //amount of time to wait for lookup to load
            @JsonProperty(value = "injective") boolean injective, //true if there is one to one mapping from key to value columns
            @JacksonInject final MahaNamespaceExtractionCacheManager manager
    )
    {
        this.extractionNamespace = Preconditions.checkNotNull(
                extractionNamespace,
                "extractionNamespace should be specified"
        );
        this.firstCacheTimeout = firstCacheTimeout;
        Preconditions.checkArgument(this.firstCacheTimeout >= 0);
        this.injective = injective;
        this.manager = manager;
        this.extractorID = extractionNamespace.getLookupName();
        this.lookupIntrospectHandler = new MahaLookupIntrospectHandler(this, manager, extractorID);
    }

    @VisibleForTesting
    public MahaLookupExtractorFactory(
            ExtractionNamespace extractionNamespace,
            MahaNamespaceExtractionCacheManager manager
    )
    {
        this(extractionNamespace, 60000, false, manager);
    }

    @Override
    public boolean start()
    {
        final Lock writeLock = startStopSync.writeLock();
        try {
            writeLock.lockInterruptibly();
        }
        catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
        try {
            if (started) {
                LOG.warn("Already started! [%s]", extractorID);
                return true;
            }
            if (firstCacheTimeout > 0) {
                LOG.info(" Received request [%s] [%d]", extractionNamespace, firstCacheTimeout);
                if (!manager.scheduleAndWait(extractorID, extractionNamespace, firstCacheTimeout)) {
                    LOG.error("Failed to schedule and wait for lookup [%s]", extractorID);
                    return false;
                }
            } else {
                LOG.info(" Received request [%s]", extractionNamespace);
                if (!manager.scheduleOrUpdate(extractorID, extractionNamespace)) {
                    LOG.error("Failed to schedule lookup [%s]", extractorID);
                    return false;
                }
            }
            LOG.debug("NamespaceLookupExtractorFactory[%s] started", extractorID);
            started = true;
            return true;
        }
        finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean close()
    {
        /* Due to a bug in druid coordinator, sometimes close() is getting called multiple times by LookupReferencesManager
        which is causing the lookup to be deleted unintentionally. We will implement this when the bug is fixed in next version.
        */

        return true;
    }

    @Override
    public boolean replaces(@Nullable LookupExtractorFactory other)
    {
        if (other != null && other instanceof MahaLookupExtractorFactory) {
            MahaLookupExtractorFactory that = (MahaLookupExtractorFactory) other;
            if (isInjective() != ((MahaLookupExtractorFactory) other).isInjective()) {
                return true;
            }
            if (getFirstCacheTimeout() != ((MahaLookupExtractorFactory) other).getFirstCacheTimeout()) {
                return true;
            }
            return !extractionNamespace.equals(that.extractionNamespace);
        }
        return true;
    }

    @Override
    public LookupIntrospectHandler getIntrospectHandler()
    {
        return lookupIntrospectHandler;
    }

    @JsonProperty
    public ExtractionNamespace getExtractionNamespace()
    {
        return extractionNamespace;
    }

    @JsonProperty
    public long getFirstCacheTimeout()
    {
        return firstCacheTimeout;
    }

    @JsonProperty
    public boolean isInjective()
    {
        return injective;
    }

    // Grab the latest snapshot from the cache manager
    @Override
    public LookupExtractor get()
    {
        final Lock readLock = startStopSync.readLock();
        try {
            readLock.lockInterruptibly();
        }
        catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
        try {
            if (!started) {
                throw new ISE("Factory [%s] not started", extractorID);
            }
            String preVersion = null, postVersion = null;
            Map<String, String> map = null;
            // Make sure we absolutely know what version of map we grabbed (for caching purposes)
            do {
                preVersion = manager.getVersion(extractorID);
                if (preVersion == null) {
                    throw new ISE("Namespace vanished for [%s]", extractorID);
                }
                map = manager.getCacheMap(extractorID);
                postVersion = manager.getVersion(extractorID);
                if (postVersion == null) {
                    // We lost some horrible race... make sure we clean up
                    manager.delete(extractorID);
                    throw new ISE("Lookup [%s] is deleting", extractorID);
                }
            } while (!preVersion.equals(postVersion));
            final byte[] v = StringUtils.toUtf8(postVersion);
            final byte[] id = StringUtils.toUtf8(extractorID);
            return manager.getLookupExtractor(extractorID);
        }
        finally {
            readLock.unlock();
        }
    }
}
