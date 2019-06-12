// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.*;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.concurrent.ExecutorServices;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.yahoo.maha.maha_druid_lookups.query.lookup.MongoLookupExtractor;
import com.yahoo.maha.maha_druid_lookups.query.lookup.RocksDBLookupExtractor;
import com.yahoo.maha.maha_druid_lookups.query.lookup.JDBCLookupExtractor;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.*;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.KafkaManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.RocksDBManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.ProtobufSchemaFactory;
import io.druid.query.lookup.LookupExtractor;

import javax.annotation.concurrent.GuardedBy;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public abstract class MahaNamespaceExtractionCacheManager<U> {
    protected static class NamespaceImplData {
        public NamespaceImplData(
                final ListenableFuture<?> future,
                final ExtractionNamespace namespace,
                final String name
        ) {
            this.future = future;
            this.namespace = namespace;
            this.name = name;
        }

        final ListenableFuture<?> future;
        final ExtractionNamespace namespace;
        final String name;
        final Object changeLock = new Object();
        final AtomicBoolean enabled = new AtomicBoolean(false);
        final CountDownLatch firstRun = new CountDownLatch(1);
        volatile String latestVersion = null;
    }

    private static final Logger log = new Logger(MahaNamespaceExtractionCacheManager.class);
    private final ListeningScheduledExecutorService listeningScheduledExecutorService;
    protected final ConcurrentMap<String, NamespaceImplData> implData = new ConcurrentHashMap<>();
    protected final ConcurrentMap<String, LookupExtractor> lookupExtractorMap = new ConcurrentHashMap<>();
    protected final AtomicLong tasksStarted = new AtomicLong(0);
    protected final ServiceEmitter serviceEmitter;
    private final Map<Class<? extends ExtractionNamespace>, ExtractionNamespaceCacheFactory<?, ?>> namespaceFunctionFactoryMap;
    @Inject
    LookupService lookupService;
    @Inject
    RocksDBManager rocksDBManager;
    @Inject
    KafkaManager kafkaManager;
    @Inject
    ProtobufSchemaFactory protobufSchemaFactory;

    public MahaNamespaceExtractionCacheManager(
            Lifecycle lifecycle,
            final ServiceEmitter serviceEmitter,
            final Map<Class<? extends ExtractionNamespace>, ExtractionNamespaceCacheFactory<?, ?>> namespaceFunctionFactoryMap
    ) {
        this.listeningScheduledExecutorService = MoreExecutors.listeningDecorator(
                Executors.newScheduledThreadPool(
                        15,
                        new ThreadFactoryBuilder()
                                .setDaemon(true)
                                .setNameFormat("MahaNamespaceExtractionCacheManager-%d")
                                .setPriority(Thread.MIN_PRIORITY)
                                .build()
                )
        );
        ExecutorServices.manageLifecycle(lifecycle, listeningScheduledExecutorService);
        this.serviceEmitter = serviceEmitter;
        this.namespaceFunctionFactoryMap = namespaceFunctionFactoryMap;
        listeningScheduledExecutorService.scheduleAtFixedRate(
                new Runnable() {
                    long priorTasksStarted = 0L;

                    @Override
                    public void run() {
                        try {
                            final long tasks = tasksStarted.get();
                            serviceEmitter.emit(
                                    ServiceMetricEvent.builder()
                                            .build("namespace/deltaTasksStarted", tasks - priorTasksStarted)
                            );
                            priorTasksStarted = tasks;
                            monitor(serviceEmitter);
                        } catch (Exception e) {
                            log.error(e, "Error emitting namespace stats");
                            if (Thread.currentThread().isInterrupted()) {
                                throw Throwables.propagate(e);
                            }
                        }
                    }
                },
                1,
                10, TimeUnit.MINUTES
        );
    }

    /**
     * Optional monitoring for overriding classes. `super.monitor` does *NOT* need to be called by overriding methods
     *
     * @param serviceEmitter The emitter to emit to
     */
    protected void monitor(ServiceEmitter serviceEmitter) {
        // Noop by default
    }

    protected boolean waitForServiceToEnd(long time, TimeUnit unit) throws InterruptedException {
        return listeningScheduledExecutorService.awaitTermination(time, unit);
    }


    protected void updateNamespace(final String id, final String cacheId, final String newVersion) {
        final NamespaceImplData namespaceDatum = implData.get(id);
        if (namespaceDatum == null) {
            // was removed
            return;
        }
        try {
            if (!namespaceDatum.enabled.get()) {
                // skip because it was disabled
                return;
            }
            synchronized (namespaceDatum.enabled) {
                if (!namespaceDatum.enabled.get()) {
                    return;
                }
                //swapAndClearCache(id, cacheId);
                namespaceDatum.latestVersion = newVersion;
            }
        } finally {
            namespaceDatum.firstRun.countDown();
        }
    }

    // return value means actually delete or not
    public boolean checkedDelete(
            String namespaceName
    ) {
        final NamespaceImplData implDatum = implData.get(namespaceName);
        if (implDatum == null) {
            // Delete but we don't have it?
            log.wtf("Asked to delete something I just lost [%s]", namespaceName);
            return false;
        }
        return delete(namespaceName);
    }

    // return value means actually schedule or not
    public boolean scheduleOrUpdate(
            final String id,
            ExtractionNamespace namespace,
            final Properties kafkaProperties,
            final ProtobufSchemaFactory protobufSchemaFactory,
            final String producerKafkaTopic
    ) {
        final NamespaceImplData implDatum = implData.get(id);
        if (implDatum == null) {
            // New, probably
            log.info("[%s] is new", id);
            schedule(id, namespace, kafkaProperties, protobufSchemaFactory, producerKafkaTopic);
            return true;
        }
        if (!implDatum.enabled.get()) {
            // Race condition. Someone else disabled it first, go ahead and reschedule
            log.info("[%s] is not new", id);
            schedule(id, namespace, kafkaProperties, protobufSchemaFactory, producerKafkaTopic);
            return true;
        }

        // Live one. Check if it needs updated
        if (implDatum.namespace.equals(namespace)) {
            // skip if no update
            return false;
        }
        if (log.isDebugEnabled()) {
            log.debug("Namespace [%s] needs updated to [%s]", implDatum.namespace, namespace);
        }
        // Ensure it is not changing state right now.
        synchronized (implDatum.changeLock) {
            removeNamespaceLocalMetadata(implDatum);
        }
        schedule(id, namespace, kafkaProperties, protobufSchemaFactory, producerKafkaTopic);
        return true;
    }

    public boolean scheduleAndWait(
            final String id,
            ExtractionNamespace namespace,
            long waitForFirstRun,
            final Properties kafkaProperties,
            final ProtobufSchemaFactory protobufSchemaFactory,
            final String producerKafkaTopic
    ) {
        if (scheduleOrUpdate(id, namespace, kafkaProperties, protobufSchemaFactory, producerKafkaTopic)) {
            log.debug("Scheduled new namespace [%s]: %s", id, namespace);
        } else {
            log.debug("Namespace [%s] already running: %s", id, namespace);
        }

        final NamespaceImplData namespaceImplData = implData.get(id);
        if (namespaceImplData == null) {
            log.warn("MahaNamespaceLookupExtractorFactory[%s] - deleted during start", id);
            return false;
        }

        boolean success = false;
        try {
            success = namespaceImplData.firstRun.await(waitForFirstRun, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error(e, "MahaNamespaceLookupExtractorFactory[%s] - interrupted during start", id);
        }
        return success;
    }

    @GuardedBy("implDatum.changeLock")
    private void cancelFuture(final NamespaceImplData implDatum) {
        final CountDownLatch latch = new CountDownLatch(1);
        final ListenableFuture<?> future = implDatum.future;
        Futures.addCallback(
                future, new FutureCallback<Object>() {
                    @Override
                    public void onSuccess(Object result) {
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        // Expect CancellationException
                        latch.countDown();
                        if (!(t instanceof CancellationException)) {
                            log.error(t, "Error in namespace [%s]", implDatum.name);
                        }
                    }
                }
        );
        future.cancel(true);
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
    }

    // Not thread safe
    @GuardedBy("implDatum.changeLock")
    private boolean removeNamespaceLocalMetadata(final NamespaceImplData implDatum) {
        if (implDatum == null) {
            return false;
        }
        // "Leader" election for doing the deletion
        if (!implDatum.enabled.compareAndSet(true, false)) {
            return false;
        }
        if (!implDatum.future.isDone()) {
            cancelFuture(implDatum);
        }
        lookupExtractorMap.remove(implDatum.name);
        return implData.remove(implDatum.name, implDatum);
    }

    // Optimistic scheduling of updates to a namespace.
    public <T extends ExtractionNamespace> ListenableFuture<?> schedule(final String id, final T namespace, final Properties kafkaProperties,
                                                                        final ProtobufSchemaFactory protobufSchemaFactory,
                                                                        final String producerKafkaTopic) {
        final ExtractionNamespaceCacheFactory<T, U> factory = (ExtractionNamespaceCacheFactory<T, U>)
                namespaceFunctionFactoryMap.get(namespace.getClass());
        if (factory == null) {
            throw new ISE("Cannot find factory for namespace [%s]", namespace);
        }
        final String cacheId = id;
        return schedule(id, namespace, factory, cacheId, kafkaProperties, protobufSchemaFactory, producerKafkaTopic);
    }

    // For testing purposes this is protected
    protected synchronized <T extends ExtractionNamespace> ListenableFuture<?> schedule(
            final String id,
            final T namespace,
            final ExtractionNamespaceCacheFactory<T, U> factory,
            final String cacheId,
            final Properties kafkaProperties,
            final ProtobufSchemaFactory protobufSchemaFactory,
            final String producerKafkaTopic
    ) {
        log.info("Trying to update namespace [%s]", id);
        final NamespaceImplData implDatum = implData.get(id);
        if (implDatum != null) {
            synchronized (implDatum.changeLock) {
                if (implDatum.enabled.get()) {
                    // We also check at the end of the function, but fail fast here
                    throw new IAE("Namespace [%s] already exists! Leaving prior running", namespace.toString());
                }
            }
        }
        final long updateMs = namespace.getPollMs();
        final CountDownLatch startLatch = new CountDownLatch(1);
        // Must be set before leader election occurs or else runnable will fail
        final AtomicReference<NamespaceImplData> implDataAtomicReference = new AtomicReference<>(null);

        final Runnable command = new Runnable() {
            @Override
            public void run() {
                try {
                    startLatch.await(); // wait for "election" to leadership or cancellation
                    if (!Thread.currentThread().isInterrupted()) {
                        final NamespaceImplData implData = implDataAtomicReference.get();
                        if (implData == null) {
                            // should never happen
                            throw new NullPointerException(String.format("No data for namespace [%s]", id));
                        }
                        final Map<String, U> cache = getCacheMap(cacheId);
                        final String preVersion = implData.latestVersion;
                        final Callable<String> runnable = factory.getCachePopulator(id, namespace, preVersion, cache, kafkaProperties, protobufSchemaFactory, producerKafkaTopic);

                        tasksStarted.incrementAndGet();
                        final String newVersion = runnable.call();
                        if (newVersion != null && !newVersion.equals(preVersion)) {
                            updateNamespace(id, cacheId, newVersion);
                            log.info("Namespace [%s] successfully updated. preVersion [%s], newVersion [%s]", id, preVersion, newVersion);
                        } else {
                            log.debug("Version `%s` already exists, skipping updating cache", preVersion);
                        }
                    }
                } catch (Throwable t) {
                    try {
                        if (t instanceof InterruptedException) {
                            log.info(t, "Namespace [%s] cancelled", id);
                        } else {
                            log.error(t, "Failed update namespace [%s]", namespace);
                        }
                    } catch (Exception e) {
                        t.addSuppressed(e);
                    }
                    if (Thread.currentThread().isInterrupted() || (t instanceof Error)) {
                        log.error(t, "propagating throwable");
                        throw Throwables.propagate(t);
                    }
                }
            }
        };

        ListenableFuture<?> future;
        try {
            if (updateMs > 0) {
                future = listeningScheduledExecutorService.scheduleAtFixedRate(command, 0, updateMs, TimeUnit.MILLISECONDS);
            } else {
                future = listeningScheduledExecutorService.schedule(command, 0, TimeUnit.MILLISECONDS);
            }

            lookupExtractorMap.putIfAbsent(id, getLookupExtractor(namespace, getCacheMap(cacheId)));

            // Do not need to synchronize here as we haven't set enabled to true yet, and haven't released startLatch
            final NamespaceImplData me = new NamespaceImplData(future, namespace, id);
            implDataAtomicReference.set(me);
            final NamespaceImplData other = implData.putIfAbsent(id, me);
            if (other != null) {
                if (!future.isDone() && !future.cancel(true)) {
                    log.warn("Unable to cancel future for namespace[%s] on race loss", id);
                }
                throw new IAE("Namespace [%s] already exists! Leaving prior running", namespace);
            } else {
                if (!me.enabled.compareAndSet(false, true)) {
                    log.wtf("How did someone enable this before ME?");
                }
                log.debug("I own namespace [%s]", id);
                return future;
            }
        } finally {
            startLatch.countDown();
        }
    }

    private LookupExtractor getLookupExtractor(final ExtractionNamespace extractionNamespace, Map<String, U> map) {
        if (extractionNamespace instanceof JDBCExtractionNamespace) {
            return new JDBCLookupExtractor((JDBCExtractionNamespace) extractionNamespace, map, lookupService);
        } else if (extractionNamespace instanceof RocksDBExtractionNamespace) {
            return new RocksDBLookupExtractor((RocksDBExtractionNamespace) extractionNamespace, map, lookupService, rocksDBManager, kafkaManager, protobufSchemaFactory, serviceEmitter);
        } else if (extractionNamespace instanceof MongoExtractionNamespace) {
            return new MongoLookupExtractor((MongoExtractionNamespace) extractionNamespace, map, lookupService);
        } else {
//            return new MapLookupExtractor(map, false);
            return null;
        }
    }

    public LookupExtractor getLookupExtractor(final String id) {
        return lookupExtractorMap.get(id);
    }

    /**
     * This method is expected to swap the cacheKey into the active namespace, and leave future requests for new cacheKey available. getCacheMap(cacheKey) should return empty data after this call.
     *
     * @param namespaceKey The namespace to swap the cache into
     * @param cacheKey     The cacheKey that contains the data of interest
     * @return true if old data was cleared. False if no old data was found
     */
    protected abstract boolean swapAndClearCache(String namespaceKey, String cacheKey);

    /**
     * Return a ConcurrentMap with the specified ID (either namespace's name or a cache key ID)
     *
     * @param namespaceOrCacheKey Either a namespace or cache key should be acceptable here.
     * @return A ConcurrentMap<String, String> that is backed by the impl which implements this method.
     */
    public abstract ConcurrentMap<String, U> getCacheMap(String namespaceOrCacheKey);

    /**
     * Clears out resources used by the namespace such as threads. Implementations may override this and call super.delete(...) if they have resources of their own which need cleared.
     *
     * @param ns The namespace to be deleted
     * @return True if a deletion occurred, false if no deletion occurred.
     * @throws ISE if there is an error cancelling the namespace's future task
     */
    public boolean delete(final String ns) {
        final NamespaceImplData implDatum = implData.get(ns);
        if (implDatum == null) {
            log.debug("Found no running cache for [%s]", ns);
            return false;
        }
        synchronized (implDatum.changeLock) {
            if (removeNamespaceLocalMetadata(implDatum)) {
                log.info("Deleted namespace [%s]", ns);
                return true;
            } else {
                log.debug("Did not delete namespace [%s]", ns);
                return false;
            }
        }
    }

    public String getVersion(String namespace) {
        if (namespace == null) {
            return null;
        }
        final NamespaceImplData implDatum = implData.get(namespace);
        if (implDatum == null) {
            return null;
        }
        return implDatum.latestVersion;
    }

    public Collection<String> getKnownIDs() {
        return implData.keySet();
    }

    public ExtractionNamespaceCacheFactory getExtractionNamespaceFunctionFactory(final Class key) {
        return namespaceFunctionFactoryMap.get(key);
    }

    public Optional<ExtractionNamespace> getExtractionNamespace(final String id) {
        return implData.containsKey(id) ? Optional.of(implData.get(id).namespace) : Optional.empty();
    }

    public void shutdown() {
        listeningScheduledExecutorService.shutdown();
    }
}
