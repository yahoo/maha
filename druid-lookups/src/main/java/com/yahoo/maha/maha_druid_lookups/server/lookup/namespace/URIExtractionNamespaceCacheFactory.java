/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Copyright 2022, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.URIExtractionNamespace;
import org.apache.druid.data.SearchableVersionedDataFinder;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.loading.URIDataPuller;
import org.apache.druid.utils.CompressionUtils;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.MapPopulator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

/**
 *
 */
public class URIExtractionNamespaceCacheFactory
        implements ExtractionNamespaceCacheFactory<URIExtractionNamespace, List<String>> {
    private static final int DEFAULT_NUM_RETRIES = 3;
    private static final Logger log = new Logger(URIExtractionNamespaceCacheFactory.class);
    private final Map<String, SearchableVersionedDataFinder> pullers;

    @Inject
    LookupService lookupService;
    @Inject
    ServiceEmitter emitter;

    @Inject
    public URIExtractionNamespaceCacheFactory(Map<String, SearchableVersionedDataFinder> pullers) {
        this.pullers = pullers;
    }

    @Override
    public Callable<String> getCachePopulator(
            final String id,
            final URIExtractionNamespace extractionNamespace,
            final String lastVersion,
            final Map<String, List<String>> cache
    ) {
        final long lastCheck = lastVersion == null ? Long.MIN_VALUE / 2 : Long.parseLong(lastVersion);

        if (!extractionNamespace.isCacheEnabled()) {
            return () -> String.valueOf(lastCheck);
        }

        return () -> getValuesFromURI(extractionNamespace, lastCheck, cache, id);
    }

    private String getValuesFromURI(URIExtractionNamespace extractionNamespace, long lastCheck, Map<String, List<String>> cache, String id) {
        final boolean doSearch = extractionNamespace.getUriPrefix() != null;
        final URI originalUri = doSearch ? extractionNamespace.getUriPrefix()
                : extractionNamespace.getUri();
        final SearchableVersionedDataFinder<URI> pullerRaw = pullers.get(originalUri.getScheme());
        if (pullerRaw == null) {
            throw new IAE("Unknown loader type[%s].  Known types are %s", originalUri.getScheme(),
                    pullers.keySet());
        }
        if (!(pullerRaw instanceof URIDataPuller)) {
            throw new IAE("Cannot load data from location [%s]. Data pulling from [%s] not supported",
                    originalUri, originalUri.getScheme());
        }
        final URIDataPuller puller = (URIDataPuller) pullerRaw;
        final URI uri;

        if (doSearch) {
            final Pattern versionRegex;

            if (extractionNamespace.getFileRegex() != null) {
                versionRegex = Pattern.compile(extractionNamespace.getFileRegex());
            } else {
                versionRegex = null;
            }
            uri = pullerRaw.getLatestVersion(extractionNamespace.getUriPrefix(), versionRegex);

            if (uri == null) {
                throw new RuntimeException(new FileNotFoundException(
                        String.format("Could not find match for pattern `%s` in [%s] for %s", versionRegex,
                                originalUri, extractionNamespace)));
            }
        } else {
            uri = extractionNamespace.getUri();
        }

        final String uriPath = uri.getPath();


        try {
            return RetryUtils.retry(() -> {
                final String version = puller.getVersion(uri);
                try {
                    long lastModified = Long.parseLong(version);
                    if (lastModified <= lastCheck) {
                        final DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
                        log.debug(
                                "URI [%s] for namespace [%s] was las modified [%s] but was last cached [%s]. Skipping ",
                                uri.toString(), id, fmt.print(lastModified), fmt.print(lastCheck));
                        return version;
                    }
                } catch (NumberFormatException ex) {
                    log.debug(ex, "Failed to get last modified timestamp. Assuming no timestamp");
                }
                final ByteSource source;
                if (CompressionUtils.isGz(uriPath)) {
                    // Simple gzip stream
                    log.debug("Loading gz");
                    source = new ByteSource() {
                        @Override
                        public InputStream openStream() throws IOException {
                            return CompressionUtils.gzipInputStream(puller.getInputStream(uri));
                        }
                    };
                } else {
                    source = new ByteSource() {
                        @Override
                        public InputStream openStream() throws IOException {
                            return puller.getInputStream(uri);
                        }
                    };
                }
                final long lineCount = new MapPopulator<>(
                        extractionNamespace.getNamespaceParseSpec().getParser()).populate(source,
                        cache).getLines();
                log.info("Finished loading %d lines for namespace [%s]", lineCount, id);
                return version;
            }, puller.shouldRetryPredicate(), DEFAULT_NUM_RETRIES);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private byte[] checkCacheAndReturn(URIExtractionNamespace extractionNamespace, Map<String, List<String>> cache, String key, String valueColumn) {
        if(!cache.containsKey(key) || !extractionNamespace.getNamespaceParseSpec().getColumns().contains(valueColumn)){
            log.error("Found no value for " + key + " with Namespace Cols: " + extractionNamespace.getNamespaceParseSpec().getColumns().toString() + " and val: " + valueColumn);
            return new byte[0];
        } else {
            return cache.get(key).get(extractionNamespace.getNamespaceParseSpec().getColumns().indexOf(valueColumn)).getBytes();
        }
    }

    @Override
    public byte[] getCacheValue(final URIExtractionNamespace extractionNamespace, final Map<String, List<String>> cache, final String key, String valueColumn, final Optional<DecodeConfig> decodeConfigOptional) {
        if(!extractionNamespace.isCacheEnabled()){
            Map<String, List<String>> tempCache = new HashMap<>();
            getValuesFromURI(extractionNamespace, 0L, tempCache, extractionNamespace.getLookupName());
            return checkCacheAndReturn(extractionNamespace, tempCache, key, valueColumn);
        } else {
            return checkCacheAndReturn(extractionNamespace, cache, key, valueColumn);
        }
    }


        @Override
    public void updateCache(final URIExtractionNamespace extractionNamespace, final Map<String, List<String>> cache,
                            final String key, final byte[] value) {
        //No-op
    }

    @Override
    public Long getLastUpdatedTime(final URIExtractionNamespace extractionNamespace) {
        if (!extractionNamespace.isCacheEnabled()) {
            return lookupService.getLastUpdatedTime(new LookupService.LookupData(extractionNamespace));
        }
        return (extractionNamespace.getPreviousLastUpdateTimestamp() != null) ? extractionNamespace.getPreviousLastUpdateTimestamp().getTime() : -1L;
    }
}
