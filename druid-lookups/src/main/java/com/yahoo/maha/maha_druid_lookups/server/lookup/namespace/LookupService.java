// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNamespace;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;

import javax.net.ssl.SSLContext;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;

@ManageLifecycle
public class LookupService {

    private static final Logger LOG = new Logger(LookupService.class);
    private static final int TIMEOUT = 5000;
    private CloseableHttpClient httpclient;
    private static final int MAX_CONNECTIONS = 200;
    private final Properties lookupServiceProperties = new Properties();
    private LoadingCache<LookupData, byte[]> loadingCache;
    private static final long MAX_CACHE_SIZE = 100000;
    private final String[] serviceNodeList;
    private AtomicInteger currentHost = new AtomicInteger(0);
    private static final Random RANDOM = new Random(0);
    private String serviceScheme = "http";
    private String servicePort = "4080";
    private final AuthHeaderFactory authHeaderFactory;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    public LookupService(final MahaNamespaceExtractionConfig mahaNamespaceExtractionConfig, AuthHeaderFactory authHeaderFactory) {
        this.lookupServiceProperties.putAll(mahaNamespaceExtractionConfig.getLookupServiceProperties());
        try {

            this.authHeaderFactory = authHeaderFactory;
            serviceScheme = lookupServiceProperties.getProperty("service_scheme", "http");
            servicePort = lookupServiceProperties.getProperty("service_port", "4080");
            serviceNodeList = lookupServiceProperties.getProperty("service_nodes").split(",");

            currentHost.set(RANDOM.nextInt(serviceNodeList.length));

            SSLContext sslContext = SSLContexts.createDefault();
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext,
                    new String[]{"TLSv1.2"},
                    null,
                    new NoopHostnameVerifier());

            PoolingHttpClientConnectionManager connMgr =
                    new PoolingHttpClientConnectionManager(RegistryBuilder.<ConnectionSocketFactory>create()
                            .register(serviceScheme, sslsf).build());
            connMgr.setMaxTotal((int)lookupServiceProperties.getOrDefault("max_connections", MAX_CONNECTIONS));
            connMgr.setDefaultMaxPerRoute((int)lookupServiceProperties.getOrDefault("max_connections", MAX_CONNECTIONS));

            final RequestConfig requestConfig =
                    RequestConfig.custom()
                            .setConnectionRequestTimeout((int) lookupServiceProperties.getOrDefault("timeout", TIMEOUT))
                            .setConnectTimeout((int) lookupServiceProperties.getOrDefault("timeout", TIMEOUT))
                            .setSocketTimeout((int)lookupServiceProperties.getOrDefault("timeout", TIMEOUT))
                            .build();

            httpclient =
                    HttpClients.custom().setSSLSocketFactory(sslsf).setConnectionManager(connMgr)
                            .setDefaultRequestConfig(requestConfig).build();

            CacheLoader<LookupData, byte[]> loader;
            loader = new CacheLoader<LookupData, byte[]>() {
                @Override
                public byte[] load(LookupData lookupData) {
                    byte[] value = new byte[0];
                    for(String serviceNode: serviceNodeList) {
                        try {
                            value = callService(lookupData);
                            return value;
                        } catch (Exception e) {
                            LOG.error(e, String.format("Exception with node [%s] while doing lookup on key [%s]. Moving on to next node", serviceNode, lookupData.key));
                        }
                    }
                    return value;
                }
            };

            loadingCache = Caffeine
                    .newBuilder()
                    .maximumSize((long) lookupServiceProperties.getOrDefault("max_cache_size", MAX_CACHE_SIZE))
                    .expireAfterWrite(1, TimeUnit.MINUTES)
                    .build(loader);

        } catch (final Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private byte[] callService(LookupData lookupData) throws URISyntaxException, IOException {

        HttpGet httpGet = new HttpGet();
        Map<String, String> authHeaders = authHeaderFactory.getAuthHeaders(lookupServiceProperties);
        if(authHeaders != null) {
            authHeaders.entrySet().stream().forEach(e -> httpGet.addHeader(e.getKey(), e.getValue()));
        }
        URIBuilder uriBuilder = new URIBuilder()
                .setScheme(serviceScheme)
                .setHost(getHost())
                .setPort(Integer.valueOf(servicePort))
                .setPath("/druid/v1/namespaces/" + lookupData.extractionNamespace.getLookupName())
                .addParameter("namespaceclass", lookupData.extractionNamespace.getClass().getName())
                .addParameter("key", lookupData.key)
                .addParameter("valueColumn", lookupData.valueColumn);

        if(lookupData.decodeConfigOptional.isPresent()) {
            uriBuilder.addParameter("decodeConfig", URLEncoder.encode(objectMapper.writeValueAsString(lookupData.decodeConfigOptional.get()), UTF_8.toString()));
        }

        httpGet.setURI(uriBuilder.build());
        final HttpResponse response = httpclient.execute(httpGet);
        final HttpEntity entity = response.getEntity();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        entity.writeTo(baos);
        return baos.toByteArray();
    }

    public Long getLastUpdatedTime(LookupData lookupData) {

        Long lastUpdatedTime = -1L;
        try {
            HttpGet httpGet = new HttpGet();
            Map<String, String> authHeaders = authHeaderFactory.getAuthHeaders(lookupServiceProperties);
            if(authHeaders != null) {
                authHeaders.entrySet().stream().forEach(e -> httpGet.addHeader(e.getKey(), e.getValue()));
            }
            httpGet.setHeader("content-type", "application/json");
            httpGet.setURI(new URIBuilder()
                    .setScheme(serviceScheme)
                    .setHost(getHost())
                    .setPort(Integer.valueOf(servicePort))
                    .setPath(String.format("/druid/v1/namespaces/%s/lastUpdatedTime", lookupData.extractionNamespace.getLookupName()))
                    .addParameter("namespaceclass", lookupData.extractionNamespace.getClass().getName())
                    .build());
            final HttpResponse response = httpclient.execute(httpGet);
            lastUpdatedTime = Long.valueOf(new BasicResponseHandler().handleResponse(response));
        } catch(Exception e) {
            LOG.error(e, "Exception while getting lastUpdatedTime");
        }
        return lastUpdatedTime;
    }

    private synchronized String getHost() {
        String host = serviceNodeList[0];
        try {
            currentHost.compareAndSet(serviceNodeList.length, 0);
            host = serviceNodeList[currentHost.getAndIncrement()];
        } catch (Exception e) {
            LOG.error(e, "Exception while getting host hence will use first host in the list");
        }
        return host;
    }

    public byte[] lookup(final LookupData lookupData) {
        byte[] value = new byte[0];
        try {
            value = loadingCache.get(lookupData);
        } catch (Exception e) {
            LOG.error(e, "Exception while loading from cache");
        }
        return value;
    }

    public long getSize() {
        return loadingCache.estimatedSize();
    }

    public void update(final LookupData lookupData, byte[] value) {
        if (loadingCache.getIfPresent(lookupData) != null) {
            loadingCache.put(lookupData, value);
        }
    }

    public static class LookupData {
        String key;
        String valueColumn;
        Optional<DecodeConfig> decodeConfigOptional = Optional.empty();
        ExtractionNamespace extractionNamespace;

        public LookupData(ExtractionNamespace extractionNamespace) {
            this.extractionNamespace = extractionNamespace;
        }

        public LookupData(ExtractionNamespace extractionNamespace, String key, String valueColumn, Optional<DecodeConfig> decodeConfigOptional) {
            this.extractionNamespace = extractionNamespace;
            this.key = key;
            this.valueColumn = valueColumn;
            this.decodeConfigOptional = decodeConfigOptional;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LookupData that = (LookupData) o;
            boolean result = Objects.equals(key, that.key) &&
                    Objects.equals(valueColumn, that.valueColumn) &&
                    Objects.equals(extractionNamespace, that.extractionNamespace);

            if(decodeConfigOptional.isPresent() && that.decodeConfigOptional.isPresent()) {
                result &= Objects.equals(decodeConfigOptional.get(), that.decodeConfigOptional.get());
            } else if (decodeConfigOptional.isPresent() || that.decodeConfigOptional.isPresent()) {
                return false;
            }
            return result;
        }

        @Override
        public int hashCode() {

            if(decodeConfigOptional.isPresent())
                return Objects.hash(key, valueColumn, decodeConfigOptional.get(), extractionNamespace);
            else
                return Objects.hash(key, valueColumn, extractionNamespace);
        }

    }

    @LifecycleStart
    public void start() {
    }

    @LifecycleStop
    public void stop() throws IOException {
        if(httpclient != null) {
            httpclient.close();
        }
    }

}
