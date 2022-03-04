// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache.MahaNamespaceExtractionCacheManager;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthConfig;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URLDecoder;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

@Path("/druid/v1/namespaces")
public class MahaNamespacesCacheResource
{
    private static final Logger log = new Logger(MahaNamespacesCacheResource.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final MahaNamespaceExtractionCacheManager mahaNamespaceExtractionCacheManager;
    private final ServiceEmitter serviceEmitter;

    @Inject
    public MahaNamespacesCacheResource(final MahaNamespaceExtractionCacheManager mahaNamespaceExtractionCacheManager,
                                       final ServiceEmitter serviceEmitter){
        this.mahaNamespaceExtractionCacheManager = mahaNamespaceExtractionCacheManager;
        this.serviceEmitter = serviceEmitter;
    }

    @GET
    @Produces({ MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
    public Response getNamespaces(@Context final HttpServletRequest request){
        try{
            request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, Access.OK.isAllowed());
            Collection<String> namespaces = mahaNamespaceExtractionCacheManager.getKnownIDs();
            Map<String, Integer> response = new HashMap<String, Integer>();
            for(String namespace: namespaces) {
                response.put(namespace, mahaNamespaceExtractionCacheManager.getCacheMap(namespace).size());
            }
            return Response.ok().entity(response).build();
        }catch (Exception ex){
            log.error("Can not get the list of known namespaces");
            return Response.serverError().entity(Strings.nullToEmpty(ex.getMessage())).build();
        }
    }

    @GET
    @Produces({ MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
    @Path("{namespace}")
    public Response getCacheValue(@PathParam("namespace") String namespace,
                                  @QueryParam("namespaceclass") String extractionNamespaceClass,
                                  @QueryParam("key") String key,
                                  @QueryParam("valueColumn") String valueColumn,
                                  @QueryParam("decodeConfig") String decodeConfigString,
                                  @QueryParam("debug") boolean debug, @Context final HttpServletRequest request) {
        try {
            request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, Access.OK.isAllowed());
            byte[] response;
            Optional<ExtractionNamespace> extractionNamespace = mahaNamespaceExtractionCacheManager.getExtractionNamespace(namespace);
            if(!extractionNamespace.isPresent()) {

                return Response.ok().entity(new byte[0]).build();

            } else {
                if (key != null) {
                    if (debug) {
                        log.info("Fetching cache value for key [%s] and valueColumn [%s], namespace [%s]", key, valueColumn, namespace);
                    }

                    Optional<DecodeConfig> decodeConfigOptional = Optional.empty();
                    if(decodeConfigString != null) {
                        DecodeConfig decodeConfig = objectMapper.readValue(URLDecoder.decode(decodeConfigString, UTF_8.toString()), DecodeConfig.class);
                        if(debug) {
                            log.info("decodeConfig [%s]", decodeConfig);
                        }
                        decodeConfigOptional = Optional.of(decodeConfig);
                    }

                    response = mahaNamespaceExtractionCacheManager
                            .getExtractionNamespaceFunctionFactory(Class.forName(extractionNamespaceClass))
                            .getCacheValue(extractionNamespace.get(),
                                    mahaNamespaceExtractionCacheManager.getCacheMap(namespace), key, valueColumn, decodeConfigOptional);
                    if (debug && response != null) {
                        log.info("Cache value is : [%s]", new String(response));
                    }
                } else {
                    log.warn("Key is not passed hence returning the size of the cache, namespace[%s]", namespace);
                    response = mahaNamespaceExtractionCacheManager
                            .getExtractionNamespaceFunctionFactory(Class.forName(extractionNamespaceClass))
                            .getCacheSize(extractionNamespace.get(),
                                    mahaNamespaceExtractionCacheManager.getCacheMap(namespace)).getBytes();
                }
                serviceEmitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_GET_CACHE_VALUE_SUCCESS, 1));
                return Response.ok().entity(response).build();
            }
        } catch (Exception ex) {
            log.error(ex, "Can not get cache value for namespace and key");
            serviceEmitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_GET_CACHE_VALUE_FAILURE, 1));
            return Response.serverError().entity(Strings.nullToEmpty(ex.getMessage())).build();
        }
    }

    @GET
    @Produces({ MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
    @Path("{namespace}/lastUpdatedTime")
    public Response getLastUpdatedTime(@PathParam("namespace") String namespace,
                                  @QueryParam("namespaceclass") String extractionNamespaceClass, @Context final HttpServletRequest request) {
        try {
            request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, Access.OK.isAllowed());
            log.info("Fetching lastUpdatedTime namespace [%s]", namespace);

            Optional<ExtractionNamespace> extractionNamespace = mahaNamespaceExtractionCacheManager.getExtractionNamespace(namespace);
            Long lastUpdatedTime = -1L;
            if(extractionNamespace.isPresent()) {
                lastUpdatedTime = mahaNamespaceExtractionCacheManager
                        .getExtractionNamespaceFunctionFactory(Class.forName(extractionNamespaceClass))
                        .getLastUpdatedTime(extractionNamespace.get());
            }
            log.info("LastUpdatedTime is : [%s]", lastUpdatedTime);

            return Response.ok().entity(lastUpdatedTime).build();
        } catch (Exception ex) {
            log.error(ex, "Can not get lastUpdatedTime for namespace");
            return Response.serverError().entity(Strings.nullToEmpty(ex.getMessage())).build();
        }
    }

}
