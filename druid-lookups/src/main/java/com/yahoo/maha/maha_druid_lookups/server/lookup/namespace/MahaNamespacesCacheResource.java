// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache.MahaExtractionCacheManager;
import io.druid.server.security.Access;
import io.druid.server.security.AuthConfig;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Path("/druid/v1/namespaces")
public class MahaNamespacesCacheResource
{
    private static final Logger log = new Logger(MahaNamespacesCacheResource.class);
    private final MahaExtractionCacheManager mahaExtractionCacheManager;
    private final ServiceEmitter serviceEmitter;

    @Inject
    public MahaNamespacesCacheResource(final MahaExtractionCacheManager mahaExtractionCacheManager,
                                       final ServiceEmitter serviceEmitter){
        this.mahaExtractionCacheManager = mahaExtractionCacheManager;
        this.serviceEmitter = serviceEmitter;
    }

    @GET
    @Produces({ MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
    public Response getNamespaces(@Context final HttpServletRequest request){
        try{
            request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, Access.OK.isAllowed());
            Collection<String> namespaces = mahaExtractionCacheManager.getKnownIDs();
            Map<String, Integer> response = new HashMap<String, Integer>();
            for(String namespace: namespaces) {
                response.put(namespace, mahaExtractionCacheManager.getCacheMap(namespace).size());
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
                                  @QueryParam("debug") boolean debug, @Context final HttpServletRequest request) {
        try {
            request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, Access.OK.isAllowed());
            byte[] response;
            Optional<ExtractionNamespace> extractionNamespace = mahaExtractionCacheManager.getExtractionNamespace(namespace);
            if(!extractionNamespace.isPresent()) {

                return Response.ok().entity(new byte[0]).build();

            } else {
                if (key != null) {
                    if (debug) {
                        log.info("Fetching cache value for key [%s]", key);
                    }
                    response = mahaExtractionCacheManager
                            .getExtractionNamespaceFunctionFactory(Class.forName(extractionNamespaceClass))
                            .getCacheValue(extractionNamespace.get(),
                                    mahaExtractionCacheManager.getCacheMap(namespace), key);
                    if (debug && response != null) {
                        log.info("Cache value is : [%s]", new String(response));
                    }
                } else {
                    log.warn("Key is not passed hence returning the size of the cache");
                    response = mahaExtractionCacheManager
                            .getExtractionNamespaceFunctionFactory(Class.forName(extractionNamespaceClass))
                            .getCacheSize(extractionNamespace.get(),
                                    mahaExtractionCacheManager.getCacheMap(namespace)).getBytes();
                }
                serviceEmitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_GET_CACHE_VALUE_SUCESS, 1));
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

            Optional<ExtractionNamespace> extractionNamespace = mahaExtractionCacheManager.getExtractionNamespace(namespace);
            Long lastUpdatedTime = -1L;
            if(extractionNamespace.isPresent()) {
                lastUpdatedTime = mahaExtractionCacheManager
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
