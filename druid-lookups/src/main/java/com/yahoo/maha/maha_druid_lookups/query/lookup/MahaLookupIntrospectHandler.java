// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.google.common.collect.ImmutableMap;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache.MahaExtractionCacheManager;
import io.druid.common.utils.ServletResourceUtils;
import io.druid.java.util.common.ISE;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.lookup.LookupIntrospectHandler;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

public class MahaLookupIntrospectHandler implements LookupIntrospectHandler
{
    private final MahaLookupExtractorFactory factory;
    private final String extractorID;
    private final MahaExtractionCacheManager manager;
    public MahaLookupIntrospectHandler(
            MahaLookupExtractorFactory factory,
            MahaExtractionCacheManager manager,
            String extractorID
    ) {
        this.factory = factory;
        this.extractorID = extractorID;
        this.manager = manager;
    }
    @GET
    @Path("/keys")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getKeys()
    {
        try {
            return Response.ok(getLatest().keySet()).build();
        }
        catch (ISE e) {
            return Response.status(Response.Status.NOT_FOUND).entity(ServletResourceUtils.sanitizeException(e)).build();
        }
    }

    @GET
    @Path("/values")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getValues()
    {
        try {
            return Response.ok(getLatest().values()).build();
        }
        catch (ISE e) {
            return Response.status(Response.Status.NOT_FOUND).entity(ServletResourceUtils.sanitizeException(e)).build();
        }
    }

    @GET
    @Path("/version")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getVersion()
    {
        final String version = manager.getVersion(extractorID);
        if (null == version) {
            // Handle race between delete and this method being called
            return Response.status(Response.Status.NOT_FOUND).build();
        } else {
            return Response.ok(ImmutableMap.of("version", version)).build();
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getMap()
    {
        try {
            return Response.ok(getLatest()).build();
        }
        catch (ISE e) {
            return Response.status(Response.Status.NOT_FOUND).entity(ServletResourceUtils.sanitizeException(e)).build();
        }
    }

    private Map<String, String> getLatest()
    {
        return ((MapLookupExtractor) factory.get()).getMap();
    }
}
