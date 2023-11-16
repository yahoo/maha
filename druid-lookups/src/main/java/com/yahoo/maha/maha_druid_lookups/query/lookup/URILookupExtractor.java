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
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.URIExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import org.apache.druid.java.util.common.logger.Logger;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class URILookupExtractor<U extends List<String>> extends OnlineDatastoreLookupExtractor<U> {
    private static final Logger LOG = new Logger(MethodHandles.lookup().lookupClass());

    public URILookupExtractor(URIExtractionNamespace extractionNamespace, Map<String, U> map, LookupService lookupService) {
        super(extractionNamespace, map, lookupService);
    }

    @Override
    protected Logger LOGGER() {
        return LOG;
    }

    @Override
    public boolean canIterate() {
        return true;
    }

    @Override
    public boolean canGetKeySet()
    {
        return true;
    }

    @Override
    public Iterable<Map.Entry<String, String>> iterable() {
        return super.iterable();
    }

    @Override
    public Set<String> keySet()
    {
        return getMap().keySet();
    }
}
