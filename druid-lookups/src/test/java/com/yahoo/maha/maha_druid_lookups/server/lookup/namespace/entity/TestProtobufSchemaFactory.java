// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.GeneratedMessageV3;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.protobuf.DefaultProtobufSchemaFactory;

public class TestProtobufSchemaFactory extends DefaultProtobufSchemaFactory {
    public TestProtobufSchemaFactory() {
        super(ImmutableMap.<String, GeneratedMessageV3>of("ad_lookup", AdProtos.Ad.getDefaultInstance()));
    }
}

