// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

public class TestProtobufSchemaFactory implements ProtobufSchemaFactory {

    @Override
    public Descriptors.Descriptor getProtobufDescriptor(final String messageType) {
        if ("ad_lookup".equals(messageType)) {
            return AdProtos.Ad.getDescriptor();
        }
        throw new IllegalArgumentException("unknown namespace");
    }

    @Override
    public Parser getProtobufParser(final String messageType) {
        if ("ad_lookup".equals(messageType)) {
            return AdProtos.Ad.PARSER;
        }
        throw new IllegalArgumentException("unknown namespace");
    }

    @Override
    public Message.Builder getProtobufMessageBuilder(final String messageType) {
        if ("ad_lookup".equals(messageType)) {
            return AdProtos.Ad.newBuilder();
        }
        throw new IllegalArgumentException("unknown namespace");
    }
}

