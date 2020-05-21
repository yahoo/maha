// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

public class NoopProtobufSchemaFactory implements ProtobufSchemaFactory {

    public Descriptors.Descriptor getProtobufDescriptor(final String messageType) {
        throw new UnsupportedOperationException("please configure appropriate factory!");
    }

    public Parser getProtobufParser(final String messageType) {
        throw new UnsupportedOperationException("please configure appropriate factory!");

    }

    public Message.Builder getProtobufMessageBuilder(final String messageType) {
        throw new UnsupportedOperationException("please configure appropriate factory!");
    }

}
