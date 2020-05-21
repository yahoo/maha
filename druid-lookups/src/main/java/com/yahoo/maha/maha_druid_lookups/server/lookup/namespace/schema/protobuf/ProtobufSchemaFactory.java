// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.BaseSchemaFactory;

public interface ProtobufSchemaFactory extends BaseSchemaFactory {

    Descriptors.Descriptor getProtobufDescriptor(final String messageType);

    Parser getProtobufParser(final String messageType);

    Message.Builder getProtobufMessageBuilder(final String messageType);

}
