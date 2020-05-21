// Copyright 2018, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by hiral on 9/6/18.
 */
public class DefaultProtobufSchemaFactory implements ProtobufSchemaFactory {

    private final Map<String, GeneratedMessageV3> messageTypeProtoMap;
    private final Map<String, Descriptors.Descriptor> messageTypeDescriptorMap;
    private final Map<String, Parser> messageTypeParserMap;

    public DefaultProtobufSchemaFactory(ImmutableMap<String, GeneratedMessageV3> messageTypeProtoMap) {
        this.messageTypeProtoMap = messageTypeProtoMap;
        this.messageTypeDescriptorMap = messageTypeProtoMap
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().getDescriptorForType()));
        this.messageTypeParserMap = messageTypeProtoMap
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().getParserForType()));
    }

    @Override
    public Descriptors.Descriptor getProtobufDescriptor(String messageType) {
        Preconditions.checkArgument(messageType != null , "messageType cannot be null!");
        Descriptors.Descriptor descriptor = messageTypeDescriptorMap.get(messageType);
        Preconditions.checkArgument(descriptor != null , String.format("unknown messageType %s", messageType));
        return descriptor;
    }

    @Override
    public Parser getProtobufParser(String messageType) {
        Preconditions.checkArgument(messageType != null , "messageType cannot be null!");
        Parser parser = messageTypeParserMap.get(messageType);
        Preconditions.checkArgument(parser != null , String.format("unknown messageType %s", messageType));
        return parser;
    }

    @Override
    public Message.Builder getProtobufMessageBuilder(String messageType) {
        Preconditions.checkArgument(messageType != null , "messageType cannot be null!");
        GeneratedMessageV3 generatedMessageV3 = messageTypeProtoMap.get(messageType);
        Preconditions.checkArgument(generatedMessageV3 != null , String.format("unknown messageType %s", messageType));
        return generatedMessageV3.newBuilderForType();
    }
}
