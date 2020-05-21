package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.flatbuffer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class DefaultFlatBufferSchemaFactory implements FlatBufferSchemaFactory {

    Map<String, FlatBufferWrapper> messageTypeToFbWrapperMap;

    public DefaultFlatBufferSchemaFactory(ImmutableMap<String, FlatBufferWrapper> messageTypeToFbWrapperMap) {
        this.messageTypeToFbWrapperMap = messageTypeToFbWrapperMap;
    }

    @Override
    public FlatBufferWrapper getFlatBuffer(String messageType) {
        Preconditions.checkArgument(messageType != null , "messageType cannot be null!");
        Preconditions.checkArgument(messageTypeToFbWrapperMap.containsKey(messageType), String.format("Failed to find messageType %s in DefaultFlatBufferSchemaFactory factory"));
        return messageTypeToFbWrapperMap.get(messageType);
    }
}
