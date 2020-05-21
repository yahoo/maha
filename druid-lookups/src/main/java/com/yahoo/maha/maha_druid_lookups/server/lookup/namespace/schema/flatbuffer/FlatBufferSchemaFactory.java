package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.flatbuffer;

import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.BaseSchemaFactory;

public interface FlatBufferSchemaFactory extends BaseSchemaFactory {
    FlatBufferWrapper getFlatBuffer(final String messageType);
}