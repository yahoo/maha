package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.flatbuffer;

import com.google.flatbuffers.Table;

import java.util.Map;

public abstract class FlatBufferWrapper {
    public abstract Map<String, Integer> getFieldNameToFieldOffsetMap();
    public abstract String readFieldValue(String fieldName, Table flatBuffer);
    public abstract Table getFlatBuffer(byte[] flatBufferBytes);

    public String readFieldValue(String fieldName, byte[] flatBufferBytes) {
        return readFieldValue(fieldName, getFlatBuffer(flatBufferBytes));
    }
}