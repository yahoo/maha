package com.yahoo.maha.maha_druid_lookups.flatbuffer;

import com.google.flatbuffers.Table;

import java.util.Map;

public interface FlatBufferWrapper {
    public Map<String, Integer> getFieldNameToFieldOffsetMap();
    public String readFieldValue(String fieldName, Table flatBuffer);
}