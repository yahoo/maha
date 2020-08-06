package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.flatbuffer;

import org.apache.commons.lang.math.NumberUtils;

public class FlatBufferValue {
    String value;
    int offsetInBuffer;

    public FlatBufferValue(String value) {
        this.value = value;
    }

    public static FlatBufferValue of(String value) {
        return new FlatBufferValue(value);
    }

    public String getValue() {
        return value;
    }

    public int getOffsetInBuffer() {
        return offsetInBuffer;
    }

    public void setOffsetInBuffer(int offsetInBuffer) {
        this.offsetInBuffer = offsetInBuffer;
    }

}
