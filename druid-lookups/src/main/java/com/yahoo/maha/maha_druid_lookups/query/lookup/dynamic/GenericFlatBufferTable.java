package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic;

import com.google.flatbuffers.Table;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class GenericFlatBufferTable extends Table {

    public GenericFlatBufferTable(ByteBuffer _bb) {
        _bb.order(ByteOrder.LITTLE_ENDIAN);
        __assign(_bb.getInt(_bb.position()) + _bb.position(), _bb);
    }

    public void __init(int _i, ByteBuffer _bb) {
        __reset(_i, _bb);
    }

    public void __assign(int _i, ByteBuffer _bb) {
        __init(_i, _bb);
    }

    public String getValue(int v_table_offset)  {
        int o = __offset(v_table_offset);
        return o != 0 ? __string(o + bb_pos) : null;
    }
}