package com.yahoo.maha.data;

import java.nio.ByteBuffer;

/**
 * Created by hiral on 8/2/18.
 */
interface Decoder<T> {
    T decode(byte[] bytes);
    T decode(ByteBuffer byteBuffer);
}
