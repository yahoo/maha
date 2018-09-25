// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.data;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by hiral on 8/28/14.
 */
public interface Compressor {
    public static final String COMPRESSOR_CODEC_PROPERTY = "maha.compressor.codec";
    void compressToBB(byte[] bytes, ByteBuffer outputBuffer) throws IOException;
    default public int compress(byte[] bytes, byte[] outputBuffer) throws IOException {
       return compress(bytes, 0, bytes.length, outputBuffer);
    }
    int compress(byte[] bytes, int offset, int len, byte[] outputBuffer) throws IOException;
    void compressBB(ByteBuffer byteBuffer, ByteBuffer outputBuffer) throws IOException;
    void decompressToBB(byte[] bytes, ByteBuffer outputBuffer) throws IOException;
    int decompress(byte[] bytes, byte[] outputBuffer) throws IOException;
    void decompressBB(ByteBuffer byteBuffer, ByteBuffer outputBuffer) throws IOException;

    enum Codec {
        GZIP(1,1), LZ4(2,2), LZ4HC(3,3), NONE(4,4);

        private final int index;
        private final int value;

        private Codec(int index, int value) {
            this.index = index;
            this.value = value;
        }
    }

    Codec codec();
}
