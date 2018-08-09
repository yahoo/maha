// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.data;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by hiral on 9/13/14.
 */
public class PassThroughCodec implements Compressor {
    @Override
    public void compressToBB(byte[] bytes, ByteBuffer outputBuffer) throws IOException {
        outputBuffer.put(bytes);
    }

    @Override
    public int compress(byte[] bytes, int offset, int len, byte[] outputBuffer) throws IOException {
        System.arraycopy(bytes, offset, outputBuffer, 0, len);
        return len;
    }

    @Override
    public void compressBB(ByteBuffer byteBuffer, ByteBuffer outputBuffer) throws IOException {
        outputBuffer.put(byteBuffer);
    }

    @Override
    public void decompressToBB(byte[] bytes, ByteBuffer outputBuffer) throws IOException {
        outputBuffer.put(bytes);
    }

    @Override
    public int decompress(byte[] bytes, byte[] outputBuffer) throws IOException {
        System.arraycopy(bytes, 0, outputBuffer, 0, bytes.length);
        return bytes.length;
    }

    @Override
    public void decompressBB(ByteBuffer byteBuffer, ByteBuffer outputBuffer) throws IOException {
        outputBuffer.put(byteBuffer);
    }

    @Override
    public Codec codec() {
        return Codec.NONE;
    }
}
