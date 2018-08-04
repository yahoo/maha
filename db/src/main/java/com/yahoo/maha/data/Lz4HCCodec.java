// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.data;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * not thread safe
 * Created by hiral on 8/28/14.
 */
public class Lz4HCCodec implements Compressor {
    private final LZ4Factory factory = LZ4Factory.fastestInstance();
    private final LZ4Compressor compressor = factory.highCompressor();
    private final LZ4SafeDecompressor decompressor = factory.safeDecompressor();

    public void compressToBB(byte[] bytes, ByteBuffer outputBuffer) throws IOException {
        compressor.compress(ByteBuffer.wrap(bytes), outputBuffer);
    }

    @Override
    public int compress(byte[] bytes, int offset, int len, byte[] outputBuffer) throws IOException {
        return compressor.compress(bytes, offset, len, outputBuffer, 0);
    }

    @Override
    public void compressBB(ByteBuffer byteBuffer, ByteBuffer outputBuffer) throws IOException {
        compressor.compress(byteBuffer, outputBuffer);
    }

    @Override
    public void decompressToBB(byte[] bytes, ByteBuffer outputBuffer) throws IOException {
        decompressor.decompress(ByteBuffer.wrap(bytes), outputBuffer);
    }

    @Override
    public int decompress(byte[] bytes, byte[] outputBuffer) throws IOException {
        return decompressor.decompress(bytes, outputBuffer);
    }

    @Override
    public void decompressBB(ByteBuffer byteBuffer, ByteBuffer outputBuffer) throws IOException {
        decompressor.decompress(byteBuffer, outputBuffer);
    }

    @Override
    public Codec codec() {
        return Codec.LZ4HC;
    }
}
