// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.data;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.concurrent.ConcurrentException;
import org.apache.commons.lang3.concurrent.LazyInitializer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by hiral on 8/2/18.
 */
public class StringDecoder implements Decoder<String> {

    private final int bufferSize;
    private final Compressor compressor;
    private final LazyInitializer<ByteBuffer> lazyByteBuffer;
    private final LazyInitializer<byte[]> lazyByteArray;

    StringDecoder(Compressor.Codec codec) {
        this(codec, 1);
    }

    StringDecoder(Compressor.Codec codec, int bufferMb) {
        bufferSize = bufferMb * 1024 * 1024;
        Preconditions.checkNotNull(codec, "Codec cannot be null!");
        compressor = CompressorFactory.getCompressor(codec);
        Preconditions.checkNotNull(compressor, "Failed to get compressor from codec");
        lazyByteBuffer = new LazyInitializer<ByteBuffer>() {
            @Override
            protected ByteBuffer initialize() throws ConcurrentException {
                return ByteBuffer.allocate(bufferSize);
            }
        };
        lazyByteArray = new LazyInitializer<byte[]>() {
            @Override
            protected byte[] initialize() throws ConcurrentException {
                return new byte[bufferSize];
            }
        };
    }

    @Override
    public String decode(byte[] bytes) {
        if(bytes == null)
            return null;
        try {
            int size = (compressor.decompress(bytes, lazyByteArray.get()));
            return new String(lazyByteArray.get(), 0, size);
        } catch (IOException | ConcurrentException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String decode(ByteBuffer byteBuffer) {
        if(byteBuffer == null)
            return null;
        try {
            ByteBuffer outputBuffer = lazyByteBuffer.get();
            outputBuffer.clear();
            compressor.decompressBB(byteBuffer, outputBuffer);
            outputBuffer.flip();
            return new String(outputBuffer.array(), outputBuffer.arrayOffset(), outputBuffer.limit());
        } catch (IOException | ConcurrentException e) {
            throw new RuntimeException(e);
        }
    }
}
