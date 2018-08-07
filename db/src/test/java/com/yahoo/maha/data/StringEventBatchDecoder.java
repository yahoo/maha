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
public class StringEventBatchDecoder implements Decoder<StringEventBatch> {

    private final int bufferSize;
    private final LazyInitializer<ByteBuffer> lazyInputByteBuffer;
    private final LazyInitializer<ByteBuffer> lazyOutputByteBuffer;
    private final LazyInitializer<byte[]> lazyOutputByteArray;
    private final Compressor compressor;

    StringEventBatchDecoder(Compressor.Codec codec) {
        this(codec, 1);
    }
    StringEventBatchDecoder(Compressor.Codec codec, int bufferMb) {
        bufferSize = bufferMb * 1024 * 1024;
        Preconditions.checkNotNull(codec, "Codec cannot be null!");
        compressor = CompressorFactory.getCompressor(codec);
        Preconditions.checkNotNull(compressor, "Failed to get compressor from codec");
        lazyInputByteBuffer = new LazyInitializer<ByteBuffer>() {
            @Override
            protected ByteBuffer initialize() throws ConcurrentException {
                return ByteBuffer.allocate(bufferSize);
            }
        };
        lazyOutputByteBuffer = new LazyInitializer<ByteBuffer>() {
            @Override
            protected ByteBuffer initialize() throws ConcurrentException {
                return ByteBuffer.allocate(bufferSize);
            }
        };
        lazyOutputByteArray = new LazyInitializer<byte[]>() {
            @Override
            protected byte[] initialize() throws ConcurrentException {
                return new byte[bufferSize];
            }
        };
    }

    @Override
    public StringEventBatch decode(byte[] bytes) {
        if(bytes == null)
            return null;
        try {
            ByteBuffer outputBuffer = lazyOutputByteBuffer.get();
            outputBuffer.clear();
            compressor.decompressToBB(bytes, outputBuffer);
            outputBuffer.flip();
            int numElements = outputBuffer.getInt();
            StringEventBatch.Builder builder = new StringEventBatch.Builder(numElements);
            for(int i = 0; i<numElements; i++) {
                int length = outputBuffer.getInt();
                byte[] elemBytes = new byte[length];
                outputBuffer.get(elemBytes, 0, length);
                builder.add(new String(elemBytes));
            }
            return (StringEventBatch) builder.build();
        } catch (IOException | ConcurrentException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public StringEventBatch decode(ByteBuffer byteBuffer) {
        if(byteBuffer == null)
            return null;
        try {
            ByteBuffer outputBuffer = lazyOutputByteBuffer.get();
            outputBuffer.clear();
            compressor.decompressBB(byteBuffer, outputBuffer);
            outputBuffer.flip();
            int numElements = outputBuffer.getInt();
            StringEventBatch.Builder builder = new StringEventBatch.Builder(numElements);
            for(int i = 0; i<numElements; i++) {
                int length = outputBuffer.getInt();
                byte[] elemBytes = new byte[length];
                outputBuffer.get(elemBytes, 0, length);
                builder.add(new String(elemBytes));
            }
            return (StringEventBatch) builder.build();
        } catch (IOException | ConcurrentException e) {
            throw new RuntimeException(e);
        }
    }
}
