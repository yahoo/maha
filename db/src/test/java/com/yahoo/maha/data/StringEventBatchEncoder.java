// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.data;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.concurrent.ConcurrentException;
import org.apache.commons.lang3.concurrent.LazyInitializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by hiral on 8/2/18.
 */
public class StringEventBatchEncoder implements Encoder<StringEventBatch> {

    private final int bufferSize;
    private final LazyInitializer<ByteBuffer> lazyInputByteBuffer;
    private final LazyInitializer<ByteBuffer> lazyOutputByteBuffer;
    private final LazyInitializer<byte[]> lazyOutputByteArray;
    private final Compressor compressor;

    public StringEventBatchEncoder(Compressor.Codec codec) {
        this(codec, 1);
    }

    public StringEventBatchEncoder(Compressor.Codec codec, int bufferMb) {
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
    public int encode(StringEventBatch stringEventBatch, byte[] outputBuffer) {
        if(stringEventBatch == null)
            return -1;

        try {
            List<String> events = stringEventBatch.getEvents();
            ByteBuffer byteBuffer = lazyInputByteBuffer.get();
            byteBuffer.clear();
            byteBuffer.putInt(events.size());
            for(String event: events) {
                byte[] bytes = event.getBytes();
                byteBuffer.putInt(bytes.length);
                byteBuffer.put(bytes);
            }
            byteBuffer.flip();
            return compressor.compress(byteBuffer.array()
                    , byteBuffer.arrayOffset(), byteBuffer.limit(), outputBuffer);
        } catch (IOException | ConcurrentException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void encode(StringEventBatch stringEventBatch, ByteBuffer outputBuffer) {
        if(stringEventBatch == null)
            return;

        try {
            List<String> events = stringEventBatch.getEvents();
            ByteBuffer byteBuffer = lazyInputByteBuffer.get();
            byteBuffer.clear();
            byteBuffer.putInt(events.size());
            for(String event: events) {
                byte[] bytes = event.getBytes();
                byteBuffer.putInt(bytes.length);
                byteBuffer.put(bytes);
            }
            byteBuffer.flip();
            compressor.compressBB(byteBuffer, outputBuffer);
        } catch (IOException | ConcurrentException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public byte[] encode(StringEventBatch stringEventBatch) {
        if(stringEventBatch == null)
            return null;

        try {
            List<String> events = stringEventBatch.getEvents();
            ByteBuffer byteBuffer = lazyInputByteBuffer.get();
            byteBuffer.clear();
            byteBuffer.putInt(events.size());
            for(String event: events) {
                byte[] bytes = event.getBytes();
                byteBuffer.putInt(bytes.length);
                byteBuffer.put(bytes);
            }
            byteBuffer.flip();
            int size = compressor.compress(byteBuffer.array()
                    , byteBuffer.arrayOffset(), byteBuffer.limit(), lazyOutputByteArray.get());
            byte[] output = new byte[size];
            System.arraycopy(lazyOutputByteArray.get(), 0, output, 0, size);
            return output;
        } catch (IOException | ConcurrentException e) {
            throw new RuntimeException(e);
        }
    }
}
