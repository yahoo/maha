// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.data;

import com.google.common.base.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by hiral on 8/28/14.
 */
public class GZIPCodec implements Compressor {
    private final ThreadLocal<ByteBufferOutputStream> compressOutputStream =
            ThreadLocal.withInitial(ByteBufferOutputStream::new);
    private final ThreadLocal<ByteBufferOutputStream> compressToBBOutputStream =
            ThreadLocal.withInitial(ByteBufferOutputStream::new);
    private final ThreadLocal<ByteBufferOutputStream> compressBBOutputStream =
            ThreadLocal.withInitial(ByteBufferOutputStream::new);

    @Override
    public void compressToBB(byte[] bytes, ByteBuffer outputBuffer) throws IOException {
        int buffSize = outputBuffer.capacity() - outputBuffer.position();
        Preconditions.checkArgument((bytes.length + 128) <= buffSize
                , "Buffer must be at least 128 bytes greater than input size"
        );
        ByteBufferOutputStream byteBufferOutputStream = compressToBBOutputStream.get();
        byteBufferOutputStream.set(outputBuffer);
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteBufferOutputStream);
        gzipOutputStream.write(bytes);
        gzipOutputStream.finish();
        byteBufferOutputStream.set(null);
    }

    @Override
    public int compress(byte[] bytes, int offset, int len, byte[] outputBuffer) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.wrap(outputBuffer);
        ByteBufferOutputStream byteBufferOutputStream = compressOutputStream.get();
        byteBufferOutputStream.set(byteBuffer);
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteBufferOutputStream);
        gzipOutputStream.write(bytes, offset, len);
        gzipOutputStream.finish();
        byteBufferOutputStream.set(null);
        byteBuffer.flip();
        return byteBuffer.limit();
    }

    @Override
    public void compressBB(ByteBuffer byteBuffer, ByteBuffer outputBuffer) throws IOException {
        ByteBufferOutputStream byteBufferOutputStream = compressBBOutputStream.get();
        byteBufferOutputStream.set(outputBuffer);
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteBufferOutputStream);
        gzipOutputStream.write(byteBuffer.array(), byteBuffer.arrayOffset(), byteBuffer.limit());
        gzipOutputStream.finish();
        byteBufferOutputStream.set(null);
    }

    @Override
    public void decompressToBB(byte[] bytes, ByteBuffer outputBuffer) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream,bytes.length);
        int p1 = outputBuffer.position();
        int pos = gzipInputStream.read(outputBuffer.array(), outputBuffer.arrayOffset(), outputBuffer.capacity() - p1);
        outputBuffer.position(pos + p1);
    }

    @Override
    public int decompress(byte[] bytes, byte[] outputBuffer) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream,bytes.length);
        return gzipInputStream.read(outputBuffer, 0, outputBuffer.length);
    }

    @Override
    public void decompressBB(ByteBuffer byteBuffer, ByteBuffer outputBuffer) throws IOException {
        int inputSize = byteBuffer.limit();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteBuffer.array(), byteBuffer.arrayOffset(), inputSize);
        GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream, inputSize);
        int p1 = outputBuffer.position();
        int pos = gzipInputStream.read(outputBuffer.array(), outputBuffer.arrayOffset(), outputBuffer.capacity() - p1);
        outputBuffer.position(pos + p1);
    }

    @Override
    public Codec codec() {
        return Codec.GZIP;
    }


}
