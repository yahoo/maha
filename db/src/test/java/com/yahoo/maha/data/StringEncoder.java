package com.yahoo.maha.data;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.concurrent.ConcurrentException;
import org.apache.commons.lang3.concurrent.LazyInitializer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by hiral on 8/2/18.
 */
public class StringEncoder implements Encoder<String> {

    private final int bufferSize;
    private final Compressor compressor;
    private final LazyInitializer<ByteBuffer> lazyByteBuffer;
    private final LazyInitializer<byte[]> lazyByteArray;

    StringEncoder(Compressor.Codec codec) {
        this(codec, 1);
    }

    StringEncoder(Compressor.Codec codec, int bufferMb) {
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
    public int encode(String s, byte[] outputBuffer) {
        byte[] bytes = s.getBytes();
        Preconditions.checkArgument(outputBuffer.length >= bytes.length, "Output buffer size too small!");
        try {
            return compressor.compress(bytes, outputBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void encode(String s, ByteBuffer outputBuffer) {
        try {
            compressor.compressToBB(s.getBytes(), outputBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] encode(String s) {
        if(s == null)
            return null;
        byte[] bytes = s.getBytes();
        try {
            int size = compressor.compress(bytes, lazyByteArray.get());
            byte[] output = new byte[size];
            System.arraycopy(lazyByteArray.get(), 0, output, 0, size);
            return output;
        } catch (IOException | ConcurrentException e) {
            throw new RuntimeException(e);
        }
    }
}
