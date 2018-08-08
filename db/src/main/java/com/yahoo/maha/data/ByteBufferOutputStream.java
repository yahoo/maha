// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.data;

/**
 * Created by shrav87 on 11/21/16.
 */

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

class ByteBufferOutputStream extends OutputStream {
    private ByteBuffer buffer = null;

    public ByteBufferOutputStream() {
    }

    public void set(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        }
        if(len > this.buffer.capacity() - this.buffer.position()) {
            throw new ByteBufferOutputStream.BufferOverflowException("ByteBuffer is not large enough.");
        } else {
            this.buffer.put(b, off, len);
        }
    }

    public void write(int b) throws IOException {
        if(1 > this.buffer.capacity() - this.buffer.position()) {
            throw new ByteBufferOutputStream.BufferOverflowException("ByteBuffer is not large enough.");
        } else {
            this.buffer.put((byte)b);
        }
    }

    public static class BufferOverflowException extends IOException {
        private static final long serialVersionUID = 1L;

        public BufferOverflowException(String message) {
            super(message);
        }
    }
}
