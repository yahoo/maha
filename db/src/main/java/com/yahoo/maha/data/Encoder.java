// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.data;

import java.nio.ByteBuffer;

/**
 * Created by hiral on 8/2/18.
 */
interface Encoder<T> {
    int encode(T t, byte[] outputBuffer);
    void encode(T t, ByteBuffer outputBuffer);
    byte[] encode(T t);
}
