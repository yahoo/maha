// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.data;

/**
 * Created by hiral on 8/28/14.
 */
public class CompressorFactory {

    public static Compressor getCompressor(Compressor.Codec codec) {
        switch (codec) {
            case GZIP:
                return new GZIPCodec();
            case LZ4:
                return new Lz4Codec();
            case LZ4HC:
                return new Lz4HCCodec();
            default:
                return new PassThroughCodec();
        }
    }
}
