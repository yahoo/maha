package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.flatbuffer;

import com.google.common.collect.ImmutableMap;

public class TestFlatBufferSchemaFactory extends DefaultFlatBufferSchemaFactory {

    public TestFlatBufferSchemaFactory() {
        super(ImmutableMap.<String, FlatBufferWrapper>of("product_ad_fb_lookup", new ProductAdWrapper()));
    }
}