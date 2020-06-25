package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.flatbuffer;

import com.google.common.collect.ImmutableMap;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.Table;
import org.apache.druid.java.util.common.logger.Logger;

import java.nio.ByteBuffer;
import java.util.Map;

public class ProductAdWrapper extends FlatBufferWrapper {

    private static final Logger LOG = new Logger(ProductAdWrapper.class);

    Map<String, Integer> fieldNameToOffsetMap = ImmutableMap.<String, Integer> builder()
            .put("id", 0)
            .put("description", 1)
            .put("status", 2)
            .put("image_url_hq", 3)
            .put("image_url_large", 4)
            .put("source_id", 5)
            .put("title", 6)
            .build();

    @Override
    public Map<String, Integer> getFieldNameToFieldOffsetMap() {
        return fieldNameToOffsetMap;
    }

    @Override
    public String readFieldValue(String fieldName, Table productAdFb) {
        ProductAd productAd = (ProductAd) productAdFb;
        if (!fieldNameToOffsetMap.containsKey(fieldName)) {
           return null;
        }
        int offset = fieldNameToOffsetMap.get(fieldName);
        switch(offset) {
            case 0:
                return new Long(productAd.id()).toString();
            case 1:
                return productAd.description();
            case 2:
                return productAd.status();
            case 3:
                return productAd.imageUrlHq();
            case 4:
                return productAd.imageUrlLarge();
            case 5:
                return productAd.sourceId();
            case 6:
                return productAd.title();
            default:
                return null;
        }
    }

    @Override
    public Table getFlatBuffer(byte[] flatBufferBytes) {
        return ProductAd.getRootAsProductAd(ByteBuffer.wrap(flatBufferBytes));
    }

    @Override
    public FlatBufferBuilder createFlatBuffer(Map<String, FlatBufferValue> nameToValueMap) {
        FlatBufferBuilder bufferBuilder = new FlatBufferBuilder(512);

        //Create Index in the buffer builder
        for (Map.Entry<String,FlatBufferValue> entry : nameToValueMap.entrySet()) {
            if (fieldNameToOffsetMap.containsKey(entry.getKey())) {
                int index = bufferBuilder.createString(entry.getValue().getValue());
                entry.getValue().setOffsetInBuffer(index);
            } else {
                LOG.error(String.format("Failed to find the field name '%s' in the fieldNameToOffsetMap, please update the offset map and flat buffer wrapper", entry.getKey()));
            }
        }

        // add indices to values in buffer builder
        ProductAd.startProductAd(bufferBuilder);
        for (Map.Entry<String, FlatBufferValue> entry : nameToValueMap.entrySet()) {
            int fbFieldOffset = fieldNameToOffsetMap.get(entry.getKey());
            bufferBuilder.addOffset(fbFieldOffset, entry.getValue().getOffsetInBuffer(), 0);
        }

        int endRoot = ProductAd.endProductAd(bufferBuilder);
        ProductAd.finishProductAdBuffer(bufferBuilder, endRoot);
        return bufferBuilder;
    }
}