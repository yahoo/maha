package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.Descriptors;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.*;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;

public class DynamicLookupCoreSchemaFactory {

    private static final Logger LOG = new Logger(DynamicLookupCoreSchemaFactory.class);

    public static DynamicLookupCoreSchema buildSchema(ExtractionNameSpaceSchemaType schemaType, JsonNode coreSchema) throws IOException , Descriptors.DescriptorValidationException {
        DynamicLookupCoreSchema dynamicLookupCoreSchema = null;

        switch (schemaType) {
            case PROTOBUF:
                dynamicLookupCoreSchema = new DynamicLookupProtobufSchemaSerDe(coreSchema);
                break;

            case FLAT_BUFFER:
                dynamicLookupCoreSchema = new DynamicLookupFlatbufferSchemaSerDe(coreSchema);
                break;
            default:
                //should never reach this code
                LOG.error("Schema_type is not currently supported" + schemaType.toString());
                throw new IllegalArgumentException("Schema_type " + schemaType.toString() + "is not currently supported");

        }

        return dynamicLookupCoreSchema;
    }
}

