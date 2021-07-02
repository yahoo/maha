package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema;

import com.google.protobuf.Descriptors;
import org.apache.druid.java.util.common.logger.Logger;


public class DynamicLookupCoreSchemaFactory {

    private static final Logger LOG = new Logger(DynamicLookupCoreSchemaFactory.class);

    public static DynamicLookupCoreSchema buildSchema(DynamicLookupSchema dynamicLookupSchema) throws Descriptors.DescriptorValidationException {
        switch (dynamicLookupSchema.getType()) {
            case PROTOBUF:
                return new DynamicLookupProtobufSchemaSerDe(dynamicLookupSchema);
            case FLAT_BUFFER:
                return new DynamicLookupFlatbufferSchemaSerDe(dynamicLookupSchema);
            default:
                //should never reach this code
                String error = String.format("Unsupported Schema Type %s for in DynamicLookup schema ", dynamicLookupSchema.getType());
                LOG.error(error);
                throw new IllegalArgumentException(error);
        }
    }
}

