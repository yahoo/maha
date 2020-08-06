package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.flatbuffer;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.MahaNamespaceExtractionConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

public class FlatBufferSchemaFactoryProvider implements Provider<FlatBufferSchemaFactory> {

    private static final Logger LOG = new Logger(FlatBufferSchemaFactoryProvider.class);

    private  FlatBufferSchemaFactory flatBufferSchemaFactory = new DefaultFlatBufferSchemaFactory(ImmutableMap.<String, FlatBufferWrapper>builder().build());

    @Inject
    public FlatBufferSchemaFactoryProvider(MahaNamespaceExtractionConfig config) {
        String schemaFactoryClass = config.getFlatBufferSchemaFactory();
        if (StringUtils.isNotBlank(schemaFactoryClass)) {
            Class clazz;
            try {
                clazz = Class.forName(schemaFactoryClass);
                if (FlatBufferSchemaFactory.class.isAssignableFrom(clazz)) {
                    flatBufferSchemaFactory = (FlatBufferSchemaFactory) clazz.newInstance();
                }
            } catch (Exception e) {
                LOG.error(e, "Failed to instantiate FlatBufferSchemaFactory from className {}", schemaFactoryClass);
                throw new IllegalArgumentException(e);
            }
        } else {
            LOG.warn("Implementation of FlatBufferSchemaFactory class name is black in the MahaNamespaceExtractionConfig, considering default implementation");
        }
    }

    @Override
    public FlatBufferSchemaFactory get() {
        return flatBufferSchemaFactory;
    }
}
