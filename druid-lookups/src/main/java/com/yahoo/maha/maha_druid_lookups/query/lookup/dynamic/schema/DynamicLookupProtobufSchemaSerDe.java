package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.yahoo.maha.maha_druid_lookups.query.lookup.*;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.*;

import java.util.Optional;
import java.util.stream.*;
import org.slf4j.*;

public class DynamicLookupProtobufSchemaSerDe implements DynamicLookupCoreSchema {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicLookupProtobufSchemaSerDe.class);

    private Descriptors.Descriptor protobufMessageDescriptor;
    private String fieldsCsv;

    public DynamicLookupProtobufSchemaSerDe(DynamicLookupSchema dynamicLookupSchema) throws Descriptors.DescriptorValidationException {
        DescriptorProtos.FileDescriptorProto.Builder fileDescProtoBuilder = DescriptorProtos.FileDescriptorProto
                .newBuilder();
        try {
            DescriptorProtos.DescriptorProto.Builder builder = DescriptorProtos.DescriptorProto.newBuilder();

            builder.setName(dynamicLookupSchema.getName());
            dynamicLookupSchema.getSchemaFieldList().forEach(field-> {
                builder.addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName(field.getField())
                        .setNumber(field.getIndex())
                        .setType(mapType(field.getDataType()))
                        .build());
            });
            DescriptorProtos.FileDescriptorProto fileDescriptorProto = fileDescProtoBuilder.addMessageType(builder.build()).build();
            protobufMessageDescriptor = Descriptors.FileDescriptor
                    .buildFrom(fileDescriptorProto,
                            new Descriptors.FileDescriptor[0])
                    .findMessageTypeByName(dynamicLookupSchema.getName());
            fieldsCsv = protobufMessageDescriptor.getFields().stream().map(s-> s.getName()).collect(Collectors.joining(", "));
        } catch (Exception ex) {
            LOG.error("failed to build protobufMessageDescriptor for schema: {}", dynamicLookupSchema, ex);
            throw ex;
        }
    }

    private DescriptorProtos.FieldDescriptorProto.Type mapType(FieldDataType fieldDataType) {
        // TODO Add Support for other types
        if (fieldDataType == FieldDataType.INT32) {
            return DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32;
        }

        if (fieldDataType == FieldDataType.INT64) {
            return DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64;
        }

        if (fieldDataType == FieldDataType.DOUBLE) {
            return DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE;
        }

        if (fieldDataType == FieldDataType.BOOL) {
            return DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL;
        }

        return DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING;
    }


    public ExtractionNameSpaceSchemaType getSchemaType(){
        return ExtractionNameSpaceSchemaType.PROTOBUF;
    }


    private Optional<DynamicMessage> getDynamicMessage(byte[] dataBytes, RocksDBExtractionNamespace extractionNamespace) {
        try {
            return Optional.of(DynamicMessage.parseFrom(protobufMessageDescriptor, dataBytes));
        } catch (Exception e) {
            LOG.error("Failed to parse as generic protobuf Message, namespace {}",extractionNamespace.getLookupName(), e);
        }
        return Optional.empty();
    }

    @Override
    public String getValue(String fieldName, byte[] dataBytes, Optional<DecodeConfig> decodeConfigOptional, RocksDBExtractionNamespace extractionNamespace) {
        Optional<DynamicMessage> dynamicMessageOptional = getDynamicMessage(dataBytes, extractionNamespace);
        if (!dynamicMessageOptional.isPresent()) {
            LOG.error("Failed to getValue for field '{}' in schema: [{}], namespace: {}}", fieldName, fieldsCsv, extractionNamespace.getLookupName());
           throw new RuntimeException("dynamicMessageOptional is empty");
        }
        DynamicMessage dynamicMessage = dynamicMessageOptional.get();
        String fieldValue = getValueForField(fieldName, dynamicMessage, extractionNamespace);
        if (decodeConfigOptional.isPresent()) {
            return handleDecode(decodeConfigOptional.get(), dynamicMessage, extractionNamespace);
        } else {
            return fieldValue;
        }
    }

    private String getValueForField(String fieldName, DynamicMessage dynamicMessage, ExtractionNamespace extractionNamespace) {
        Descriptors.FieldDescriptor fieldDescriptor =  protobufMessageDescriptor.findFieldByName(fieldName);
        if (fieldDescriptor == null) {
            LOG.error("Failed to find fieldDescriptor for field: '{}' in schema: [{}], namespace: {}}", fieldName, fieldsCsv, extractionNamespace.getLookupName());
           return "";
        }
        if (dynamicMessage.hasField(fieldDescriptor)) {
            String fieldValue = String.valueOf(dynamicMessage.getField(fieldDescriptor));
            return fieldValue != null ? fieldValue : "";
        } else {
            //if empty value for specific field, decoded dynamic message won't have the fieldDescriptor for that field
            LOG.info("Failed to find value for field: {} namespace: {}", fieldName, extractionNamespace.getLookupName());
        }
        return "";
    }

    public String handleDecode(DecodeConfig decodeConfig, DynamicMessage dynamicMessage, ExtractionNamespace extractionNamespace) {
        try {
            String columnToCheck = getValueForField(decodeConfig.getColumnToCheck(), dynamicMessage, extractionNamespace);

            if (decodeConfig.getValueToCheck().equals(columnToCheck)) {
                return getValueForField(decodeConfig.getColumnIfValueMatched(), dynamicMessage, extractionNamespace);
            } else {
                return getValueForField(decodeConfig.getColumnIfValueNotMatched(), dynamicMessage, extractionNamespace);
            }
        } catch (Exception e) {
            LOG.error("Caught exception while handleDecode: {}", e.getMessage(), e);
            throw e;
        }
    }

    public Descriptors.Descriptor getProtobufMessageDescriptor() {
        return protobufMessageDescriptor;
    }

    @Override
    public String toString() {
        return "DynamicLookupProtobufSchemaSerDe() : ";
    }
}

