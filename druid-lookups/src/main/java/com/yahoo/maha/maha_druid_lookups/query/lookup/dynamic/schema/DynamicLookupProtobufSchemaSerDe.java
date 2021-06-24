package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.yahoo.maha.maha_druid_lookups.query.lookup.*;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.*;
import org.apache.druid.java.util.common.logger.Logger;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Optional;

public class DynamicLookupProtobufSchemaSerDe implements DynamicLookupCoreSchema {
    private static final Logger LOG = new Logger(DynamicLookupProtobufSchemaSerDe.class);

    private final JsonNode coreSchema;
    private Descriptors.Descriptor protobufMessageDescriptor;


    public DynamicLookupProtobufSchemaSerDe(JsonNode coreSchema) throws IOException, Descriptors.DescriptorValidationException {
        this.coreSchema = coreSchema;
        DescriptorProtos.FileDescriptorSet set = null;
        FileInputStream fin = null;
        String path ="";

        try {
            path = getDescFileName();
            fin = new FileInputStream(path);
            set = DescriptorProtos.FileDescriptorSet.parseFrom(fin);
            protobufMessageDescriptor = Descriptors.FileDescriptor.buildFrom(set.getFile(0), new Descriptors.FileDescriptor[]{}).getMessageTypes().get(0);
        } catch (Exception ex) {
            LOG.error("failed to read the binary coreSchema as protobuf Message or build FileDescriptor at path {} ", path, ex);
            throw ex;
        } finally {
            if(fin != null){
                try {
                    fin.close();
                } catch(Exception ex){
                    LOG.error("Failed closing FileInputStream for path {} ", path , ex);
                }
            }
        }
    }


    public ExtractionNameSpaceSchemaType getSchemaType(){
        return ExtractionNameSpaceSchemaType.PROTOBUF;
    }


    public String getDescFileName() throws IllegalArgumentException {
        if(coreSchema != null && coreSchema.has("descFilePath")){
            return coreSchema.get("descFilePath").textValue();
        } else {
            throw new IllegalArgumentException("Field coreSchema not present in schema file ");
        }
    }

    private Optional<DynamicMessage> getDynamicMessage(byte[] dataBytes, RocksDBExtractionNamespace extractionNamespace) {
        try {
            return Optional.of(DynamicMessage.parseFrom(protobufMessageDescriptor, dataBytes));
        } catch (Exception e) {
            LOG.error("failed to parse as generic protobuf Message, namespace {} ",extractionNamespace.getLookupName(), e);
        }
        return Optional.empty();
    }

    @Override
    public String getValue(String fieldName, byte[] dataBytes, Optional<DecodeConfig> decodeConfigOptional, RocksDBExtractionNamespace extractionNamespace) {
        Optional<DynamicMessage> dynamicMessageOptional = getDynamicMessage(dataBytes, extractionNamespace);

        if (!dynamicMessageOptional.isPresent()) {
           return "";
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
        if (dynamicMessage.hasField(fieldDescriptor)) {
            String fieldValue = (String) dynamicMessage.getField(fieldDescriptor);
            return fieldValue != null ? fieldValue : "";
        } else {
            LOG.error("Field missing in protobuf Message Descriptor for field: {}  in  {}", fieldName,  extractionNamespace.getLookupName());
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
            LOG.error(e, "Caught exception while handleDecode");
            throw e;
        }
    }


    @Override
    public String toString(){
            return "DynamicLookupProtobufSchemaSerDe{" +
                    "name = " + getDescFileName() + "}" ;
    }

}

