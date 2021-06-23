package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DynamicLookupProtobufSchemaSerDe implements DynamicLookupCoreSchema<String,Optional<Long>> {
    private static final Logger LOG = new Logger(DynamicLookupProtobufSchemaSerDe.class);

    private final JsonNode coreSchema;
    private Descriptors.Descriptor protobufMessageDescriptor;


    public DynamicLookupProtobufSchemaSerDe(JsonNode coreSchema) throws IOException, Descriptors.DescriptorValidationException {
        this.coreSchema = coreSchema;
        DescriptorProtos.FileDescriptorSet set = null;
        FileInputStream fin = null;
        String path ="";

        try {
            path = getSchema();
            fin = new FileInputStream(path);
            set = DescriptorProtos.FileDescriptorSet.parseFrom(fin);
            protobufMessageDescriptor = Descriptors.FileDescriptor.buildFrom(set.getFile(0), new Descriptors.FileDescriptor[]{}).getMessageTypes().get(0);
        }catch (Exception ex){
            LOG.error("failed to read the binary coreSchema as protobuf Message or build FileDescriptor at path " + path  + ex);
            throw ex;
        } finally{
            if(fin != null){
                try {
                    fin.close();
                } catch(Exception ex){
                    LOG.error("Failed closing FileInputStream for path " + path  + ex);
                }
            }
        }
    }


    public SCHEMA_TYPE getSchemaType(){
        return SCHEMA_TYPE.PROTOBUF;
    }


    public String getSchema() throws IllegalArgumentException{
        if(coreSchema != null && coreSchema.has("descFilePath")){
            return coreSchema.get("descFilePath").textValue();
        } else {
            throw new IllegalArgumentException("Field coreSchema not present in schema file ");
        }
    }

    public ImmutablePair<String, Optional<Long>> getValue(String fieldName, byte[] dataBytes, RocksDBExtractionNamespace extractionNamespace){
        DynamicMessage genericMessage = null;
        try {
            genericMessage = DynamicMessage.parseFrom(protobufMessageDescriptor, dataBytes);
        } catch (InvalidProtocolBufferException e) {
            LOG.error("failed to parse as generic protobuf Message " + dataBytes + e);
        }
        Descriptors.FieldDescriptor fieldDescriptor;
        String fieldValue = "";
        Optional<Long> lastUpdated = Optional.of(0L);
        try{
            fieldDescriptor =  protobufMessageDescriptor.findFieldByName(fieldName);
            if(genericMessage.hasField(fieldDescriptor)) {
                fieldValue = (String) genericMessage.getField(fieldDescriptor);
            } else {
                LOG.error("Field missing in protobuf Message Descriptor for field: "
                        + fieldName +   " in " + extractionNamespace.getLookupName());
            }
            Descriptors.FieldDescriptor lastUpdatedDescriptor =  protobufMessageDescriptor.findFieldByName(extractionNamespace.getTsColumn());

            if(genericMessage.hasField(lastUpdatedDescriptor)){
                lastUpdated = Optional.of(Long.valueOf(genericMessage.getField(lastUpdatedDescriptor).toString()));
            }else{
                LOG.error("Last updated field missing in protobuf Message Descriptor for field: "
                        + fieldName +  " and last updated column : " + extractionNamespace.getTsColumn() + " in " + extractionNamespace.getLookupName() );

            }
        } catch (Exception ex){
            LOG.error("Caught error while getValue from protobufMessageDescriptor"  + ex);
            throw ex;
        }

        return new ImmutablePair<>(fieldValue,lastUpdated);
    }

    @Override
    public String toString(){
            return "DynamicLookupProtobufSchemaSerDe{" +
                    "name = " + getSchema() + "}" ;
    }

}

