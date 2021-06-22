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
    private Map<String,String> schemaPathAsMap ;
    private Descriptors.Descriptor protobufMessageDescriptor;

    public DynamicLookupProtobufSchemaSerDe(JsonNode coreSchema) throws IOException, Descriptors.DescriptorValidationException {
        this.coreSchema = coreSchema;
        schemaPathAsMap =  new HashMap<String, String>();
        DescriptorProtos.FileDescriptorSet set = null;
        String path ="";

        try {
            path = getSchema();
            FileInputStream fin = new FileInputStream(path);
            set = DescriptorProtos.FileDescriptorSet.parseFrom(fin);
            protobufMessageDescriptor = Descriptors.FileDescriptor.buildFrom(set.getFile(0), new Descriptors.FileDescriptor[]{}).getMessageTypes().get(0);
        }catch (IOException ex){
            LOG.error("failed to read the binary coreSchema as protobuf Message at path " + path  + ex);
            throw ex;
        }catch(Descriptors.DescriptorValidationException ex){
            LOG.error("failed to build FileDescriptor from path " + path  + ex);
            throw ex;
        }
    }

    public SCHEMA_TYPE getSchemaType(){
        return SCHEMA_TYPE.PROTOBUF;
    }


    public String getSchema() throws IllegalArgumentException{ //does this method need to to exposed?
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
        Descriptors.FieldDescriptor fieldDescriptor =  protobufMessageDescriptor.findFieldByName(fieldName);
        String fieldValue = (String) genericMessage.getField(fieldDescriptor);
        Descriptors.FieldDescriptor lastUpdatedDescriptor =  protobufMessageDescriptor.findFieldByName(extractionNamespace.getTsColumn());
        Long lastUpdated = Long.valueOf(genericMessage.getField(lastUpdatedDescriptor).toString());
        return new ImmutablePair<>(fieldValue,Optional.of(lastUpdated));
    }

    @Override
    public String toString(){
        if(!schemaPathAsMap.isEmpty() && schemaPathAsMap.containsKey("descFilePath")){
            return "DynamicLookupProtobufSchemaSerDe{" +
                    "name = " + schemaPathAsMap.get("descFilePath") + "}" ;
        }
        return "DynamicLookupProtobufSchemaSerDe{ + name =  }";
    }

}

