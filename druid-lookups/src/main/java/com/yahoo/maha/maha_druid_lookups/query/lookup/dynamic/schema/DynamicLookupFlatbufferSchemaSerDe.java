package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema;


import com.yahoo.maha.maha_druid_lookups.query.lookup.*;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.*;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.*;
import org.apache.druid.java.util.common.logger.Logger;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class DynamicLookupFlatbufferSchemaSerDe implements  DynamicLookupCoreSchema {
    private static final Logger LOG = new Logger(DynamicLookupFlatbufferSchemaSerDe.class);

    private DynamicLookupSchema dynamicLookupSchema;
    private String fieldsCsv;


    public DynamicLookupFlatbufferSchemaSerDe(DynamicLookupSchema dynamicLookupSchema){
        this.dynamicLookupSchema = dynamicLookupSchema;
        fieldsCsv = dynamicLookupSchema.getSchemaFieldList().stream().map(s-> s.getField()).collect(Collectors.joining(", "));
    }

    public ExtractionNameSpaceSchemaType getSchemaType(){
        return ExtractionNameSpaceSchemaType.FLAT_BUFFER;
    }


    @Override
    public String getValue(String fieldName, byte[] dataBytes, Optional<DecodeConfig> decodeConfigOptional, RocksDBExtractionNamespace extractionNamespace) {
        // init Generic Table from DataBytes; GenericFlatBufferTable
        GenericFlatBufferTable genericFlatBufferTable = getDynamicFlatbufferTable(dataBytes);

        // handleDecode;
        if(decodeConfigOptional.isPresent()){
            return handleDecode(decodeConfigOptional.get(), genericFlatBufferTable, extractionNamespace);
        } else {
            return getValueForField(fieldName, genericFlatBufferTable, extractionNamespace);
        }
    }


    // get the index for field
    private Optional<Integer> getSchemaFieldIndex(String fieldName){
        List<SchemaField> fieldList = dynamicLookupSchema.getSchemaFieldList();
        Optional<SchemaField> schemaField = fieldList.stream().filter(e -> fieldName.equals(e.getField())).findFirst();
        if(schemaField.isPresent()) return Optional.of(schemaField.get().getIndex());

        return Optional.empty();
    }

    // getValue with fieldName;
    private String getValueForField(String fieldName , GenericFlatBufferTable genericFlatBufferTable, ExtractionNamespace extractionNamespace) {
        Optional<Integer> fieldIndex = getSchemaFieldIndex(fieldName);
        if (!fieldIndex.isPresent()) {
            LOG.error("Failed to find the field '%s' in schema: [%s], namespace: %s", fieldName, fieldsCsv, extractionNamespace.getLookupName());
            return "";
        }
        String fieldValue = genericFlatBufferTable.getValue(fieldIndex.get());
        return fieldValue == null ? "" : fieldValue;
    }


    public String handleDecode(DecodeConfig decodeConfig, GenericFlatBufferTable genericFlatBufferTable, ExtractionNamespace extractionNamespace) {
        try {
            String columnToCheck = getValueForField(decodeConfig.getColumnToCheck(), genericFlatBufferTable, extractionNamespace);

            if (decodeConfig.getValueToCheck() != null && decodeConfig.getValueToCheck() != "" && decodeConfig.getValueToCheck().equals(columnToCheck)) {
                return getValueForField(decodeConfig.getColumnIfValueMatched(), genericFlatBufferTable, extractionNamespace);
            } else {
                return getValueForField(decodeConfig.getColumnIfValueNotMatched(), genericFlatBufferTable, extractionNamespace);
            }
        } catch (Exception e) {
            LOG.error(e, "Caught exception while handleDecode " + e.getMessage());
            throw e;
        }
    }

    private GenericFlatBufferTable getDynamicFlatbufferTable(byte[] dataBytes) {
        return new GenericFlatBufferTable(ByteBuffer.wrap(dataBytes));
    }
}
