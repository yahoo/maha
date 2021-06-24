package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema;

import com.yahoo.maha.maha_druid_lookups.query.lookup.*;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.*;

import java.util.*;

public interface DynamicLookupCoreSchema {
    ExtractionNameSpaceSchemaType getSchemaType();
    String getValue(String fieldName, byte[] dataBytes, Optional<DecodeConfig> decodeConfigOptional, RocksDBExtractionNamespace extractionNamespace);
}
