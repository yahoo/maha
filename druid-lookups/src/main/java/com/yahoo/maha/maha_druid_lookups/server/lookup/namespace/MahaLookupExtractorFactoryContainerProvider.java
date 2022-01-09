package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.google.inject.*;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache.*;
import org.apache.druid.query.lookup.*;

import java.util.*;
import java.util.stream.*;

public class MahaLookupExtractorFactoryContainerProvider implements LookupExtractorFactoryContainerProvider {

    private final MahaNamespaceExtractionCacheManager mahaNamespaceExtractionCacheManager;

    @Inject
    public MahaLookupExtractorFactoryContainerProvider(MahaNamespaceExtractionCacheManager mahaNamespaceExtractionCacheManager) {
        this.mahaNamespaceExtractionCacheManager = mahaNamespaceExtractionCacheManager;
    }

    @Override
    public Set<String> getAllLookupNames() {
       return (Set<String>) mahaNamespaceExtractionCacheManager.getKnownIDs().stream().collect(Collectors.toSet());
    }

    @Override
    public Optional<LookupExtractorFactoryContainer> get(String s) {
/*
       LookupExtractor lookupExtractor =  mahaNamespaceExtractionCacheManager.getLookupExtractor(s);
       mahaNamespaceExtractionCacheManager.
       mahaNamespaceExtractionCacheManager.getExtractionNamespace(s);
       String version = mahaNamespaceExtractionCacheManager.getVersion(lookupExtractor)
        return Optional.of(new LookupExtractorFactoryContainer());
*/
        return Optional.empty();
    }
}
