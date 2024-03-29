/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Module;
import com.yahoo.maha.maha_druid_lookups.query.lookup.parser.CSVFlatDataParser;
import com.yahoo.maha.maha_druid_lookups.query.lookup.parser.FlatDataParser;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.GuiceAnnotationIntrospector;
import org.apache.druid.guice.GuiceInjectableValues;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * Note: these classes are testing Jackson version 2.9.9, and druid-server
 * 0.11.0 uses Jackson 2.4.*
 * Multiple bindings (ExtractionNamespace) are not supported before Jackson 2.6.*,
 * so whichever Binding is mentioned first is used by Druid.
 */
public class URIExtractionNamespaceTest {
    static {
        NullHandling.initializeForTests();
    }

    public static ObjectMapper registerTypes(
            final ObjectMapper mapper
    )
    {
        mapper.setInjectableValues(
                new GuiceInjectableValues(
                        Guice.createInjector(
                                ImmutableList.of(
                                        new Module()
                                        {
                                            @Override
                                            public void configure(Binder binder)
                                            {
                                                binder.bind(ObjectMapper.class).annotatedWith(Json.class).toInstance(mapper);
                                                binder.bind(ObjectMapper.class).toInstance(mapper);
                                            }
                                        }
                                )
                        )
                )
        ).registerSubtypes(URIExtractionNamespace.class, FlatDataParser.class);

        final GuiceAnnotationIntrospector guiceIntrospector = new GuiceAnnotationIntrospector();
        mapper.setAnnotationIntrospectors(
                new AnnotationIntrospectorPair(
                        guiceIntrospector,
                        mapper.getSerializationConfig().getAnnotationIntrospector()
                ),
                new AnnotationIntrospectorPair(
                        guiceIntrospector,
                        mapper.getDeserializationConfig().getAnnotationIntrospector()
                )
        );
        return mapper;
    }

    @Test
    public void testCSV()
    {
        CSVFlatDataParser parser = new CSVFlatDataParser(
                ImmutableList.of(
                        "col1",
                        "col2",
                        "col3"
                ), "col2"
        );
        Assert.assertEquals(ImmutableMap.of("B", Arrays.asList("A", "B", "C")), parser.getParser().parseToMap("A,B,C"));
    }
    @Test
    public void testCSVWithHeader()
    {
        CSVFlatDataParser parser = new CSVFlatDataParser(
                ImmutableList.of("col1", "col2", "col3"),
                "col2",
                true,
                1,
                "null"
        );
        // parser return empyt list as the 1 row header need to be skipped.
        Assert.assertEquals(ImmutableMap.of(), parser.getParser().parseToMap("row to skip "));
        //Header also need to be skipped.
        Assert.assertEquals(ImmutableMap.of(), parser.getParser().parseToMap("col1,col2,col3"));
        // test the header is parsed
        Assert.assertEquals(ImmutableList.of("col1", "col2", "col3"), parser.getParser().getFieldNames());
        // The third row will parse to data
        Assert.assertEquals(ImmutableMap.of("val2", Arrays.asList("val1","val2","val3")), parser.getParser().parseToMap("val1,val2,val3"));
    }


    @Test//(expected = NullPointerException.class)
    public void testNullKey()
    {
        CSVFlatDataParser parser = new CSVFlatDataParser(
                ImmutableList.of(
                        "col1",
                        "col2",
                        "col3"
                ), "col2", "nullval"
        );
        Map<String, List<String>> map = parser.getParser().parseToMap("A,,C");
        Assert.assertEquals(new HashMap<String, List<String>>(){{put("nullval", Arrays.asList("A",null,"C"));}}, map);
    }


    @Test(expected = JsonMappingException.class)
    public void testExplicitJsonException() throws IOException
    {
        final ObjectMapper mapper = registerTypes(new DefaultObjectMapper());
        mapper.readValue(
                "{\"type\":\"uri\", \"uri\":\"file:/foo\", \"namespaceParseSpec\":{\"format\":\"simpleJson\"}, \"pollPeriod\":\"PT5M\", \"versionRegex\":\"a.b.c\", \"namespace\":\"testNamespace\"}",
                URIExtractionNamespace.class
        );
    }



}
