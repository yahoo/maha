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

// Copyright 2022, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.

package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.yahoo.maha.maha_druid_lookups.query.lookup.JDBCLookupExtractor;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.*;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.query.lookup.*;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import org.joda.time.Period;
import java.util.*;

public class MahaLookupExprMacroTest {


    private static final Expr.ObjectBinding BINDINGS = InputBindings.withMap(
            ImmutableMap.<String, Object>builder()
                    .put("id1", "dim_key1")
                    .build()
    );

    private static Map<String, List> lookupCache = ImmutableMap.of(
            "dim_key1", Arrays.asList("dim_key1", "dim_val1")
    );

    private static final String TEST_LOOKUP = "test_maha_lookup";

    static LookupExtractorFactoryContainerProvider manager;
    static ExprMacroTable macroTable;

    @BeforeClass
    public static void setUpClass()
    {
        ExpressionProcessing.initializeForTests(false);
        NullHandling.initializeForTests();
        ExpressionProcessing.initializeForStrictBooleansTests(false);


        //Construct LookupExtractor
        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(
                        metadataStorageConnectorConfig, "test", new ArrayList<>(Arrays.asList("dim_key_column", "dim_val_column")),
                        "dim_key_column", "", new Period(), true, TEST_LOOKUP);
        LookupService lookupService = EasyMock.createStrictMock(LookupService.class);
        JDBCLookupExtractor LOOKUP_EXTRACTOR = new JDBCLookupExtractor(extractionNamespace, lookupCache, lookupService);

        //Construct conatiner
        LookupExtractorFactoryContainer container = new LookupExtractorFactoryContainer("v0", new LookupExtractorFactory()
        {
            @Override
            public boolean start()
            {
                return false;
            }

            @Override
            public boolean replaces(@Nullable LookupExtractorFactory other)
            {
                return false;
            }

            @Override
            public boolean close()
            {
                return false;
            }

            @Nullable
            @Override
            public LookupIntrospectHandler getIntrospectHandler()
            {
                return null;
            }

            @Override
            public LookupExtractor get()
            {
                return LOOKUP_EXTRACTOR;
            }
        }
        );

        //Construct LookupReferencesManager
        manager = EasyMock.createStrictMock(LookupReferencesManager.class);
        EasyMock.expect(manager.get(EasyMock.eq(TEST_LOOKUP))).andReturn(Optional.of(container)).anyTimes();
        EasyMock.replay(manager);

        macroTable = new ExprMacroTable(
                Lists.newArrayList(
                        Collections.singletonList(
                                new MahaLookupExprMacro(manager)
                        )
                )
        );
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testLookup()
    {
        assertExpr("maha_lookup(id1, 'test_maha_lookup', 'dim_val_column')", "dim_val1");
        assertExpr("maha_lookup(id1, 'test_maha_lookup', 'dim_val_column', 'NA')", "dim_val1");
        assertExpr("maha_lookup(id1, 'test_maha_lookup', 'dim_val_column', 'NA', 'dim_val1->UNKNOWN')", "UNKNOWN");
        assertExpr("maha_lookup(id1, 'test_maha_lookup', 'dim_val_column', 'NA', null, 'dim_key1->UNKNOWN')", "UNKNOWN");
    }

    private void assertExpr(final String expression, final Object expectedResult)
    {
        final Expr expr = Parser.parse(expression, macroTable);
        expr.eval(BINDINGS);
        Assert.assertEquals(expression, expectedResult, expr.eval(BINDINGS).value());

        final Expr exprNotFlattened = Parser.parse(expression, macroTable, false);
        String str = exprNotFlattened.stringify();
        final Expr roundTripNotFlattened =
                Parser.parse(exprNotFlattened.stringify(), macroTable);
        Assert.assertEquals(exprNotFlattened.stringify(), expectedResult, roundTripNotFlattened.eval(BINDINGS).value());

        final Expr roundTrip = Parser.parse(expr.stringify(), macroTable);
        Assert.assertEquals(exprNotFlattened.stringify(), expectedResult, roundTrip.eval(BINDINGS).value());
    }
}
