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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.lookup.*;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import org.easymock.EasyMock;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class MahaLookupOperatorConversionTest {

    @BeforeTest
    public void setUp(){
        NullHandling.initializeForTests();
    }

    @AfterTest
    public void shutDown() {

    }

    @Test
    public void testLookupReturnsExpectedResults() throws JsonProcessingException {
        MahaLookupTestUtil util = new MahaLookupTestUtil();
        RexBuilder rexBuilder = new RexBuilder(util.typeFactory);
        RowSignature ROW_SIGNATURE = RowSignature
                .builder()
                .add("d", ColumnType.DOUBLE)
                .add("l", ColumnType.LONG)
                .add("s", ColumnType.STRING)
                .add("student_id", ColumnType.STRING)
                .build();

        final LookupExtractorFactoryContainerProvider manager = EasyMock.createStrictMock(LookupReferencesManager.class);

        MahaLookupOperatorConversion opConversion = new MahaLookupOperatorConversion(manager);
        ExprMacroTable exprMacroTable = TestExprMacroTable.INSTANCE;

        PlannerContext plannerContext = EasyMock.createStrictMock(PlannerContext.class);
        EasyMock.expect(plannerContext.getExprMacroTable()).andReturn(exprMacroTable).anyTimes();
        EasyMock.replay(plannerContext);
        MahaLookupTestUtil.MAHA_LOOKUP mahaLookup = new MahaLookupTestUtil.MAHA_LOOKUP(
                util.makeInputRef("student_id", ROW_SIGNATURE, rexBuilder)
                , Pair.of("student_lookup", SqlTypeName.VARCHAR)
                , Pair.of("student_id", SqlTypeName.VARCHAR)
                , Pair.of("123", SqlTypeName.VARCHAR)
                , rexBuilder
        );

        RexNode rn2 = mahaLookup.makeCall(rexBuilder, opConversion.calciteOperator());

        DruidExpression druidExp = opConversion.toDruidExpression(plannerContext, ROW_SIGNATURE, rn2);
        assert druidExp != null;

        String expectedDruidExpr = "DruidExpression{simpleExtraction=MahaRegisteredLookupExtractionFn{delegate=null, lookup='student_lookup', retainMissingValue=false, replaceMissingValueWith='123', injective=false, optimize=false, valueColumn=student_id, decodeConfig=null, useQueryLevelCache=false}(student_id), expression='maha_lookup(\"student_id\",'student_lookup','student_id','123')'}";
        String json = util.convertToJson(druidExp, "testing_stats", "Student ID");
        assert druidExp.toString().equals(expectedDruidExpr);
        assert json.contains("\"dimensions\":[{\"type\":\"extraction\",\"dimension\":\"student_id\",\"outputName\":\"Student ID\",\"outputType\":\"STRING\",\"extractionFn\":{\"type\":\"mahaRegisteredLookup\",\"lookup\":\"student_lookup\",\"retainMissingValue\":false,\"replaceMissingValueWith\":\"123\",\"injective\":false,\"optimize\":false,\"valueColumn\":\"student_id\",\"decode\":null,\"dimensionOverrideMap\":null,\"secondaryColOverrideMap\":null,\"useQueryLevelCache\":false}}]");
    }

    @Test
    public void testBasicMappedLookup() throws JsonProcessingException {
        MahaLookupTestUtil util = new MahaLookupTestUtil();
        RexBuilder rexBuilder = new RexBuilder(util.typeFactory);
        RowSignature ROW_SIGNATURE = RowSignature
                .builder()
                .add("d", ColumnType.DOUBLE)
                .add("l", ColumnType.LONG)
                .add("grade", ColumnType.STRING)
                .add("student_id", ColumnType.STRING)
                .build();

        final LookupExtractorFactoryContainerProvider manager = EasyMock.createStrictMock(LookupReferencesManager.class);

        MahaLookupOperatorConversion opConversion = new MahaLookupOperatorConversion(manager);
        ExprMacroTable exprMacroTable = TestExprMacroTable.INSTANCE;

        PlannerContext plannerContext = EasyMock.createStrictMock(PlannerContext.class);
        EasyMock.expect(plannerContext.getExprMacroTable()).andReturn(exprMacroTable).anyTimes();
        EasyMock.replay(plannerContext);
        MahaLookupTestUtil.MAHA_LOOKUP mahaLookup = new MahaLookupTestUtil.MAHA_LOOKUP(
                util.makeInputRef("student_id", ROW_SIGNATURE, rexBuilder)
                , Pair.of("student_lookup", SqlTypeName.VARCHAR)
                , Pair.of("grade", SqlTypeName.VARCHAR)
                , Pair.of("A+", SqlTypeName.VARCHAR)
                , Pair.of("a->A,b->B", SqlTypeName.VARCHAR)
                , Pair.of("a->A,b->B", SqlTypeName.VARCHAR)
                , rexBuilder
        );

        RexNode rn2 = mahaLookup.makeCall(rexBuilder, opConversion.calciteOperator());

        DruidExpression druidExp = opConversion.toDruidExpression(plannerContext, ROW_SIGNATURE, rn2);
        assert druidExp != null;

        String json = util.convertToJson(druidExp, "testing_stats", "Grade Avg");
        assert json.contains("{\"type\":\"extraction\",\"dimension\":\"student_id\",\"outputName\":\"Grade Avg\",\"outputType\":\"STRING\",");
        assert json.contains("\"extractionFn\":{\"type\":\"mahaRegisteredLookup\",\"lookup\":\"student_lookup\"");
        assert json.contains("\"replaceMissingValueWith\":\"A+\",\"injective\":false,\"optimize\":false,\"valueColumn\":\"grade\"");
        assert json.contains("\"dimensionOverrideMap\":{\"a\":\"A\",\"b\":\"B\"},\"secondaryColOverrideMap\":{\"a\":\"A\",\"b\":\"B\"},\"useQueryLevelCache\":false");
    }

    @Test
    public void testInvalidLookupCol() {
        MahaLookupTestUtil util = new MahaLookupTestUtil();
        RexBuilder rexBuilder = new RexBuilder(util.typeFactory);
        RowSignature ROW_SIGNATURE = RowSignature
                .builder()
                .add("d", ColumnType.DOUBLE)
                .add("l", ColumnType.LONG)
                .add("grade", ColumnType.STRING)
                .add("student_id", ColumnType.STRING)
                .build();

        final LookupExtractorFactoryContainerProvider manager = EasyMock.createStrictMock(LookupReferencesManager.class);

        MahaLookupOperatorConversion opConversion = new MahaLookupOperatorConversion(manager);
        ExprMacroTable exprMacroTable = TestExprMacroTable.INSTANCE;

        PlannerContext plannerContext = EasyMock.createStrictMock(PlannerContext.class);
        EasyMock.expect(plannerContext.getExprMacroTable()).andReturn(exprMacroTable).anyTimes();
        EasyMock.replay(plannerContext);
        try {
            MahaLookupTestUtil.MAHA_LOOKUP mahaLookup = new MahaLookupTestUtil.MAHA_LOOKUP(
                    util.makeInputRef("student_id_fake", ROW_SIGNATURE, rexBuilder)
                    , Pair.of("student_lookup", SqlTypeName.VARCHAR)
                    , Pair.of("grade", SqlTypeName.VARCHAR)
                    , Pair.of("A+", SqlTypeName.VARCHAR)
                    , Pair.of("a->A,b->B", SqlTypeName.VARCHAR)
                    , Pair.of("a->A,b->B", SqlTypeName.VARCHAR)
                    , rexBuilder
            );
        } catch(IndexOutOfBoundsException ex) {
            assert ex.getMessage().contains("index (-1) must not be negative");
        }
    }

    //Currently, we don't assert on the value col used in the lookup & simply allow bad lookup cols to pass through.
    @Test
    public void testInvalidValueCol() throws JsonProcessingException {
        MahaLookupTestUtil util = new MahaLookupTestUtil();
        RexBuilder rexBuilder = new RexBuilder(util.typeFactory);
        RowSignature ROW_SIGNATURE = RowSignature
                .builder()
                .add("d", ColumnType.DOUBLE)
                .add("l", ColumnType.LONG)
                .add("grade", ColumnType.STRING)
                .add("student_id", ColumnType.STRING)
                .build();

        final LookupExtractorFactoryContainerProvider manager = EasyMock.createStrictMock(LookupReferencesManager.class);

        MahaLookupOperatorConversion opConversion = new MahaLookupOperatorConversion(manager);
        ExprMacroTable exprMacroTable = TestExprMacroTable.INSTANCE;

        PlannerContext plannerContext = EasyMock.createStrictMock(PlannerContext.class);
        EasyMock.expect(plannerContext.getExprMacroTable()).andReturn(exprMacroTable).anyTimes();
        EasyMock.replay(plannerContext);
        MahaLookupTestUtil.MAHA_LOOKUP mahaLookup = new MahaLookupTestUtil.MAHA_LOOKUP(
                util.makeInputRef("student_id", ROW_SIGNATURE, rexBuilder)
                , Pair.of("student_lookup", SqlTypeName.VARCHAR)
                , Pair.of("grade_fake", SqlTypeName.VARCHAR)
                , Pair.of("A+", SqlTypeName.VARCHAR)
                , Pair.of("a->A,b->B", SqlTypeName.VARCHAR)
                , Pair.of("a->A,b->B", SqlTypeName.VARCHAR)
                , rexBuilder
        );

        RexNode rn2 = mahaLookup.makeCall(rexBuilder, opConversion.calciteOperator());

        DruidExpression druidExp = opConversion.toDruidExpression(plannerContext, ROW_SIGNATURE, rn2);
        assert druidExp != null;

        String json = util.convertToJson(druidExp, "testing_stats", "Grade Avg");
        assert json.contains("{\"type\":\"extraction\",\"dimension\":\"student_id\",\"outputName\":\"Grade Avg\",\"outputType\":\"STRING\",");
        assert json.contains("\"extractionFn\":{\"type\":\"mahaRegisteredLookup\",\"lookup\":\"student_lookup\"");
        assert json.contains("\"replaceMissingValueWith\":\"A+\",\"injective\":false,\"optimize\":false,\"valueColumn\":\"grade_fake\"");
        assert json.contains("\"dimensionOverrideMap\":{\"a\":\"A\",\"b\":\"B\"},\"secondaryColOverrideMap\":{\"a\":\"A\",\"b\":\"B\"},\"useQueryLevelCache\":false");
        

    }

    @Test
    public void testMappedLookupWithNullKeys() throws JsonProcessingException {
        MahaLookupTestUtil util = new MahaLookupTestUtil();
        RexBuilder rexBuilder = new RexBuilder(util.typeFactory);
        RowSignature ROW_SIGNATURE = RowSignature
                .builder()
                .add("d", ColumnType.DOUBLE)
                .add("l", ColumnType.LONG)
                .add("grade", ColumnType.STRING)
                .add("student_id", ColumnType.STRING)
                .build();

        final LookupExtractorFactoryContainerProvider manager = EasyMock.createStrictMock(LookupReferencesManager.class);

        MahaLookupOperatorConversion opConversion = new MahaLookupOperatorConversion(manager);
        ExprMacroTable exprMacroTable = TestExprMacroTable.INSTANCE;

        PlannerContext plannerContext = EasyMock.createStrictMock(PlannerContext.class);
        EasyMock.expect(plannerContext.getExprMacroTable()).andReturn(exprMacroTable).anyTimes();
        EasyMock.replay(plannerContext);
        MahaLookupTestUtil.MAHA_LOOKUP mahaLookup = new MahaLookupTestUtil.MAHA_LOOKUP(
                util.makeInputRef("student_id", ROW_SIGNATURE, rexBuilder)
                , Pair.of("student_lookup", SqlTypeName.VARCHAR)
                , Pair.of("grade", SqlTypeName.VARCHAR)
                , Pair.of("A+", SqlTypeName.VARCHAR)
                , Pair.of("->A,b->B", SqlTypeName.VARCHAR)
                , Pair.of("a->A,->B", SqlTypeName.VARCHAR)
                , rexBuilder
        );

        RexNode rn2 = mahaLookup.makeCall(rexBuilder, opConversion.calciteOperator());

        DruidExpression druidExp = opConversion.toDruidExpression(plannerContext, ROW_SIGNATURE, rn2);
        assert druidExp != null;

        String json = util.convertToJson(druidExp, "testing_stats", "Grade Avg");
        System.err.println(json);
        assert json.contains("{\"type\":\"extraction\",\"dimension\":\"student_id\",\"outputName\":\"Grade Avg\",\"outputType\":\"STRING\",");
        assert json.contains("\"extractionFn\":{\"type\":\"mahaRegisteredLookup\",\"lookup\":\"student_lookup\"");
        assert json.contains("\"replaceMissingValueWith\":\"A+\",\"injective\":false,\"optimize\":false,\"valueColumn\":\"grade\"");
        assert json.contains("\"dimensionOverrideMap\":{\"a\":\"A\",\"NULL\":\"B\"},\"secondaryColOverrideMap\":{\"b\":\"B\",\"NULL\":\"A\"},\"useQueryLevelCache\":false");
    }


}
