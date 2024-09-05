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

import com.google.common.base.Splitter;
import com.google.inject.*;
import com.yahoo.maha.maha_druid_lookups.query.lookup.*;
import com.yahoo.maha.maha_druid_lookups.query.lookup.util.LookupUtil;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.*;
import org.apache.druid.java.util.common.*;
import org.apache.druid.java.util.common.logger.*;
import org.apache.druid.math.expr.*;
import org.apache.druid.query.aggregation.*;
import org.apache.druid.query.filter.*;
import org.apache.druid.query.lookup.*;
import org.apache.druid.segment.column.*;
import org.apache.druid.sql.calcite.expression.*;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.*;

import javax.annotation.*;
import java.util.*;

public class MahaLookupOperatorConversion implements SqlOperatorConversion {

    private static final String DRUID_FUNC_NAME = "MAHA_LOOKUP";
    private static final String MISSING_VALUE = "NA";
    private static final String SEPARATOR = ",";
    private static final String KV_DEFAULT = "->";
    private static final Logger LOG = new Logger(MahaLookupOperatorConversion.class);
    private static final LookupUtil util = new LookupUtil();
    private static final List<String> REPL_LIST = Arrays.asList("", null);

    private static final SqlFunction SQL_FUNCTION = OperatorConversions
            .operatorBuilder(DRUID_FUNC_NAME)
            .operandTypes(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)
            .returnTypeNullable(SqlTypeName.VARCHAR)
            .requiredOperands(3)
            .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
            .build();

    private final LookupExtractorFactoryContainerProvider lookupReferencesManager;

    @Inject
    public MahaLookupOperatorConversion(LookupExtractorFactoryContainerProvider lookupProvider) {
        lookupReferencesManager = lookupProvider;
    }

    @Override
    public SqlOperator calciteOperator()
    {
        return SQL_FUNCTION;
    }

    @Nullable
    @Override
    public DruidExpression toDruidExpression(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode) {

        DruidExpression simpleExtraction = OperatorConversions.convertCall(
                plannerContext,
                rowSignature,
                rexNode,
                StringUtils.toLowerCase(calciteOperator().getName()),
                inputExpressions -> {
                    final DruidExpression arg = inputExpressions.get(0);

                    final Expr lookupName = plannerContext.parseExpression(inputExpressions.get(1).getExpression());
                    final Expr columnName = plannerContext.parseExpression(inputExpressions.get(2).getExpression());

                    LookupReferencesManager lrm = (LookupReferencesManager) lookupReferencesManager;
                    String missingValue = getMissingValue(inputExpressions, plannerContext, 3, MISSING_VALUE);
                    //TODO: Enhance by passing in KV separator & delimeter.
                    //Also, allow passing of Map type statements: Case, KV pair, etc. ex: CASE, MAP(',','->')
                    Map<String, String> dimensionOverrideMap = getMapOrDefault(inputExpressions, 5, plannerContext); //keyMap
                    Map<String, String> secondaryColOverrideMap = getMapOrDefault(inputExpressions, 4, plannerContext); //valueMap

                    if ( arg.isSimpleExtraction() && lookupName.isLiteral() && columnName.isLiteral() ) {
                        MahaRegisteredLookupExtractionFn mahaRegisteredLookupExtractionFn = new MahaRegisteredLookupExtractionFn(lrm,
                                (String) lookupName.getLiteralValue(),
                                false,
                                missingValue,
                                false,
                                false,
                                (String) columnName.getLiteralValue(),
                                null,
                                dimensionOverrideMap,
                                secondaryColOverrideMap,
                                false);

                        LOG.debug("valid call: lookupName = "+lookupName+", columnName = "+columnName+", arg = "+arg);
                        return arg.getSimpleExtraction().cascade(mahaRegisteredLookupExtractionFn);
                    } else {
                        LOG.error("Invalid call: lookupName = "+lookupName+", columnName = "+columnName+", arg = "+arg);
                        return null;
                    }
                }
        );
        if(simpleExtraction == null) return null;
        LOG.debug(String.format("Maha Lookup SimpleExtraction: %s", simpleExtraction.toString()));
        return DruidExpression.of(simpleExtraction.isSimpleExtraction()? simpleExtraction.getSimpleExtraction(): null, simpleExtraction.getExpression());
    }

    private Map<String, String> getMapOrDefault(List<DruidExpression> inputExpressions, int index, PlannerContext plannerContext) {
        String map = getMissingValue(inputExpressions, plannerContext, index, "");
        HashMap<String, String> reqMap = map == null || map.isEmpty() ? null : new HashMap<>(Splitter.on(SEPARATOR).withKeyValueSeparator(KV_DEFAULT).split(map));
        reqMap = mapCase(reqMap);

        return reqMap;
    }

    private HashMap<String, String> fixKeys(HashMap<String, String> input, String keyToFix) {
        String mod = input.remove(keyToFix);
        input.put(util.NULL_VAL, mod);
        return input;
    }

    private HashMap<String, String> mapCase(HashMap<String, String> input) {
        if(input == null)
            return input;
        for(String item: REPL_LIST){
            if(input.containsKey(item)) {
                input = fixKeys(input, item);
            }
        }

        return input;
    }

    private String getMissingValue(List<DruidExpression> list, PlannerContext plannerContext, int index, String valueIfMissing) {
        if (list==null) {
            return valueIfMissing;
        }
        if (list.size() >= index+1) {
            DruidExpression expression = list.get(index);
            if (expression != null) {
                Expr expr = plannerContext.parseExpression(expression.getExpression());
                if(expr != null) {
                    return (String) expr.getLiteralValue();
                }
            }
        }
        return valueIfMissing;
    }

    @Nullable
    @Override
    public DimFilter toDruidFilter(PlannerContext plannerContext, RowSignature rowSignature, @Nullable VirtualColumnRegistry virtualColumnRegistry, RexNode rexNode) {
        return null;
    }

    @Nullable
    @Override
    public PostAggregator toPostAggregator(PlannerContext plannerContext, RowSignature querySignature, RexNode rexNode, PostAggregatorVisitor postAggregatorVisitor) {
        return null;
    }
}
