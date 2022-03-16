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
import com.google.inject.Inject;
import com.yahoo.maha.maha_druid_lookups.query.lookup.MahaRegisteredLookupExtractionFn;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.LookupReferencesManager;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MahaLookupExprMacro implements ExprMacroTable.ExprMacro
{
    private static final Logger LOG = new Logger(MahaLookupExprMacro.class);
    private static final String FN_NAME = "maha_lookup";
    private final LookupExtractorFactoryContainerProvider lookupExtractorFactoryContainerProvider;
    private static final String SEPARATOR = ",";
    private static final String KV_DEFAULT = "->";

    @Inject
    public MahaLookupExprMacro(final LookupExtractorFactoryContainerProvider lookupExtractorFactoryContainerProvider)
    {
        this.lookupExtractorFactoryContainerProvider = lookupExtractorFactoryContainerProvider;
    }

    @Override
    public String name()
    {
        return FN_NAME;
    }

    @Override
    public Expr apply(final List<Expr> args)
    {

        //Checks
        if (args.size() < 3) {
            throw new IAE("Function[%s] must have at least 3 arguments: usage: maha_lookup(arg, 'lookup_name', 'dimension_colunn')", name());
        }

        final Expr arg = args.get(0);
        final Expr lookupExpr = args.get(1);
        if (lookupExpr == null || !lookupExpr.isLiteral() || lookupExpr.getLiteralValue() == null) {
            throw new IAE("Function[%s] 2nd argument must be a registered lookup name", name());
        }
        final Expr columnExpr = args.get(2);
        if (columnExpr == null || !columnExpr.isLiteral() || columnExpr.getLiteralValue() == null) {
            throw new IAE("Function[%s] 3th argument must be a targeted dimension name", name());
        }
        final Expr missingValueExpr = getExprOrNull(args, 3);
        final Expr secondaryColOverrideMapExpr = getExprOrNull(args, 4);
        final Expr dimensionOverrideMapStrExpr = getExprOrNull(args, 5);

        //construct required strings and maps from Expr for mahaRegisteredLookupExtractionFn
        final String lookupName = convertExprToStringOrDefault(lookupExpr, "");
        final String columnStr = convertExprToStringOrDefault(columnExpr, "");
        final String missingValueStr = convertExprToStringOrDefault(missingValueExpr, "");
        final String secondaryColOverrideMapStr = convertExprToStringOrDefault(secondaryColOverrideMapExpr, "");
        final String dimensionOverrideMapStr = convertExprToStringOrDefault(dimensionOverrideMapStrExpr, "");

        //valueMap
        Map<String, String> secondaryColOverrideMap = secondaryColOverrideMapStr!= null && !secondaryColOverrideMapStr.isEmpty() ?
                new HashMap<>(Splitter.on(SEPARATOR).withKeyValueSeparator(KV_DEFAULT).split(secondaryColOverrideMapStr)) :
                null;

        //keyMap
        Map<String, String> dimensionOverrideMap = dimensionOverrideMapStr != null && !dimensionOverrideMapStr.isEmpty() ?
                new HashMap<>(Splitter.on(SEPARATOR).withKeyValueSeparator(KV_DEFAULT).split(dimensionOverrideMapStr)) :
                null;


        LookupReferencesManager lrm = (LookupReferencesManager) lookupExtractorFactoryContainerProvider;
        MahaRegisteredLookupExtractionFn mahaRegisteredLookupExtractionFn = new MahaRegisteredLookupExtractionFn(lrm,
                lookupName,
                false,
                missingValueStr,
                false,
                false,
                columnStr,
                null,
                dimensionOverrideMap,
                secondaryColOverrideMap,
                false);
        LOG.debug("MahaMacro: valid call: lookupName = " + lookupName + ", columnName = " + columnStr + ", arg = " + arg);

        class MahaLookupExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
        {
            private MahaLookupExpr(Expr arg)
            {
                super(FN_NAME, arg);
            }

            @Nonnull
            @Override
            public ExprEval eval(final ObjectBinding bindings)
            {
                return ExprEval.of(mahaRegisteredLookupExtractionFn.apply(NullHandling.emptyToNullIfNeeded(arg.eval(bindings).asString())));
            }

            @Override
            public Expr visit(Shuttle shuttle)
            {
                Expr newArg = arg.visit(shuttle);
                return shuttle.visit(new MahaLookupExpr(newArg));
            }

            @Nullable
            @Override
            public ExprType getOutputType(InputBindingInspector inspector)
            {
                return ExprType.STRING;
            }

            @Override
            public String stringify()
            {
                StringBuilder sb = new StringBuilder();
                sb.append(FN_NAME + "(");
                sb.append(StringUtils.format("%s, %s, %s", arg.stringify(), lookupExpr.stringify(), columnExpr.stringify()));

                if(args.size() >= 4) {
                    sb.append(", " + missingValueExpr.stringify());
                }

                if(args.size() >= 5) {
                    sb.append(", " + secondaryColOverrideMapExpr.stringify());
                }

                if(args.size() >= 6) {
                    sb.append(", " + dimensionOverrideMapStrExpr.stringify());
                }

                sb.append(")");
                return sb.toString();
            }
        }

        return new MahaLookupExpr(arg);
    }

    private Expr getExprOrNull(List<Expr> list, int index) {
        return list != null && list.size() >= index+1? list.get(index): null;
    }

    private String convertExprToStringOrDefault(Expr expr, String defaultValue) {
        return (expr != null)? (String) expr.getLiteralValue(): defaultValue;
    }
}
