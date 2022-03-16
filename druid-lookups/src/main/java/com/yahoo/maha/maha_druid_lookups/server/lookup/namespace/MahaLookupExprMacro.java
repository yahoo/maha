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
        if (args.size() < 4) {
            throw new IAE("Function[%s] must have at least 4 arguments", name());
        }

        final Expr arg = args.get(0);
        final Expr lookupExpr = args.get(1);
        final Expr columnExpr = args.get(2);
        String columnStr = (String) columnExpr.getLiteralValue();
        final Expr missingValueExpr = args.get(3);
        String missingValueStr = (String) missingValueExpr.getLiteralValue();
        final Expr secondaryColOverrideMapExpr = args.size() >= 5? args.get(4): null;
        final String secondaryColOverrideMapStr = secondaryColOverrideMapExpr != null ? (String) secondaryColOverrideMapExpr.getLiteralValue(): null;
        final Expr dimensionOverrideMapStrExpr = args.size() >= 6? args.get(5): null;
        final String dimensionOverrideMapStr = dimensionOverrideMapStrExpr != null ? (String) dimensionOverrideMapStrExpr.getLiteralValue(): null;

        //valueMap
        Map<String, String> secondaryColOverrideMap = secondaryColOverrideMapStr!= null && !secondaryColOverrideMapStr.isEmpty() ?
                new HashMap<>(Splitter.on(SEPARATOR).withKeyValueSeparator(KV_DEFAULT).split(secondaryColOverrideMapStr)) :
                null;

        //keyMap
        Map<String, String> dimensionOverrideMap = dimensionOverrideMapStr != null && !dimensionOverrideMapStr.isEmpty() ?
                new HashMap<>(Splitter.on(SEPARATOR).withKeyValueSeparator(KV_DEFAULT).split(dimensionOverrideMapStr)) :
                null;

        if (!lookupExpr.isLiteral() || lookupExpr.getLiteralValue() == null) {
            throw new IAE("Function[%s] second argument must be a registered lookup name", name());
        }

        final String lookupName = lookupExpr.getLiteralValue().toString();

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
                sb.append(StringUtils.format("%s, %s, %s, %s", arg.stringify(), lookupExpr.stringify(), columnExpr.stringify(), missingValueExpr.stringify()));
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
}
