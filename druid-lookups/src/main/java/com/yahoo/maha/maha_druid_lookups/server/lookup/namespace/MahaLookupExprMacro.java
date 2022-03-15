package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

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
import org.apache.druid.query.lookup.RegisteredLookupExtractionFn;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public class MahaLookupExprMacro implements ExprMacroTable.ExprMacro
{
    private static final Logger LOG = new Logger(MahaLookupExprMacro.class);
    private static final String FN_NAME = "maha_lookup";
    private final LookupExtractorFactoryContainerProvider lookupExtractorFactoryContainerProvider;


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


        if (args.size() != 4) {
            throw new IAE("Function[%s] must have 2 arguments", name());
        }

        final Expr arg = args.get(0);
        final Expr lookupExpr = args.get(1);
        final Expr columnExpr = args.get(2);
        String columnStr = (String) columnExpr.getLiteralValue();
        final Expr missingValueExpr = args.get(3);
        String missingValueStr = (String) missingValueExpr.getLiteralValue();

        if (!lookupExpr.isLiteral() || lookupExpr.getLiteralValue() == null) {
            throw new IAE("Function[%s] second argument must be a registered lookup name", name());
        }

        final String lookupName = lookupExpr.getLiteralValue().toString();
//        final RegisteredLookupExtractionFn extractionFn = new RegisteredLookupExtractionFn(
//                lookupExtractorFactoryContainerProvider,
//                lookupName,
//                false,
//                null,
//                false,
//                null
//        );


        LookupReferencesManager lrm = (LookupReferencesManager) lookupExtractorFactoryContainerProvider;
        MahaRegisteredLookupExtractionFn mahaRegisteredLookupExtractionFn = new MahaRegisteredLookupExtractionFn(lrm,
                lookupName,
                false,
                missingValueStr,
                false,
                false,
                columnStr,
                null,
                null,
                null,
                false);
        LOG.error("Macro: valid call to Maha_lookup: lookupName = " + lookupName + ", columnName = " + columnStr + ", arg = " + arg);

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
                return StringUtils.format("%s(%s, %s, %s, %s)", FN_NAME, arg.stringify(), lookupExpr.stringify(), columnStr);
            }
        }

        return new MahaLookupExpr(arg);
    }
}
