package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.google.inject.*;
import com.yahoo.maha.maha_druid_lookups.query.lookup.*;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache.*;
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
    private static final Logger LOG = new Logger(MahaLookupOperatorConversion.class);

    private static final SqlFunction SQL_FUNCTION = OperatorConversions
            .operatorBuilder(DRUID_FUNC_NAME)
            .operandTypes(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)
            .returnTypeNullable(SqlTypeName.VARCHAR)
            .requiredOperands(3)
            .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
            .build();

    private final LookupExtractorFactoryContainerProvider lookupReferencesManager;

    OnHeapMahaNamespaceExtractionCacheManager onHeapMahaNamespaceExtractionCacheManager;

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
                    final DruidExpression arg = inputExpressions.get(0); // maha lookup function
                    final Expr lookupName = inputExpressions.get(1).parse(plannerContext.getExprMacroTable()); // maha lookup name
                    final Expr columnName = inputExpressions.get(2).parse(plannerContext.getExprMacroTable()); // maha lookup name

                    LookupReferencesManager lrm = (LookupReferencesManager) lookupReferencesManager;
                    String missingValue = getMissingValue(inputExpressions, plannerContext);

                    if (arg.isSimpleExtraction() && lookupName.isLiteral() && columnName.isLiteral()) {
                        MahaRegisteredLookupExtractionFn mahaRegisteredLookupExtractionFn = new MahaRegisteredLookupExtractionFn(lrm,
                                (String) lookupName.getLiteralValue(),
                                false,
                                missingValue,
                                false,
                                false,
                                (String) columnName.getLiteralValue(),
                                null,
                                null,
                                false);

                        return arg.getSimpleExtraction().cascade(mahaRegisteredLookupExtractionFn);
                    } else {
                        LOG.error("Invalid call to Maha_lookup: lookupName = "+lookupName+", columnName = "+columnName+", "+arg);
                        return null;
                    }
                }
        );
        if(simpleExtraction == null) return null;
       return DruidExpression.of(simpleExtraction.getSimpleExtraction(), "maha");
    }

    private String getMissingValue(List<DruidExpression> list, PlannerContext plannerContext) {
        if (list==null) {
            return MISSING_VALUE;
        }
        if (list.size() >= 4) {
            DruidExpression expression = list.get(3);
            if (expression != null) {
                return (String) expression.parse(plannerContext.getExprMacroTable()).getLiteralValue();
            }
        }
        return MISSING_VALUE;
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
