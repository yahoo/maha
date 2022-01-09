package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.google.inject.*;
import com.yahoo.maha.maha_druid_lookups.query.lookup.*;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache.*;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.druid.query.aggregation.*;
import org.apache.druid.query.aggregation.post.*;
import org.apache.druid.query.filter.*;
import org.apache.druid.query.lookup.*;
import org.apache.druid.segment.column.*;
import org.apache.druid.sql.calcite.expression.*;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.*;

import javax.annotation.*;

public class MahaLookupOperatorConversion implements SqlOperatorConversion {

    private static final String DRUID_FUNC_NAME = "MYFUNC";

    private static final SqlFunction SQL_FUNCTION = OperatorConversions
            .operatorBuilder(DRUID_FUNC_NAME)
            .operandTypes(SqlTypeFamily.STRING)
            .operandTypes(SqlTypeFamily.STRING)
            .operandTypes(SqlTypeFamily.ANY)
            .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
            .returnTypeInference(ReturnTypes.ARG0)
            .build();

    private final LookupExtractorFactoryContainerProvider lookupReferencesManager;

    OnHeapMahaNamespaceExtractionCacheManager onHeapMahaNamespaceExtractionCacheManager;

    @Inject
    public MahaLookupOperatorConversion(LookupExtractorFactoryContainerProvider lookupProvider) {
        lookupReferencesManager = lookupProvider;
        if (lookupReferencesManager!=null) {
            System.out.println("############ Lookups : ");
            //lookupReferencesManager.getAllLookupNames().stream().forEach(System.out::println);
        }
    }


    @Override
    public SqlOperator calciteOperator()
    {
        return SQL_FUNCTION;
    }

    @Nullable
    @Override
    public DruidExpression toDruidExpression(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode) {
        LookupReferencesManager lrm = (LookupReferencesManager) lookupReferencesManager;
        MahaRegisteredLookupExtractionFn mahaRegisteredLookupExtractionFn = new MahaRegisteredLookupExtractionFn(lrm, "customer_lookup",
                false, "NA_ValueFROM_MAHA_LOOKUP", false, false, "status", null, null, null);

        return DruidExpression.of(
                new SimpleExtraction("added",
                        mahaRegisteredLookupExtractionFn), "abc");
    }

    @Nullable
    @Override
    public DruidExpression toDruidExpressionWithPostAggOperands(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode, PostAggregatorVisitor postAggregatorVisitor) {
        LookupReferencesManager lrm = (LookupReferencesManager) lookupReferencesManager;
        MahaRegisteredLookupExtractionFn mahaRegisteredLookupExtractionFn = new MahaRegisteredLookupExtractionFn(lrm, "customer_lookup",
                false, "NA_ValueFROM_MAHA_LOOKUP", false, false, "status", null, null, null);

        return DruidExpression.of(
                new SimpleExtraction("added",
                        mahaRegisteredLookupExtractionFn), "abc_added");

    }

    @Nullable
    @Override
    public DimFilter toDruidFilter(PlannerContext plannerContext, RowSignature rowSignature, @Nullable VirtualColumnRegistry virtualColumnRegistry, RexNode rexNode) {
        LookupReferencesManager lrm = (LookupReferencesManager) lookupReferencesManager;
        MahaRegisteredLookupExtractionFn mahaRegisteredLookupExtractionFn = new MahaRegisteredLookupExtractionFn(lrm, "customer_lookup",
                false, "MahaLookupOperatorConversion.java", false, false, "status", null, null, null);

        return new ExtractionDimFilter("testabc", "testvalue", mahaRegisteredLookupExtractionFn,  mahaRegisteredLookupExtractionFn );

    }

    @Nullable
    @Override
    public PostAggregator toPostAggregator(PlannerContext plannerContext, RowSignature querySignature, RexNode rexNode, PostAggregatorVisitor postAggregatorVisitor) {

        return new FieldAccessPostAggregator("abc_test", "abc_w");
    }

/*    @Override
    public DruidExpression toDruidExpression(
            final PlannerContext plannerContext,
            final RowSignature rowSignature,
            final RexNode rexNode
    )
    {
//        return DruidExpression.of(new SimpleExtraction("dummy column", new ExtractionFn() {
//            @Nullable
//            @Override
//            public String apply(@Nullable Object value) {
//                return null;
//            }
//
//            @Nullable
//            @Override
//            public String apply(@Nullable String value) {
//                return null;
//            }
//
//            @Override
//            public String apply(long value) {
//                return null;
//            }
//
//            @Override
//            public boolean preservesOrdering() {
//                return false;
//            }
//
//            @Override
//            public ExtractionType getExtractionType() {
//                return null;
//            }
//
//            @Override
//            public byte[] getCacheKey() {
//                return new byte[0];
//            }
//        }), "1 == 1");
        return DruidExpression.of(
                new SimpleExtraction("dummy column",
                new MahaLookupExtractionFn(
                        null,
                        false,
                        "replaced",
                        false,
                        true,
                        "value_column",
                        null,
                        null)
                ), "12345678"
        );
    }*/
}
