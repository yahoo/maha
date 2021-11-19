package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.yahoo.maha.maha_druid_lookups.query.lookup.MahaLookupExtractionFn;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.druid.sql.calcite.expression.*;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignature;

public class MahaLookupOperatorConversion implements SqlOperatorConversion {

    private static final String DRUID_FUNC_NAME = "MYFUNC";

    private static final SqlFunction SQL_FUNCTION = OperatorConversions
            .operatorBuilder(DRUID_FUNC_NAME)
            .operandTypes(SqlTypeFamily.CHARACTER)
            .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
            .returnTypeInference(ReturnTypes.ARG0)
            .build();

    @Override
    public SqlOperator calciteOperator()
    {
        return SQL_FUNCTION;
    }

    public MahaLookupOperatorConversion() {
        //super(SQL_FUNCTION, DRUID_FUNC_NAME);

    }

    @Override
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
    }
}
