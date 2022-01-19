package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;


import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.lookup.*;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;

import org.easymock.EasyMock;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.apache.calcite.rel.type.RelDataTypeFactory;

public class MahaLookupOperatorConversionTest {

    private RexBuilder rexBuilder;
    private RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

    private static final RowSignature ROW_SIGNATURE = RowSignature
            .builder()
            .add("d", ValueType.DOUBLE)
            .add("l", ValueType.LONG)
            .add("s", ValueType.STRING)
            .add("student_id", ValueType.STRING)
            .build();

    @BeforeTest
    public void setUp(){
        this.rexBuilder = new RexBuilder(typeFactory);
        NullHandling.initializeForTests();
    }

    @AfterTest
    public void shutDown() {

    }

    @Test
    public void testLookupReturnsExpectedResults() {
        final LookupExtractorFactoryContainerProvider manager = EasyMock.createStrictMock(LookupReferencesManager.class);

        MahaLookupOperatorConversion opConversion = new MahaLookupOperatorConversion(manager);
        ExprMacroTable exprMacroTable = TestExprMacroTable.INSTANCE;

        PlannerContext plannerContext = EasyMock.createStrictMock(PlannerContext.class);
        EasyMock.expect(plannerContext.getExprMacroTable()).andReturn(exprMacroTable).anyTimes();
        EasyMock.replay(plannerContext);
        RexNode rn2 = rexBuilder.makeCall(opConversion.calciteOperator(),
                makeInputRef("student_id"),
                rexBuilder.makeLiteral(
                        "student_lookup",
                        typeFactory.createSqlType(SqlTypeName.VARCHAR), true
                ),
                rexBuilder.makeLiteral(
                        "student_id",
                        typeFactory.createSqlType(SqlTypeName.VARCHAR), true
                ),
                rexBuilder.makeLiteral(
                        "123",
                        typeFactory.createSqlType(SqlTypeName.VARCHAR), true
                ));

        DruidExpression druidExp = opConversion.toDruidExpression(plannerContext, ROW_SIGNATURE, rn2);
        assert druidExp != null;
        String expectedDruidExpr = "DruidExpression{simpleExtraction=MahaRegisteredLookupExtractionFn{delegate=null, lookup='student_lookup', retainMissingValue=false, replaceMissingValueWith='123', injective=false, optimize=false, valueColumn=student_id, decodeConfig=null, useQueryLevelCache=false}(student_id), expression='maha'}";
        assert druidExp.toString().equals(expectedDruidExpr);
    }

    RexNode makeInputRef(String columnName)
    {
        RelDataType relDataType = RowSignatures.toRelDataType(ROW_SIGNATURE, typeFactory);
        int columnNumber = ROW_SIGNATURE.indexOf(columnName);
        return rexBuilder.makeInputRef(relDataType.getFieldList().get(columnNumber).getType(), columnNumber);
    }
}
