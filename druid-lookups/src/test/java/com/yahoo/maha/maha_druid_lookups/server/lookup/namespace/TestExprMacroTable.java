package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.google.common.collect.ImmutableList;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.*;

public class TestExprMacroTable extends ExprMacroTable
{
    public static final ExprMacroTable INSTANCE = new TestExprMacroTable();

    private TestExprMacroTable()
    {
        super(
                ImmutableList.of(
                        new IPv4AddressMatchExprMacro(),
                        new IPv4AddressParseExprMacro(),
                        new IPv4AddressStringifyExprMacro(),
                        new LikeExprMacro(),
                        new RegexpExtractExprMacro(),
                        new TimestampCeilExprMacro(),
                        new TimestampExtractExprMacro(),
                        new TimestampFloorExprMacro(),
                        new TimestampFormatExprMacro(),
                        new TimestampParseExprMacro(),
                        new TimestampShiftExprMacro(),
                        new TrimExprMacro.BothTrimExprMacro(),
                        new TrimExprMacro.LeftTrimExprMacro(),
                        new TrimExprMacro.RightTrimExprMacro()
                )
        );
    }
}
