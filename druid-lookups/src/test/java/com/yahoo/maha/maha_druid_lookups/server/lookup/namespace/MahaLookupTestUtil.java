package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.aggregation.DimensionExpression;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class MahaLookupTestUtil {
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

    DateTime todayTime = DateTime.now(DateTimeZone.UTC);
    DateTime lastWeekTime = todayTime.minusDays(7);
    String today = todayTime.toString("YYYY-MM-DD");
    String lastWeek = lastWeekTime.toString("YYYY-MM-DD");

    RexNode makeInputRef(String columnName, RowSignature ROW_SIGNATURE, RexBuilder rexBuilder)
    {
        RelDataType relDataType = RowSignatures.toRelDataType(ROW_SIGNATURE, typeFactory);
        int columnNumber = ROW_SIGNATURE.indexOf(columnName);
        return rexBuilder.makeInputRef(relDataType.getFieldList().get(columnNumber).getType(), columnNumber);
    }

    String convertToJson(DruidExpression druidExpression, String cubeName, String outputName) throws JsonProcessingException {
        GroupByQuery query = GroupByQuery.builder()
                .setDataSource(cubeName)
                .setInterval(lastWeek + "T00:00:00.000Z/" + today + "T00:00:00.000Z")
                .setGranularity(Granularities.ALL)
                .setDimensions(
                        DimensionExpression.ofSimpleColumn(outputName, druidExpression, ValueType.STRING).toDimensionSpec()
                )
                .build();

        DefaultObjectMapper mapper = new DefaultObjectMapper();

        return mapper.writeValueAsString(query);
    }

    static class MAHA_LOOKUP{
        private RexNode inputRef;
        private RexNode lookupName;
        private RexNode lookupCol;
        private RexNode replaceIfNull;
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

        MAHA_LOOKUP(RexNode inputRef
                , Pair<String, SqlTypeName> lookupName
                , Pair<String, SqlTypeName> lookupCol
                , Pair<String, SqlTypeName> replaceIfNull
                , RexBuilder rexBuilder
        ) {
            this.inputRef = inputRef;
            this.lookupName = makeLiteral(rexBuilder, lookupName.getKey(), lookupName.getValue(), true);
            this.lookupCol = makeLiteral(rexBuilder, lookupCol.getKey(), lookupCol.getValue(), true);
            this.replaceIfNull = makeLiteral(rexBuilder, replaceIfNull.getKey(), replaceIfNull.getValue(), true);
        }

        RexNode makeLiteral(RexBuilder rexBuilder, String literalName, SqlTypeName dataType, Boolean allowCast) {
            return rexBuilder.makeLiteral(literalName, typeFactory.createSqlType(dataType), allowCast);
        }

        RexNode makeCall(RexBuilder rexBuilder, SqlOperator operator){
            return rexBuilder.makeCall(operator, inputRef, lookupName, lookupCol, replaceIfNull);
        }

    }
}
