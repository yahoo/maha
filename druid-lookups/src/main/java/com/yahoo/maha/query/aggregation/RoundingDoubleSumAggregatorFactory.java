package com.yahoo.maha.query.aggregation;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.math.expr.ExprMacroTable;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class RoundingDoubleSumAggregatorFactory extends DoubleSumAggregatorFactory {

    private final int scale;

    @JsonCreator
    public RoundingDoubleSumAggregatorFactory(@JsonProperty("name") String name, @JsonProperty("fieldName") String fieldName, @JsonProperty("scale") Integer scale, @JsonProperty("expression") String expression, @JacksonInject ExprMacroTable macroTable) {
        super(name, fieldName, expression, macroTable);
        this.scale = scale;
    }

    public Object finalizeComputation(Object object) {
        if(!(object instanceof Double)) {
            return object;
        }
        return new BigDecimal((Double)object).setScale(scale, RoundingMode.HALF_UP).doubleValue();
    }

}
