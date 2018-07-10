package com.yahoo.maha.query.aggregation;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.math.expr.ExprMacroTable;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class RoundingDoubleSumAggregatorFactory extends DoubleSumAggregatorFactory {

    private final int scale;
    private final boolean enableRoundingDoubleSumAggregatorFactory;

    @JsonCreator
    public RoundingDoubleSumAggregatorFactory(@JsonProperty("name") String name,
                                              @JsonProperty("fieldName") String fieldName,
                                              @JsonProperty("scale") Integer scale,
                                              @JsonProperty("expression") String expression,
                                              @JacksonInject ExprMacroTable macroTable,
                                              @JacksonInject @JsonProperty("enableRoundingDoubleSumAggregatorFactory") Boolean enableRoundingDoubleSumAggregatorFactory) {
        super(name, fieldName, expression, macroTable);
        Preconditions.checkNotNull(scale, "Must have a valid, non-null scale");
        Preconditions.checkArgument(scale >= 0, "Must have a valid, greater than or equal to 0 scale");
        this.scale = scale;
        this.enableRoundingDoubleSumAggregatorFactory = enableRoundingDoubleSumAggregatorFactory == null ? false : enableRoundingDoubleSumAggregatorFactory;
    }

    public RoundingDoubleSumAggregatorFactory(String name, String fieldName, int scale) {
        this(name, fieldName, scale, null, ExprMacroTable.nil(), null);
    }

    @Override
    public Object finalizeComputation(Object object) {
        if(!(object instanceof Double) || !enableRoundingDoubleSumAggregatorFactory) {
            return object;
        }
        return new BigDecimal((Double)object).setScale(scale, RoundingMode.HALF_UP).doubleValue();
    }

}
