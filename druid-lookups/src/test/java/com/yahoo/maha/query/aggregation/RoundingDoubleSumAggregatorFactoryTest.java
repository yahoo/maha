package com.yahoo.maha.query.aggregation;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RoundingDoubleSumAggregatorFactoryTest {

    @Test
    public void testFinalization() {
        RoundingDoubleSumAggregatorFactory factory = new RoundingDoubleSumAggregatorFactory("name", "fieldName", 2, null, null);
        Assert.assertEquals(factory.finalizeComputation(123.45678999d), 123.46d);
        Assert.assertEquals(factory.finalizeComputation(123.45178999d), 123.45d);
    }
}
