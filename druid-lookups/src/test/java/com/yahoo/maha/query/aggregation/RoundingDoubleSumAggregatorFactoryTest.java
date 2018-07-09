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

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "Must have a valid, non-null scale")
    public void testNullScale() {
        RoundingDoubleSumAggregatorFactory factory = new RoundingDoubleSumAggregatorFactory("name", "fieldName", null, null, null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Must have a valid, greater than 0 scale")
    public void testInvalidScale() {
        RoundingDoubleSumAggregatorFactory factory = new RoundingDoubleSumAggregatorFactory("name", "fieldName", -1, null, null);
    }
}
