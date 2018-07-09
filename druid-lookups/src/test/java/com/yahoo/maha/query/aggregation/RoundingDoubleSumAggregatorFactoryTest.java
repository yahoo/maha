package com.yahoo.maha.query.aggregation;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RoundingDoubleSumAggregatorFactoryTest {

    @Test
    public void testFinalization() {
        RoundingDoubleSumAggregatorFactory factory = new RoundingDoubleSumAggregatorFactory("name", "fieldName", 2, null, null, true);
        Assert.assertEquals(factory.finalizeComputation(123.45678999d), 123.46d);
        Assert.assertEquals(factory.finalizeComputation(123.45178999d), 123.45d);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "Must have a valid, non-null scale")
    public void testNullScale() {
        RoundingDoubleSumAggregatorFactory factory = new RoundingDoubleSumAggregatorFactory("name", "fieldName", null, null, null, true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Must have a valid, greater than 0 scale")
    public void testInvalidScale() {
        RoundingDoubleSumAggregatorFactory factory = new RoundingDoubleSumAggregatorFactory("name", "fieldName", -1, null, null, true);
    }

    @Test
    public void testWhenRoundingDoubleSumAggregatorFactoryIsDisabled() {
        RoundingDoubleSumAggregatorFactory factory = new RoundingDoubleSumAggregatorFactory("name", "fieldName", 2, null, null, false);
        Assert.assertEquals(factory.finalizeComputation(123.45678999d), 123.45678999d);
    }

    @Test
    public void testWhenRoundingDoubleSumAggregatorFactoryIsNull() {
        RoundingDoubleSumAggregatorFactory factory = new RoundingDoubleSumAggregatorFactory("name", "fieldName", 2, null, null, null);
        Assert.assertEquals(factory.finalizeComputation(123.45678999d), 123.45678999d);
    }

    @Test
    public void testFinalizationWhenValueIsNull() {
        RoundingDoubleSumAggregatorFactory factory = new RoundingDoubleSumAggregatorFactory("name", "fieldName", 2, null, null, true);
        Assert.assertNull(factory.finalizeComputation(null));
    }
}
