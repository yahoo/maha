// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
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

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Must have a valid, greater than or equal to 0 scale")
    public void testInvalidScale() {
        RoundingDoubleSumAggregatorFactory factory = new RoundingDoubleSumAggregatorFactory("name", "fieldName", -1, null, null, true);
    }

    @Test
    public void testWhenRoundingDoubleSumAggregatorFactoryIsDisabled() {
        RoundingDoubleSumAggregatorFactory factory = new RoundingDoubleSumAggregatorFactory("name", "fieldName", 2, null, null, false);
        Assert.assertEquals(factory.finalizeComputation(123.45678999d), 123.45678999d);
    }

    @Test
    public void testFinalizationWhenValueIsNull() {
        RoundingDoubleSumAggregatorFactory factory = new RoundingDoubleSumAggregatorFactory("name", "fieldName", 2, null, null, true);
        Assert.assertNull(factory.finalizeComputation(null));
    }
}
