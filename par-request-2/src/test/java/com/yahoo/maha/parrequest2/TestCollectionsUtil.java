// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2;

import com.yahoo.maha.parrequest2.CollectionsUtil;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertTrue;

/**
 * Created by hiral on 6/26/14.
 */
public class TestCollectionsUtil {

    @Test
    public void testEmptyList() {
        List<Integer> integers = CollectionsUtil.emptyList();
        assertTrue(integers.isEmpty());
    }

    @Test
    public void testEmptyMap() {
        Map<Integer, Long> integersLongMap = CollectionsUtil.emptyMap();
        assertTrue(integersLongMap.isEmpty());
    }
}
