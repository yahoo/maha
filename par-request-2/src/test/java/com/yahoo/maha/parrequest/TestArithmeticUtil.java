// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest;

import com.yahoo.maha.parrequest2.ArithmeticUtil;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * Created by shrav87 on 3/12/15.
 */
public class TestArithmeticUtil {

    @Test
    public void testAddLongList() {
        List<Long> listofVals = Arrays.asList(1L, 2L, 3L, 4L, null);
        assertEquals(ArithmeticUtil.addLongList(listofVals), (Long) 10L);
    }
}
