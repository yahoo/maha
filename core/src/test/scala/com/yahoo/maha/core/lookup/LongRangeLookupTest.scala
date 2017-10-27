// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.lookup

import org.scalatest.FunSuite

/**
 * Created by hiral on 3/31/16.
 */
class LongRangeLookupTest extends FunSuite {
  
  val list : IndexedSeq[(LongRange, Double)] = IndexedSeq(LongRange(10,20) -> 1.0, LongRange(21, 30) -> 1.1, LongRange(31, 40) -> 1.2)
  val lkp = LongRangeLookup(list)
  test("successfully find element in range") {
    assert(lkp.find(10) === Option(1.0))
    assert(lkp.find(15) === Option(1.0))
    assert(lkp.find(20) === Option(1.0))
    assert(lkp.find(21) === Option(1.1))
    assert(lkp.find(25) === Option(1.1))
    assert(lkp.find(30) === Option(1.1))
    assert(lkp.find(31) === Option(1.2))
    assert(lkp.find(35) === Option(1.2))
    assert(lkp.find(40) === Option(1.2))
  }
  test("successfully not find element in not in range") {
    assert(lkp.find(9) === None)
    assert(lkp.find(41) === None)
  }
  test("fail to create a lookup with overlapping ranges") {
    intercept[IllegalArgumentException] {
      LongRangeLookup(IndexedSeq(LongRange(10,20) -> 1.0, LongRange(21, 30) -> 1.1, LongRange(30, 40) -> 1.2))
    }
  }
}
