// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.lookup

import org.scalatest.FunSuite

/**
 * Created by hiral on 3/31/16.
 */
class LongRangeTest extends FunSuite{
  val range = LongRange(10, 20)
  test("successfully test an element in range") {
    assert(range.inRange(10))
    assert(range.inRange(15))
    assert(range.inRange(20))
  }
  test("successfully test an element out of range") {
    assert(!range.inRange(9))
    assert(!range.inRange(21))
  }
  test("successfully test an element is before range") {
    assert(range.isBefore(9))
  }
  test("successfully test an element is not before range") {
    assert(!range.isBefore(10))
    assert(!range.isBefore(15))
    assert(!range.isBefore(20))
    assert(!range.isBefore(21))
  }
  test("fail to create an invalid range") {
    intercept[IllegalArgumentException] {
      LongRange(20, 10)
    }
  }
}
