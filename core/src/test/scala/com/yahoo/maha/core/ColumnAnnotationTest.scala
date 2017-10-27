// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import org.scalatest.{FunSuite, Matchers}

/**
  * Created by hiral on 9/14/17.
  */
class ColumnAnnotationTest extends FunSuite with Matchers {
  test("successfully find HiveShardingExpression with instance") {
    val set: Set[ColumnAnnotation] = Set(HiveShardingExpression(null))
    set.contains(HiveShardingExpression.instance) === true
  }

  test("successfully find ForeignKey with instance") {
    val set: Set[ColumnAnnotation] = Set(ForeignKey("fid"))
    set.contains(ForeignKey.instance) === true
  }

  test("successfully find DayColumn with instance") {
    val set: Set[ColumnAnnotation] = Set(DayColumn("YYYYMMDD"))
    set.contains(DayColumn.instance) === true
  }
}
