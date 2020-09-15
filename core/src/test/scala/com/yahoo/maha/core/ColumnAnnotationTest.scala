// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import com.yahoo.maha.core.BaseExpressionTest.PRESTO_TIMESTAMP_TO_FORMATTED_DATE
import com.yahoo.maha.core.HiveExpression._
import org.json4s.JObject
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
  * Created by hiral on 9/14/17.
  */
class ColumnAnnotationTest extends AnyFunSuite with Matchers {
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

  test("successfully find PrestoShardingExpression with instance") {
    val set: Set[ColumnAnnotation] = Set(PrestoShardingExpression(null))
    set.contains(PrestoShardingExpression.instance) === true
  }

  test("Instantiate  Hive/PrestoShardingExpression") {
    implicit val cc: ColumnContext = new ColumnContext
    val derivedMin : HiveDerivedExpression = HiveDerivedExpression.fromExpression(MIN("{thing}"))
    val shardingExpr : HiveShardingExpression = new HiveShardingExpression(derivedMin)
    assert(shardingExpr.instance == HiveShardingExpression.instance)

    val prestoMin : PrestoDerivedExpression = PrestoDerivedExpression.fromExpression(PRESTO_TIMESTAMP_TO_FORMATTED_DATE("{created_date}", "YYYY-MM-dd"))
    val prestoShardingExpr : PrestoShardingExpression = new PrestoShardingExpression(prestoMin)
    assert(prestoShardingExpr.instance == PrestoShardingExpression.instance)
  }

  test("Instantiate ForeignKey and DayColumn") {
    val fk : ForeignKey = new ForeignKey("public_name")
    assert(fk.publicDimName == "public_name")
    assert(fk.instance == ForeignKey.instance)
    val dc : DayColumn = new DayColumn("format")
    assert(dc.fmt == "format")
    assert(dc.instance == DayColumn.instance)
  }

  test("Column annotations should convert to JSON properly.") {
    implicit val cc: ColumnContext = new ColumnContext
    val fk: ForeignKey = new ForeignKey("pd")
    val dc : DayColumn = new DayColumn("format")
    val derivedMin : HiveDerivedExpression = HiveDerivedExpression.fromExpression(MIN("{thing}"))
    val prestoMin : PrestoDerivedExpression = PrestoDerivedExpression.fromExpression(PRESTO_TIMESTAMP_TO_FORMATTED_DATE("{created_date}", "YYYY-MM-dd"))

    val hs: HiveShardingExpression = new HiveShardingExpression(derivedMin)
    val ps: PrestoShardingExpression = new PrestoShardingExpression(prestoMin)

    val expns = Set(fk, dc, hs, ps)

    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    implicit val formats = DefaultFormats

    //All actual Annotations
    val allJSONs: String = expns.map(expn => compact(expn.asJSON)).mkString(",")

    //All instances with null args (null checking)
    val instances: String = expns.map(expn => compact(expn.instance.asJSON)).mkString(",")

    val allAnnotations = List(
      """{"annotation":"ForeignKey","publicDimName":"pd"}"""
      ,"""{"annotation":"DayColumn","fmt":"format"}"""
      ,"""{"annotation":"HiveShardingExpression","expression":"HiveDerivedExpression(""" //split into two to avoid columnContext.
      ,""",MIN(COL({thing},false,false)))"}"""
      ,"""{"annotation":"PrestoShardingExpression","expression":"PrestoDerivedExpression"""
      ,""",PRESTO_TIMESTAMP_TO_FORMATTED_DATE(COL({created_date},false,false),YYYY-MM-dd))"}"""
    )
    val allnstances = List(
      """{"annotation":"ForeignKey","publicDimName":"instance"}"""
      ,"""{"annotation":"DayColumn","fmt":"instance"}"""
      ,"""{"annotation":"HiveShardingExpression","expression":null}"""
      ,"""{"annotation":"PrestoShardingExpression","expression":null}"""
    )
    assert(allAnnotations.forall(
      annotation =>
        allJSONs.contains(annotation))
    )
    assert(allnstances.forall(
      instance =>
        instances.contains(instance))
    )

  }

}
