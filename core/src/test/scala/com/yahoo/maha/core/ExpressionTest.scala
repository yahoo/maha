// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import org.apache.druid.jackson.DefaultObjectMapper
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Created by hiral on 10/12/15.
 */
class ExpressionTest extends AnyFunSuite with Matchers {
  
  val objectMapper = new DefaultObjectMapper()

  test("generate hive expression with +") {
    import HiveExpression._
    val exp : HiveExp = "{clicks}" ++ "1"
    exp.asString should equal ("{clicks} + 1")
  }
  test("generate hive expression with -") {
    import HiveExpression._
    val exp : HiveExp = "{clicks}" - "1"
    exp.asString should equal("{clicks} - 1")
  }
  test("generate hive expression with *") {
    import HiveExpression._
    val exp : HiveExp = "{clicks}" * "1"
    exp.asString should equal("{clicks} * 1")
  }
  test("generate hive expression with /") {
    import HiveExpression._
    val exp : HiveExp = "{clicks}" / "1"
    exp.asString should equal("{clicks} / 1")
  }
  test("generate hive expression with /-") {
    import HiveExpression._
    val exp : HiveExp = "{clicks}" /- "{impressions}"
    exp.asString should equal("CASE WHEN {impressions} = 0 THEN 0.0 ELSE {clicks} / {impressions} END")
  }
  test("generate hive expression with SUM") {
    import HiveExpression._
    val exp : HiveExp = SUM(COL("{avg_pos}") * "{impressions}") /- SUM("{impressions}")
    exp.asString should equal("CASE WHEN SUM({impressions}) = 0 THEN 0.0 ELSE SUM({avg_pos} * {impressions}) / (SUM({impressions})) END")
  }
  test("generate hive expression with nested derivation") {
    import HiveExpression._
    val exp0 : HiveExp = "{clicks}" ++ "1"
    val exp1 : HiveExp = "{impressions}" /- exp0
    exp1.asString should equal("CASE WHEN {clicks} + 1 = 0 THEN 0.0 ELSE {impressions} / ({clicks} + 1) END")
    exp1.render(true) should equal("(CASE WHEN {clicks} + 1 = 0 THEN 0.0 ELSE {impressions} / ({clicks} + 1) END)")
  }
  test("generate hive expression with MAX") {
    import HiveExpression._
    val exp : HiveExp = MAX("{max_bid}")
    exp.asString should equal("MAX({max_bid})")
  }
  test("generate hive expression with TIMESTAMP_TO_FORMATTED_DATE") {
    import HiveExpression._
    import com.yahoo.maha.core.BaseExpressionTest._
    val exp: HiveExp = TIMESTAMP_TO_FORMATTED_DATE("{created_date}","YYYY-MM-dd")
    exp.asString should equal("getDateFromEpoch({created_date}, 'YYYY-MM-dd')")
  }
  test("generate hive expression with GET_INTERVAL_DATE") {
    import HiveExpression._
    import com.yahoo.maha.core.BaseExpressionTest._
    val exp1: HiveExp = GET_INTERVAL_DATE("{stats_date}", "d")
    exp1.asString should equal("getIntervalDate({stats_date}, 'd')")
    val exp2: HiveExp = GET_INTERVAL_DATE("{stats_date}", "w")
    exp2.asString should equal("getIntervalDate({stats_date}, 'w')")
    val exp3: HiveExp = GET_INTERVAL_DATE("{stats_date}", "m")
    exp3.asString should equal("getIntervalDate({stats_date}, 'm')")
  }
  test("generate hive expression with DECODE") {
    import HiveExpression._
    import com.yahoo.maha.core.BaseExpressionTest._
    val exp: HiveExp = DECODE("{engagement_type}", "1", "{engagement_count}", "0")
    exp.asString should equal("decodeUDF({engagement_type}, 1, {engagement_count}, 0)")
  }
  test("generate hive expression with COALESCE") {
    import HiveExpression._
    val exp: HiveExp = COALESCE("{conversions}", "0")
    exp.asString should equal("coalesce({conversions}, 0)")
  }
  test("generate hive expression with GET_A_BY_B") {
    import HiveExpression._
    import com.yahoo.maha.core.BaseExpressionTest._
    val exp: HiveExp = GET_A_BY_B("{spend}", "{clicks}")
    exp.asString should equal("getAbyB({spend}, {clicks})")
  }
  test("generate hive expression with GET_A_BY_B_PLUS_C") {
    import HiveExpression._
    import com.yahoo.maha.core.BaseExpressionTest._
    val exp: HiveExp = GET_A_BY_B_PLUS_C("{spend}", "{conversions}", "{post_imp_conversions}")
    exp.asString should equal("getAbyBplusC({spend}, {conversions}, {post_imp_conversions})")
  }
  test("generate hive expression with GET_CONDITIONAL_A_BY_B") {
    import HiveExpression._
    import com.yahoo.maha.core.BaseExpressionTest._
    val exp: HiveExp = "100" * GET_CONDITIONAL_A_BY_B("{engagement_type}", "1", "{engagement_count}", "0", "{impressions}")
    exp.asString should equal("100 * (getConditionalAbyB({engagement_type}, 1, {engagement_count}, 0, {impressions}))")
  }
  test("generate oracle expression with +") {
    import OracleExpression._
    val exp : OracleExp = "{clicks}" ++ "1"
    exp.asString should equal ("{clicks} + 1")
  }
  test("generate oracle expression with -") {
    import OracleExpression._
    val exp : OracleExp = "{clicks}" - "1"
    exp.asString should equal("{clicks} - 1")
  }
  test("generate oracle expression with *") {
    import OracleExpression._
    val exp : OracleExp = "{clicks}" * "1"
    exp.asString should equal("{clicks} * 1")
  }
  test("generate oracle expression with /") {
    import OracleExpression._
    val exp : OracleExp = "{clicks}" / "1"
    exp.asString should equal("{clicks} / 1")
  }
  test("generate oracle expression with /-") {
    import OracleExpression._
    val exp : OracleExp = "{clicks}" /- "{impressions}"
    exp.asString should equal("CASE WHEN {impressions} = 0 THEN 0.0 ELSE {clicks} / {impressions} END")
  }
  test("generate oracle expression with sum") {
    import OracleExpression._
    val exp : OracleExp = SUM(COL("{avg_pos}") * "{impressions}") /- SUM("{impressions}")
    exp.asString should equal("CASE WHEN SUM({impressions}) = 0 THEN 0.0 ELSE SUM({avg_pos} * {impressions}) / (SUM({impressions})) END")
  }
  test("generate oracle expression with nested derivation") {
    import OracleExpression._
    val exp0 : OracleExp = "{clicks}" ++ "1"
    val exp1 : OracleExp = "{impressions}" /- exp0
    exp1.asString should equal("CASE WHEN {clicks} + 1 = 0 THEN 0.0 ELSE {impressions} / ({clicks} + 1) END")
    exp1.render(true) should equal("(CASE WHEN {clicks} + 1 = 0 THEN 0.0 ELSE {impressions} / ({clicks} + 1) END)")
  }
  test("generate oracle expression with max") {
    import OracleExpression._
    val exp : OracleExp = MAX("{max_bid}")
    exp.asString should equal("MAX({max_bid})")
  }
  test("generate oracle expression with TIMESTAMP_TO_FORMATTED_DATE") {
    import OracleExpression._
    val exp: OracleExp = TIMESTAMP_TO_FORMATTED_DATE("{created_date}", "YYYY-MM-dd")
    exp.asString should equal("NVL(TO_CHAR(DATE '1970-01-01' + ( 1 / 24 / 60 / 60 / 1000)*CAST(MOD({created_date}}, 32503680000000) AS NUMBER) , 'YYYY-MM-dd'), 'NULL')")
  }
  test("generate oracle expression with GET_INTERVAL_DATE") {
    import OracleExpression._
    val exp1: OracleExp = GET_INTERVAL_DATE("{stats_date}", "D")
    exp1.asString should equal("TRUNC({stats_date})")
    val exp2: OracleExp = GET_INTERVAL_DATE("{stats_date}", "W")
    exp2.asString should equal("TRUNC({stats_date}, 'IW')")
    val exp3: OracleExp = GET_INTERVAL_DATE("{stats_date}", "M")
    exp3.asString should equal("TRUNC({stats_date}, 'MM')")
  }
  test("generate oracle expression with DECODE") {
    import OracleExpression._
    val exp: OracleExp = DECODE("{engagement_type}", "1", "{engagement_count}", "0")
    exp.asString should equal("DECODE({engagement_type}, 1, {engagement_count}, 0)")
  }
  test("generate oracle expression with COALESCE") {
    import OracleExpression._
    val exp: OracleExp = COALESCE("{conversions}", "0")
    exp.asString should equal("COALESCE({conversions}, 0)")
  }
  test("generate oracle expression with TO_CHAR") {
    import OracleExpression._
    val exp: OracleExp = TO_CHAR("{stats_date}", "YYYY-MM-DD")
    exp.asString should equal("TO_CHAR({stats_date}, 'YYYY-MM-DD')")
  }

  test("generate Postgres expression with +") {
    import PostgresExpression._
    val exp : PostgresExp = "{clicks}" ++ "1"
    exp.asString should equal ("{clicks} + 1")
  }
  test("generate Postgres expression with -") {
    import PostgresExpression._
    val exp : PostgresExp = "{clicks}" - "1"
    exp.asString should equal("{clicks} - 1")
  }
  test("generate Postgres expression with *") {
    import PostgresExpression._
    val exp : PostgresExp = "{clicks}" * "1"
    exp.asString should equal("{clicks} * 1")
  }
  test("generate Postgres expression with /") {
    import PostgresExpression._
    val exp : PostgresExp = "{clicks}" / "1"
    exp.asString should equal("{clicks} / 1")
  }
  test("generate Postgres expression with /-") {
    import PostgresExpression._
    val exp : PostgresExp = "{clicks}" /- "{impressions}"
    exp.asString should equal("CASE WHEN {impressions} = 0 THEN 0.0 ELSE {clicks} / {impressions} END")
  }
  test("generate Postgres expression with sum") {
    import PostgresExpression._
    val exp : PostgresExp = SUM(COL("{avg_pos}") * "{impressions}") /- SUM("{impressions}")
    exp.asString should equal("CASE WHEN SUM({impressions}) = 0 THEN 0.0 ELSE SUM({avg_pos} * {impressions}) / (SUM({impressions})) END")
  }
  test("generate Postgres expression with nested derivation") {
    import PostgresExpression._
    val exp0 : PostgresExp = "{clicks}" ++ "1"
    val exp1 : PostgresExp = "{impressions}" /- exp0
    exp1.asString should equal("CASE WHEN {clicks} + 1 = 0 THEN 0.0 ELSE {impressions} / ({clicks} + 1) END")
    exp1.render(true) should equal("(CASE WHEN {clicks} + 1 = 0 THEN 0.0 ELSE {impressions} / ({clicks} + 1) END)")
  }
  test("generate Postgres expression with max") {
    import PostgresExpression._
    val exp : PostgresExp = MAX("{max_bid}")
    exp.asString should equal("MAX({max_bid})")
  }
  test("generate Postgres expression with TIMESTAMP_TO_FORMATTED_DATE") {
    import PostgresExpression._
    val exp: PostgresExp = TIMESTAMP_TO_FORMATTED_DATE("{created_date}", "YYYY-MM-dd")
    exp.asString should equal("COALESCE(TO_CHAR(TIMESTAMP '1970-01-01' + ( 1 / 24 / 60 / 60 / 1000)*CAST(MOD({created_date}, 32503680000000) AS NUMERIC) , 'YYYY-MM-dd'), 'NULL')")
  }
  test("generate Postgres expression with GET_INTERVAL_DATE") {
    import PostgresExpression._
    val exp1: PostgresExp = GET_INTERVAL_DATE("{stats_date}", "D")
    exp1.asString should equal("DATE_TRUNC('day', {stats_date})::DATE")
    val exp2: PostgresExp = GET_INTERVAL_DATE("{stats_date}", "W")
    exp2.asString should equal("DATE_TRUNC('week', {stats_date})::DATE")
    val exp3: PostgresExp = GET_INTERVAL_DATE("{stats_date}", "M")
    exp3.asString should equal("DATE_TRUNC('month', {stats_date})::DATE")
  }
  test("generate Postgres expression with DECODE") {
    import PostgresExpression._
    val exp: PostgresExp = DECODE("{engagement_type}", "1", "{engagement_count}", "0")
    exp.asString should equal("CASE WHEN {engagement_type} = 1 THEN {engagement_count} ELSE 0 END")
    val exp2: PostgresExp = DECODE("{engagement_type}", "1", "{engagement_count}", "0", "{engagement_count_2}")
    exp2.asString should equal("CASE WHEN {engagement_type} = 1 THEN {engagement_count} WHEN {engagement_type} = 0 THEN {engagement_count_2} END")
  }
  test("generate Postgres expression with DECODE_DIM") {
    import PostgresExpression._
    val exp: PostgresExp = DECODE_DIM("{engagement_type}", "1", "{engagement_count}", "0")
    exp.asString should equal("CASE WHEN {engagement_type} = 1 THEN {engagement_count} ELSE 0 END")
    val exp2: PostgresExp = DECODE_DIM("{engagement_type}", "1", "{engagement_count}", "0", "{engagement_count_2}")
    exp2.asString should equal("CASE WHEN {engagement_type} = 1 THEN {engagement_count} WHEN {engagement_type} = 0 THEN {engagement_count_2} END")
  }
  test("generate Postgres expression with COALESCE") {
    import PostgresExpression._
    val exp: PostgresExp = COALESCE("{conversions}", "0")
    exp.asString should equal("COALESCE({conversions}, 0)")
  }
  test("generate Postgres expression with TO_CHAR") {
    import PostgresExpression._
    val exp: PostgresExp = TO_CHAR("{stats_date}", "YYYY-MM-DD")
    exp.asString should equal("TO_CHAR({stats_date}, 'YYYY-MM-DD')")
  }


  test("generate druid expression with ++") {
    import DruidExpression._
    val exp : DruidExp = "{clicks}" ++ "{impressions}"
    objectMapper.writeValueAsString(exp.render(false)("sum",Map("clicks"->"Clicks"))) shouldBe """{"type":"arithmetic","name":"sum","fn":"+","fields":[{"type":"fieldAccess","name":"clicks","fieldName":"Clicks"},{"type":"fieldAccess","name":"impressions","fieldName":"impressions"}],"ordering":null}"""
  }

  test("generate druid expression with -") {
    import DruidExpression._
    val exp : DruidExp = "{clicks}" - "{impressions}"
    objectMapper.writeValueAsString(exp.render(false)("subtract",Map("clicks"->"Clicks"))) shouldBe """{"type":"arithmetic","name":"subtract","fn":"-","fields":[{"type":"fieldAccess","name":"clicks","fieldName":"Clicks"},{"type":"fieldAccess","name":"impressions","fieldName":"impressions"}],"ordering":null}"""
  }

  test("generate druid expression with *") {
    import DruidExpression._
    val exp : DruidExp = "{clicks}" * "{impressions}"
    objectMapper.writeValueAsString(exp.render(false)("multiply",Map("clicks"->"Clicks"))) shouldBe """{"type":"arithmetic","name":"multiply","fn":"*","fields":[{"type":"fieldAccess","name":"clicks","fieldName":"Clicks"},{"type":"fieldAccess","name":"impressions","fieldName":"impressions"}],"ordering":null}"""
  }

  test("generate druid expression with /") {
    import DruidExpression._
    val exp : DruidExp = "{clicks}" / "{impressions}"
    objectMapper.writeValueAsString(exp.render(false)("divide",Map("clicks"->"Clicks"))) shouldBe """{"type":"arithmetic","name":"divide","fn":"/","fields":[{"type":"fieldAccess","name":"clicks","fieldName":"Clicks"},{"type":"fieldAccess","name":"impressions","fieldName":"impressions"}],"ordering":null}"""
  }

  test("generate druid expression with /-") {
    import DruidExpression._
    val exp : DruidExp = "{clicks}" /- "{impressions}"
    objectMapper.writeValueAsString(exp.render(false)("safe_divide",Map.empty)) shouldBe """{"type":"arithmetic","name":"safe_divide","fn":"/","fields":[{"type":"fieldAccess","name":"clicks","fieldName":"clicks"},{"type":"fieldAccess","name":"impressions","fieldName":"impressions"}],"ordering":null}"""
  }

  test("generate druid expression with * ++ - /-") {
    import DruidExpression._
    val exp : DruidExp = (("{one}" * "{two}") ++ "{three}") /- "{four}"
    objectMapper.writeValueAsString(exp.render(false)("complex_math",Map.empty)) shouldBe """{"type":"arithmetic","name":"complex_math","fn":"/","fields":[{"type":"arithmetic","name":"_placeHolder_3","fn":"+","fields":[{"type":"arithmetic","name":"_placeHolder_2","fn":"*","fields":[{"type":"fieldAccess","name":"one","fieldName":"one"},{"type":"fieldAccess","name":"two","fieldName":"two"}],"ordering":null},{"type":"fieldAccess","name":"three","fieldName":"three"}],"ordering":null},{"type":"fieldAccess","name":"four","fieldName":"four"}],"ordering":null}"""
  }

  test("generate Bigquery expression with +") {
    import BigqueryExpression._
    val exp : BigqueryExp = "{clicks}" ++ "1"
    exp.asString should equal ("{clicks} + 1")
  }

  test("generate Bigquery expression with -") {
    import BigqueryExpression._
    val exp : BigqueryExp = "{clicks}" - "1"
    exp.asString should equal("{clicks} - 1")
  }

  test("generate Bigquery expression with *") {
    import BigqueryExpression._
    val exp : BigqueryExp = "{clicks}" * "1"
    exp.asString should equal("{clicks} * 1")
  }

  test("generate Bigquery expression with /") {
    import BigqueryExpression._
    val exp : BigqueryExp = "{clicks}" / "1"
    exp.asString should equal("{clicks} / 1")
  }

  test("generate Bigquery expression with /-") {
    import BigqueryExpression._
    val exp : BigqueryExp = "{clicks}" /- "{impressions}"
    exp.asString should equal("CASE WHEN {impressions} = 0 THEN 0.0 ELSE {clicks} / {impressions} END")
  }

  test("generate Bigquery expression with SUM") {
    import BigqueryExpression._
    val exp : BigqueryExp = SUM(COL("{avg_pos}") * "{impressions}") /- SUM("{impressions}")
    exp.asString should equal("CASE WHEN SUM({impressions}) = 0 THEN 0.0 ELSE SUM({avg_pos} * {impressions}) / (SUM({impressions})) END")
  }

  test("generate Bigquery expression with nested derivation") {
    import BigqueryExpression._
    val exp0 : BigqueryExp = "{clicks}" ++ "1"
    val exp1 : BigqueryExp = "{impressions}" /- exp0
    exp1.asString should equal("CASE WHEN {clicks} + 1 = 0 THEN 0.0 ELSE {impressions} / ({clicks} + 1) END")
    exp1.render(true) should equal("(CASE WHEN {clicks} + 1 = 0 THEN 0.0 ELSE {impressions} / ({clicks} + 1) END)")
  }

  test("generate Bigquery expression with MAX") {
    import BigqueryExpression._
    val exp : BigqueryExp = MAX("{max_bid}")
    exp.asString should equal("MAX({max_bid})")
  }

  test("generate Bigquery expression with TIMESTAMP_TO_FORMATTED_DATE") {
    import BigqueryExpression._
    val exp: BigqueryExp = TIMESTAMP_TO_FORMATTED_DATE("{created_date}","YYYY-MM-dd")
    exp.asString should equal("FORMAT_TIMESTAMP('YYYY-MM-dd', {created_date})")
  }

  test("generate Bigquery expression with GET_INTERVAL_DATE") {
    import BigqueryExpression._
    val exp1: BigqueryExp = GET_INTERVAL_DATE("{stats_date}", "d")
    exp1.asString should equal("DATE_TRUNC({stats_date}, DAY)")
    val exp2: BigqueryExp = GET_INTERVAL_DATE("{stats_date}", "w")
    exp2.asString should equal("DATE_TRUNC({stats_date}, WEEK)")
    val exp3: BigqueryExp = GET_INTERVAL_DATE("{stats_date}", "m")
    exp3.asString should equal("DATE_TRUNC({stats_date}, MONTH)")
  }

  test("generate Bigquery expression with DECODE") {
    import BigqueryExpression._
    val exp: BigqueryExp = DECODE("{engagement_type}", "1", "{engagement_count}", "0")
    exp.asString should equal("CASE WHEN {engagement_type} = 1 THEN {engagement_count} ELSE 0 END")
  }

  test("generate Bigquery expression with COALESCE") {
    import BigqueryExpression._
    val exp: BigqueryExp = COALESCE("{conversions}", "0")
    exp.asString should equal("COALESCE({conversions}, 0)")
  }
}
