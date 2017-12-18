// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import com.yahoo.maha.core.BaseExpressionTest.{FACT_HIVE_EXPRESSION, PRESTO_TIMESTAMP_TO_FORMATTED_DATE, TIMESTAMP_TO_FORMATTED_DATE}
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact._
import io.druid.jackson.DefaultObjectMapper
import org.scalatest.{FunSuite, Matchers}

/**
 * Created by hiral on 10/13/15.
 */
class DerivedExpressionTest extends FunSuite with Matchers {

  test("successfully derive dependent columns from HiveDerivedExpression") {
    import HiveExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("created_date", IntType())
      val col = HiveDerDimCol("Keyword Date Created", StrType(), TIMESTAMP_TO_FORMATTED_DATE("{created_date}", "YYYY-MM-dd"), annotations = Set())
      col.derivedExpression.sourceColumns.contains("created_date") should equal(true)
      col.derivedExpression.render(col.name) should equal("getDateFromEpoch(created_date, 'YYYY-MM-dd')")
    }
  }

  test("successfully derive dependent columns from HiveDerivedExpression TRIM") {
    import HiveExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("created_date", IntType())
      val col = HiveDerDimCol("Keyword Date Created", StrType(), TRIM("{created_date}"), annotations = Set())
      col.derivedExpression.sourceColumns.contains("created_date") should equal(true)
      col.derivedExpression.render(col.name) should equal("trim(created_date)")
    }
  }

  test("successfully derive dependent columns from HiveShardingExpression") {
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent columns
      DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
      HivePartDimCol("shard", StrType(), annotations = Set())

      val col = HiveShardingExpression("{shard} = PMOD({advertiser_id}, 31)")
      col.expression.sourceColumns.contains("shard") should equal(true)
      col.expression.sourceColumns.contains("advertiser_id") should equal(true)
      col.expression.render(col.expression.toString) should equal("shard = PMOD(advertiser_id, 31)")
    }
  }

  test("successfully derive dependent columns from OracleDerivedExpression") {
    import OracleExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("created_date", IntType())
      
      val col = OracleDerDimCol("Keyword Date Created", StrType(), FORMAT_DATE_WITH_LEAST("{created_date}", "YYYY-MM-DD"))
      col.derivedExpression.sourceColumns.contains("created_date") should equal(true)
      col.derivedExpression.render(col.name) should equal("NVL(TO_CHAR(LEAST(created_date,TO_DATE('01-Jan-3000','dd-Mon-YYYY')), 'YYYY-MM-DD'), 'NULL')")
    }
  }

  test("successfully derive dependent columns from OracleDerivedExpression NVL") {
    import OracleExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("created_date", IntType())

      val col = OracleDerDimCol("Keyword Date Created", StrType(), NVL("{created_date}", "Default String"))
      col.derivedExpression.sourceColumns.contains("created_date") should equal(true)
    }
  }

  test("successfully derive dependent columns from DruidDerivedExpression") {
    import DruidExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("clicks", IntType())
      DimCol("impressions", IntType())

      val om = new DefaultObjectMapper()
      val col = DruidDerFactCol("BLAH", StrType(), "{clicks}" ++ "{impressions}")
      col.derivedExpression.sourceColumns.contains("clicks") should equal(true)
      col.derivedExpression.sourceColumns.contains("impressions") should equal(true)
      val json = om.writeValueAsString(col.derivedExpression.render(col.name)("BLAH", Map("clicks"->"Clicks")))
      println(json)
      json should equal("""{"type":"arithmetic","name":"BLAH","fn":"+","fields":[{"type":"fieldAccess","name":"clicks","fieldName":"Clicks"},{"type":"fieldAccess","name":"impressions","fieldName":"impressions"}],"ordering":null}""")
    }
  }

  test("successfully derive dependent columns from OracleDerivedExpression with table alias") {
    import OracleExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("clicks", IntType())
      DimCol("impressions", IntType())


      val col = OracleDerFactCol("ctr", DecType(), "{clicks}" /- "{impressions}" * "1000")
      col.derivedExpression.sourceColumns.contains("clicks") should equal(true)
      col.derivedExpression.sourceColumns.contains("impressions") should equal(true)
      col.derivedExpression.render(col.name, columnPrefix = Option("tableAlias.")) should equal(
        """CASE WHEN tableAlias."impressions" = 0 THEN 0.0 ELSE tableAlias."clicks" / tableAlias."impressions" END * 1000""")



      val col2 = OracleDerFactCol("ad_extn_spend", DecType(), DECODE("{ad_extn_spend}", """'\N'""", "NULL", "{ad_extn_spend}"))
      col2.derivedExpression.sourceColumns.contains("ad_extn_spend") should equal(true)
      col2.derivedExpression.render(col2.name, columnPrefix = Option("tableAlias.")) should equal(
        """DECODE(tableAlias."ad_extn_spend", '\N', NULL, tableAlias."ad_extn_spend")""")

    }
  }

  test("successfully derive dependent columns from OracleDerivedExpressions when expandDerivedExpression is true and false") {
    import OracleExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("clicks", IntType())
      DimCol("impressions", IntType())
      OracleDerFactCol("De 1", DecType(), "{clicks}" * "1000")
      OracleDerFactCol("De 2", DecType(), "{impressions}" * "1000")
      val col = OracleDerFactCol("De 3", DecType(), "{De 1}" /- "{De 2}")
      col.derivedExpression.sourceColumns.contains("De 1") should equal(true)
      col.derivedExpression.sourceColumns.contains("De 2") should equal(true)
      col.derivedExpression.render(col.name, columnPrefix = Option("tableAlias."), expandDerivedExpression = false) should equal(
        """CASE WHEN tableAlias."De 2" = 0 THEN 0.0 ELSE tableAlias."De 1" / tableAlias."De 2" END"""
      )
      col.derivedExpression.render(col.name, columnPrefix = Option("tableAlias."), expandDerivedExpression = true) should equal(
        """CASE WHEN (tableAlias."impressions" * 1000) = 0 THEN 0.0 ELSE (tableAlias."clicks" * 1000) / (tableAlias."impressions" * 1000) END"""
      )

      val renderedColumnAliasMap : scala.collection.Map[String, String] = Map("De 1" -> """tableAlias."De 1"""", "De 2" -> """tableAlias."De 2"""")
      col.derivedExpression.render(col.name, renderedColumnAliasMap, expandDerivedExpression = false) should equal(
        """CASE WHEN tableAlias."De 2" = 0 THEN 0.0 ELSE tableAlias."De 1" / tableAlias."De 2" END"""
      )
      col.derivedExpression.render(col.name, renderedColumnAliasMap, expandDerivedExpression = true) should equal(
        """CASE WHEN (impressions * 1000) = 0 THEN 0.0 ELSE (clicks * 1000) / (impressions * 1000) END"""
      )

    }
  }

  test("successfully derive dependent columns from HiveDerivedExpressions when expandDerivedExpression is true and false") {
    import HiveExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("clicks", IntType())
      DimCol("impressions", IntType())
      HiveDerFactCol("De 1", DecType(), "{clicks}" * "1000")
      HiveDerFactCol("De 2", DecType(), "{impressions}" * "1000")
      val col = HiveDerFactCol("De 3", DecType(), "{De 1}" /- "{De 2}")
      col.derivedExpression.sourceColumns.contains("De 1") should equal(true)
      col.derivedExpression.sourceColumns.contains("De 2") should equal(true)
      col.derivedExpression.render(col.name, columnPrefix = Option("tableAlias."), expandDerivedExpression = false) should equal(
        """CASE WHEN tableAlias."De 2" = 0 THEN 0.0 ELSE tableAlias."De 1" / tableAlias."De 2" END"""
      )
      col.derivedExpression.render(col.name, columnPrefix = Option("tableAlias."), expandDerivedExpression = true) should equal(
        """CASE WHEN (tableAlias."impressions" * 1000) = 0 THEN 0.0 ELSE (tableAlias."clicks" * 1000) / (tableAlias."impressions" * 1000) END"""
      )

      val renderedColumnAliasMap : scala.collection.Map[String, String] = Map("De 1" -> """mang_de1""", "De 2" -> """mang_de2""")
      col.derivedExpression.render(col.name, renderedColumnAliasMap, expandDerivedExpression = false) should equal(
        """CASE WHEN mang_de2 = 0 THEN 0.0 ELSE mang_de1 / mang_de2 END"""
      )
      col.derivedExpression.render(col.name, renderedColumnAliasMap, expandDerivedExpression = true) should equal(
        """CASE WHEN (impressions * 1000) = 0 THEN 0.0 ELSE (clicks * 1000) / (impressions * 1000) END"""
      )

    }
  }

  test("successfully derive dependent columns from OracleDerivedExpression IS_ALERT_ELIGIBLE") {
    import OracleExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("recommended_bid", DecType())
      DimCol("native_bid", DecType())

      val col = OracleDerDimCol("alert_eligible", StrType(),
        COMPARE_PERCENTAGE("{native_bid}", "{recommended_bid}", 95, "ADGROUP_LOW_BID",
          COMPARE_PERCENTAGE("{recommended_bid}", "{native_bid}", 100, "CAMPAIGN_BUDGET_CAP", "'NA'")))

      col.derivedExpression.sourceColumns.contains("recommended_bid") should equal(true)
      col.derivedExpression.sourceColumns.contains("native_bid") should equal(true)
      val result = col.derivedExpression.render(col.name, columnPrefix = Option("tableAlias."))
      result should equal(
        """CASE WHEN tableAlias."native_bid" < 0.95 * tableAlias."recommended_bid" THEN 'ADGROUP_LOW_BID'  WHEN tableAlias."recommended_bid" < 1.0 * tableAlias."native_bid" THEN 'CAMPAIGN_BUDGET_CAP' ELSE 'NA' END """)

    }
  }

  test("GET_AVERAGE_VIDEO_SHOWN test") {
    import HiveExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      FactCol("25_complete", DecType())
      FactCol("50_complete", DecType())
      FactCol("75_complete", DecType())
      FactCol("100_complete", DecType())

      val col = HiveDerFactCol("alert_eligible", StrType(), FACT_HIVE_EXPRESSION	("{25_complete}", "{50_complete}","{75_complete}","{100_complete}"))
      col.derivedExpression.sourceColumns.contains("50_complete") should equal(true)
      col.derivedExpression.sourceColumns.contains("25_complete") should equal(true)
    }
  }

  test("GET_SOV_SHARE test") {
    import HiveExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      FactCol("arg1", DecType())
      FactCol("arg2", DecType())

      val col = HiveDerFactCol("alert_eligible", StrType(), FACT_HIVE_EXPRESSION("{arg1}", "{arg2}"))
      col.derivedExpression.sourceColumns.contains("arg1") should equal(true)
      col.derivedExpression.sourceColumns.contains("arg2") should equal(true)
    }
  }

  test("GET_SOV_LOST_SHARE_RANK test") {
    import HiveExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      FactCol("arg1", DecType())
      FactCol("arg2", DecType())

      val col = HiveDerFactCol("alert_eligible", StrType(), FACT_HIVE_EXPRESSION("{arg1}", "{arg2}"))
      col.derivedExpression.sourceColumns.contains("arg1") should equal(true)
      col.derivedExpression.sourceColumns.contains("arg2") should equal(true)
    }
  }

  test("GET_SOV_LOST_SHARE_BUDGET test") {
    import HiveExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      FactCol("arg1", DecType())
      FactCol("arg2", DecType())

      val col = HiveDerFactCol("alert_eligible", StrType(), FACT_HIVE_EXPRESSION("{arg1}", "{arg2}"))
      col.derivedExpression.sourceColumns.contains("arg1") should equal(true)
      col.derivedExpression.sourceColumns.contains("arg2") should equal(true)
    }
  }

  test("GET_WEIGHTED_VIDEO_SHOWN test") {
    import OracleExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      FactCol("25_complete", DecType())
      FactCol("50_complete", DecType())
      FactCol("75_complete", DecType())
      FactCol("100_complete", DecType())

      val col = OracleDerFactCol("alert_eligible", StrType(), GET_WEIGHTED_VIDEO_SHOWN	("{25_complete}", "{50_complete}","{75_complete}","{100_complete}"))
      col.derivedExpression.sourceColumns.contains("50_complete") should equal(true)
      col.derivedExpression.sourceColumns.contains("25_complete") should equal(true)
    }
  }
  test("GET_VIDEO_SHOWN_SUM test") {
    import OracleExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      FactCol("25_complete", DecType())
      FactCol("50_complete", DecType())
      FactCol("75_complete", DecType())
      FactCol("100_complete", DecType())

      val col = OracleDerFactCol("alert_eligible", StrType(), GET_VIDEO_SHOWN_SUM	("{25_complete}", "{50_complete}","{75_complete}","{100_complete}"))
      col.derivedExpression.sourceColumns.contains("50_complete") should equal(true)
      col.derivedExpression.sourceColumns.contains("25_complete") should equal(true)
    }
  }

  test("DAY_OF_WEEK test") {
    import HiveExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("stats_date", DateType())
      val col = HiveDerDimCol("Day of Week", StrType(), DAY_OF_WEEK	("stats_date", "yyyyMMdd"))
      col.derivedExpression.render(col.name) should equal("from_unixtime(unix_timestamp(stats_date, 'yyyyMMdd'), 'EEEE')")
    }
  }

  test("GET_INTERVAL_DATE NEGATIVE test") {
    //expect string OracleExp and string fmt
    import OracleExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("stats_date", DateType())
      val _IWregex = """[wW]""".r
      val _Mregex = """[mM]""".r
      val _Dregex = """[dD]""".r
      val _DAYregex = """[dD][aA][yY]""".r
      val _YRregex = """[Yy][Rr]""".r
      val inputs = Array("a", "ay", "yd", "yad", "YAD", "DAY", "DA", "SAY", "W", "AW", "dAy", "y", "d", "M", "AM", "WM", "g", "EE", "yR")
      for (input <- inputs) {
        if (_DAYregex.pattern.matcher(input).matches) {
          val col = OracleDerDimCol(s"$input", DateType(), GET_INTERVAL_DATE("{stats_date}", "DAY"))
          col.derivedExpression.render(col.name) should equal("TO_CHAR(stats_date, 'DAY')")
        }
        else if(_IWregex.pattern.matcher(input).matches) {
          val col = OracleDerDimCol(s"$input", DateType(), GET_INTERVAL_DATE("{stats_date}", "W"))
          col.derivedExpression.render(col.name) should equal(s"TRUNC(stats_date, 'IW')")
        }
        else if(_Mregex.pattern.matcher(input).matches) {
          val col = OracleDerDimCol(s"$input", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
          col.derivedExpression.render(col.name) should equal(s"TRUNC(stats_date, 'MM')")
        }
        else if(_Dregex.pattern.matcher(input).matches) {
          val col = OracleDerDimCol(s"$input", DateType(), GET_INTERVAL_DATE("{stats_date}", "D"))
          col.derivedExpression.render(col.name) should equal(s"TRUNC(stats_date)")
        }
        else if(_YRregex.pattern.matcher(input).matches) {
          val col = OracleDerDimCol(s"$input", DateType(), GET_INTERVAL_DATE("{stats_date}", "YR"))
          col.derivedExpression.render(col.name) should equal("TO_CHAR(stats_date, 'yyyy')")
        }
        else {
          assertThrows[IllegalArgumentException] {
            //println(s"$input FAILED")
            val col = OracleDerDimCol(s"$input", DateType(), GET_INTERVAL_DATE("{stats_date}", s"$input"))
            col.derivedExpression.render(col.name) should equal(s"TO_CHAR(stats_date, '$input')")
          }
        }
      }
    }
  }

  /* Presto Expression tests */

  test("successfully derive dependent columns from PrestoDerivedExpression") {
    import PrestoExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("created_date", IntType())
      val col = PrestoDerDimCol("Keyword Date Created", StrType(), PRESTO_TIMESTAMP_TO_FORMATTED_DATE("{created_date}", "YYYY-MM-dd"), annotations = Set())
      col.derivedExpression.sourceColumns.contains("created_date") should equal(true)
      col.derivedExpression.render(col.name) should equal("getDateFromEpoch(created_date, 'YYYY-MM-dd')")
    }
  }

  test("successfully derive dependent columns from PrestoShardingExpression") {
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent columns
      DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
      PrestoPartDimCol("shard", StrType(), annotations = Set())

      val col = PrestoShardingExpression("{shard} = PMOD({advertiser_id}, 31)")
      col.expression.sourceColumns.contains("shard") should equal(true)
      col.expression.sourceColumns.contains("advertiser_id") should equal(true)
      col.expression.render(col.expression.toString) should equal("shard = PMOD(advertiser_id, 31)")
    }
  }

  test("successfully derive dependent columns from PrestoDerivedExpressions when expandDerivedExpression is true and false") {
    import PrestoExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("clicks", IntType())
      DimCol("impressions", IntType())
      PrestoDerFactCol("De 1", DecType(), "{clicks}" * "1000")
      PrestoDerFactCol("De 2", DecType(), "{impressions}" * "1000")
      val col = PrestoDerFactCol("De 3", DecType(), "{De 1}" /- "{De 2}")
      col.derivedExpression.sourceColumns.contains("De 1") should equal(true)
      col.derivedExpression.sourceColumns.contains("De 2") should equal(true)
      col.derivedExpression.render(col.name, columnPrefix = Option("tableAlias."), expandDerivedExpression = false) should equal(
        """CASE WHEN tableAlias."De 2" = 0 THEN 0.0 ELSE tableAlias."De 1" / tableAlias."De 2" END"""
      )
      col.derivedExpression.render(col.name, columnPrefix = Option("tableAlias."), expandDerivedExpression = true) should equal(
        """CASE WHEN (tableAlias."impressions" * 1000) = 0 THEN 0.0 ELSE (tableAlias."clicks" * 1000) / (tableAlias."impressions" * 1000) END"""
      )

      val renderedColumnAliasMap : scala.collection.Map[String, String] = Map("De 1" -> """mang_de1""", "De 2" -> """mang_de2""")
      col.derivedExpression.render(col.name, renderedColumnAliasMap, expandDerivedExpression = false) should equal(
        """CASE WHEN mang_de2 = 0 THEN 0.0 ELSE mang_de1 / mang_de2 END"""
      )
      col.derivedExpression.render(col.name, renderedColumnAliasMap, expandDerivedExpression = true) should equal(
        """CASE WHEN (impressions * 1000) = 0 THEN 0.0 ELSE (clicks * 1000) / (impressions * 1000) END"""
      )
    }
  }

  test("Presto DAY_OF_WEEK test") {
    import PrestoExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      DimCol("stats_date", DateType())
      val col = PrestoDerDimCol("Day of Week", StrType(), DAY_OF_WEEK	("stats_date", "yyyyMMdd"))
      col.derivedExpression.render(col.name) should equal("from_unixtime(unix_timestamp(stats_date, 'yyyyMMdd'), 'EEEE')")
    }
  }

  test("Hive Column MAX/MIN test") {
    import HiveExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      FactCol("input_column", IntType())
      val minCol = HiveDerFactCol("Min Col", IntType(), MIN	("input_column"))
      val maxCol = HiveDerFactCol("Max Col", IntType(), MAX	("input_column"))
      minCol.derivedExpression.render(minCol.name) should equal("MIN(input_column)")
      maxCol.derivedExpression.render(maxCol.name) should equal("MAX(input_column)")
    }
  }

  test("Oracle Column MAX/MIN test") {
    import OracleExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      FactCol("input_column", IntType())
      val minCol = OracleDerFactCol("Min Col", IntType(), MIN	("input_column"))
      val maxCol = OracleDerFactCol("Max Col", IntType(), MAX	("input_column"))
      minCol.derivedExpression.render(minCol.name) should equal("MIN(input_column)")
      maxCol.derivedExpression.render(maxCol.name) should equal("MAX(input_column)")
    }
  }
}
