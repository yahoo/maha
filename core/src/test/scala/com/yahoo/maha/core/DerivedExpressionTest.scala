// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import com.yahoo.maha.core.BaseExpressionTest.{FACT_HIVE_EXPRESSION, PRESTO_TIMESTAMP_TO_FORMATTED_DATE, TIMESTAMP_TO_FORMATTED_DATE}
import com.yahoo.maha.core.DruidPostResultFunction.POST_RESULT_DECODE
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact._
import org.apache.druid.jackson.DefaultObjectMapper
import org.json4s.JsonAST.JObject
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Created by hiral on 10/13/15.
 */
class DerivedExpressionTest extends AnyFunSuite with Matchers {

  test("successfully derive dependent columns from HiveDerivedExpression") {
    import HiveExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("created_date", IntType())
      val col = HiveDerDimCol("Keyword Date Created", StrType(), TIMESTAMP_TO_FORMATTED_DATE("{created_date}", "YYYY-MM-dd"), annotations = Set())
      assert(TIMESTAMP_TO_FORMATTED_DATE("{created_date}", "YYYY-MM-dd").isUDF, "UDF hive expression should return true")
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

  test("successfully derive dependent columns from PostgresDerivedExpression") {
    import PostgresExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("created_date", IntType())

      val col = PostgresDerDimCol("Keyword Date Created", StrType(), FORMAT_DATE_WITH_LEAST("{created_date}", "YYYY-MM-DD"))
      col.derivedExpression.sourceColumns.contains("created_date") should equal(true)
      col.derivedExpression.render(col.name) should equal("COALESCE(TO_CHAR(LEAST(created_date,TO_DATE('01-Jan-3000','dd-Mon-YYYY')), 'YYYY-MM-DD'), 'NULL')")
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

  test("successfully derive dependent columns from PostgresDerivedExpression NVL") {
    import PostgresExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("created_date", IntType())

      val col = PostgresDerDimCol("Keyword Date Created", StrType(), NVL("{created_date}", "Default String"))
      col.derivedExpression.sourceColumns.contains("created_date") should equal(true)
    }
  }

  test("successfully derive dependent columns from BigqueryDerivedExpression NVL") {
    import BigqueryExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      DimCol("created_date", IntType())

      val col = BigqueryDerDimCol("Keyword Date Created", StrType(), NVL("{created_date}", "Default String"))
      col.derivedExpression.sourceColumns.contains("created_date") should equal(true)
    }
  }

  test("successfully derive dependent columns from OracleDerivedExpression TRUNC") {
    import OracleExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("created_date", IntType())

      val col = OracleDerDimCol("Keyword Date Created", StrType(), TRUNC("{created_date}"))
      col.derivedExpression.sourceColumns.contains("created_date") should equal(true)
    }
  }

  test("successfully derive dependent columns from PostgresDerivedExpression TRUNC") {
    import PostgresExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("created_date", IntType())

      val col = PostgresDerDimCol("Keyword Date Created", StrType(), TRUNC("{created_date}"))
      col.derivedExpression.sourceColumns.contains("created_date") should equal(true)
    }
  }

  test("successfully derive dependent columns from BigqueryDerivedExpression TRUNC") {
    import BigqueryExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      DimCol("created_date", IntType())

      val col = BigqueryDerDimCol("Keyword Date Created", StrType(), TRUNC("{created_date}"))
      col.derivedExpression.sourceColumns.contains("created_date") should equal(true)
    }
  }

  test("Should correctly display source columns when the source is also derived") {
    import DruidExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("clicks", IntType())
      DimCol("impressions", IntType())

      val col = DruidDerFactCol("BLAH", IntType(), "{clicks}" ++ "{impressions}")
      val col2 = DimCol("fakeDim", IntType(), alias = Option("account_id"))

      val anotherCol = DruidDerFactCol("newCol", IntType(), "{clicks}" ++ "{BLAH}")
      val anotherCol2 = FactCol("fakeMet", IntType(), DruidFilteredRollup(EqualityFilter("fakeDim", "1"), "newCol", SumRollup))

      assert(anotherCol.derivedExpression.sourceColumns.contains("clicks") && anotherCol.derivedExpression.sourceColumns.contains("BLAH"))
      assert(anotherCol.derivedExpression.sourcePrimitiveColumns.contains("clicks") && anotherCol.derivedExpression.sourcePrimitiveColumns.contains("impressions"))

      val sourceCols = anotherCol2.rollupExpression.sourceColumns
      val realSources2: Set[String] = anotherCol2.rollupExpression.sourceColumns.map(colName => {
        val col = anotherCol2.columnContext.getColumnByName(colName)
        if (!col.isDefined) {
          Set.empty
        } else if (!col.get.isDerivedColumn) {
          Set(col.get.alias.getOrElse(col.get.name))
        } else {
          col.get.asInstanceOf[DerivedColumn].derivedExpression.sourcePrimitiveColumns
        }
      }).flatten

      assert(sourceCols.contains("newCol") && sourceCols.contains("fakeDim"))
      assert(realSources2.contains("clicks") && realSources2.contains("impressions") && realSources2.contains("account_id"))

      val derColWithRollup = DruidDerFactCol("newCol2", IntType(), "{clicks}" ++ "{fakeMet}")

      val derRollupSources = derColWithRollup.derivedExpression.sourcePrimitiveColumns

      assert(derRollupSources.contains("impressions") && derRollupSources.contains("clicks") && derRollupSources.contains("account_id"))

    }
  }

  test("Should create a column from a tree of derivation") {
    import DruidExpression._
    ColumnContext.withColumnContext{
      implicit ctx: ColumnContext =>
        DimCol("clicks", IntType())
        FactCol("impressions", IntType())
        DimCol("account_id", IntType())
        DimCol("adv_id", IntType(), alias = Option("account_id"))

        FactCol("additive", IntType())

        DruidDerFactCol("derived_clicks_count", IntType(), "{clicks}" ++ "{impressions}")
        FactCol("rollup_clicks", IntType(), DruidFilteredRollup(EqualityFilter("adv_id", "10"), "derived_clicks_count", SumRollup))
        DruidDerFactCol("derived_rollup", IntType(), "{impressions}" ++ "{rollup_clicks}")
        FactCol("filtered_derived_filter", IntType(), DruidFilteredListRollup(List(EqualityFilter("impressions", "1"), EqualityFilter("rollup_clicks", "2")), "derived_rollup", SumRollup))
        DruidDerFactCol("mega_col", IntType(), "{additive}" ++ "{filtered_derived_filter}")
        val additiveRollup = FactCol("new_id", IntType(), DruidCustomRollup("{new_id}" ++ "{derived_rollup}"))
        val finalDerived = DruidDerFactCol("self_call", IntType(), "{self_call}" ++ "{new_id}")

        val finalSources: Set[String] = finalDerived.derivedExpression.sourcePrimitiveColumns
        val finalRollup: Set[String] = additiveRollup.rollupExpression.sourcePrimitiveColumns
        assert(finalSources.contains("impressions") && finalSources.contains("account_id") && finalSources.contains("clicks"))
        assert(finalRollup.contains("impressions") && finalRollup.contains("account_id") && finalRollup.contains("clicks"))
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

      json should equal("""{"type":"arithmetic","name":"BLAH","fn":"+","fields":[{"type":"fieldAccess","name":"clicks","fieldName":"Clicks"},{"type":"fieldAccess","name":"impressions","fieldName":"impressions"}],"ordering":null}""")

      val cc = new ColumnContext
      val cc2 = new ColumnContext
      val postResultCol = DruidPostResultDerivedFactCol("copyWithTest", IntType(), "{clicks}" ++ "{impressions}", postResultFunction = POST_RESULT_DECODE("{impressions}", "0", "N/A"))
      val postResultCopy = postResultCol.copyWith(cc, Map("copyWithTest" -> "copyWithResult"), true)
      val postResultCopyNoReset = postResultCol.copyWith(cc2, Map("copyWithTest" -> "copyWithResult"), false)
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

  test("successfully derive dependent columns from PostgresDerivedExpression with table alias") {
    import PostgresExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("clicks", IntType())
      DimCol("impressions", IntType())


      val col = PostgresDerFactCol("ctr", DecType(), "{clicks}" /- "{impressions}" * "1000")
      col.derivedExpression.sourceColumns.contains("clicks") should equal(true)
      col.derivedExpression.sourceColumns.contains("impressions") should equal(true)
      col.derivedExpression.render(col.name, columnPrefix = Option("tableAlias.")) should equal(
        """CASE WHEN tableAlias."impressions" = 0 THEN 0.0 ELSE tableAlias."clicks" / tableAlias."impressions" END * 1000""")



      val col2 = PostgresDerFactCol("ad_extn_spend", DecType(), DECODE("{ad_extn_spend}", """'\N'""", "NULL", "{ad_extn_spend}"))
      col2.derivedExpression.sourceColumns.contains("ad_extn_spend") should equal(true)
      col2.derivedExpression.render(col2.name, columnPrefix = Option("tableAlias.")) should equal(
        """CASE WHEN tableAlias."ad_extn_spend" = '\N' THEN NULL ELSE tableAlias."ad_extn_spend" END""")

    }
  }

  test("successfully derive dependent columns from BigqueryDerivedExpression with table alias") {
    import BigqueryExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      DimCol("clicks", IntType())
      DimCol("impressions", IntType())

      val col = BigqueryDerFactCol("ctr", DecType(), "{clicks}" /- "{impressions}" * "1000")
      col.derivedExpression.sourceColumns.contains("clicks") should equal(true)
      col.derivedExpression.sourceColumns.contains("impressions") should equal(true)
      col.derivedExpression.render(col.name, columnPrefix = Option("tableAlias.")) should equal(
        """CASE WHEN tableAlias."impressions" = 0 THEN 0.0 ELSE tableAlias."clicks" / tableAlias."impressions" END * 1000""")

      val col2 = BigqueryDerFactCol("ad_extn_spend", DecType(), DECODE("{ad_extn_spend}", """'\N'""", "NULL", "{ad_extn_spend}"))
      col2.derivedExpression.sourceColumns.contains("ad_extn_spend") should equal(true)
      col2.derivedExpression.render(col2.name, columnPrefix = Option("tableAlias.")) should equal(
        """CASE WHEN tableAlias."ad_extn_spend" = '\N' THEN NULL ELSE tableAlias."ad_extn_spend" END""")
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

  test("successfully derive dependent columns from PostgresDerivedExpressions when expandDerivedExpression is true and false") {
    import PostgresExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("clicks", IntType())
      DimCol("impressions", IntType())
      PostgresDerFactCol("De 1", DecType(), "{clicks}" * "1000")
      PostgresDerFactCol("De 2", DecType(), "{impressions}" * "1000")
      val col = PostgresDerFactCol("De 3", DecType(), "{De 1}" /- "{De 2}")
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

  test("successfully derive dependent columns from BigqueryDerivedExpressions when expandDerivedExpression is true and false") {
    import BigqueryExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      DimCol("clicks", IntType())
      DimCol("impressions", IntType())
      BigqueryDerFactCol("De 1", DecType(), "{clicks}" * "1000")
      BigqueryDerFactCol("De 2", DecType(), "{impressions}" * "1000")
      val col = BigqueryDerFactCol("De 3", DecType(), "{De 1}" /- "{De 2}")
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

  test("successfully derive dependent columns from PostgresDerivedExpression IS_ALERT_ELIGIBLE") {
    import PostgresExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("recommended_bid", DecType())
      DimCol("native_bid", DecType())

      val col = PostgresDerDimCol("alert_eligible", StrType(),
        COMPARE_PERCENTAGE("{native_bid}", "{recommended_bid}", 95, "ADGROUP_LOW_BID",
          COMPARE_PERCENTAGE("{recommended_bid}", "{native_bid}", 100, "CAMPAIGN_BUDGET_CAP", "'NA'")))

      col.derivedExpression.sourceColumns.contains("recommended_bid") should equal(true)
      col.derivedExpression.sourceColumns.contains("native_bid") should equal(true)
      val result = col.derivedExpression.render(col.name, columnPrefix = Option("tableAlias."))
      result should equal(
        """CASE WHEN tableAlias."native_bid" < 0.95 * tableAlias."recommended_bid" THEN 'ADGROUP_LOW_BID'  WHEN tableAlias."recommended_bid" < 1.0 * tableAlias."native_bid" THEN 'CAMPAIGN_BUDGET_CAP' ELSE 'NA' END """)

    }
  }

  test("successfully derive dependent columns from BigqueryDerivedExpression IS_ALERT_ELIGIBLE") {
    import BigqueryExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      DimCol("recommended_bid", DecType())
      DimCol("native_bid", DecType())

      val col = BigqueryDerDimCol("alert_eligible", StrType(),
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

  test("REGEX_EXTRACT test") {
    import HiveExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("stats_date", DateType())
      val col1 = HiveDerDimCol("Click Exp ID", StrType(), REGEX_EXTRACT("internal_bucket_id", "(cl-)(.*?)(,|$)", 2, replaceMissingValue = true, "-3"))
      col1.derivedExpression.render(col1.name) should equal("CASE WHEN LENGTH(regexp_extract(internal_bucket_id, '(cl-)(.*?)(,|$)', 2)) > 0 THEN regexp_extract(internal_bucket_id, '(cl-)(.*?)(,|$)', 2) ELSE '-3' END")
      val col2 = HiveDerDimCol("Default Exp ID", StrType(), REGEX_EXTRACT("internal_bucket_id", "(df-)(.*?)(,|$)", 2, replaceMissingValue = false, ""))
      col2.derivedExpression.render(col2.name) should equal("regexp_extract(internal_bucket_id, '(df-)(.*?)(,|$)', 2)")
    }
  }

  test("Oracle GET_INTERVAL_DATE NEGATIVE test") {
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
            //
            val col = OracleDerDimCol(s"$input", DateType(), GET_INTERVAL_DATE("{stats_date}", s"$input"))
            col.derivedExpression.render(col.name) should equal(s"TO_CHAR(stats_date, '$input')")
          }
        }
      }
    }
  }

  test("Postgres GET_INTERVAL_DATE NEGATIVE test") {
    //expect string PostgresExp and string fmt
    import PostgresExpression._
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
          val col = PostgresDerDimCol(s"$input", DateType(), GET_INTERVAL_DATE("{stats_date}", "DAY"))
          col.derivedExpression.render(col.name) should equal("TO_CHAR(stats_date, 'DAY')")
        }
        else if(_IWregex.pattern.matcher(input).matches) {
          val col = PostgresDerDimCol(s"$input", DateType(), GET_INTERVAL_DATE("{stats_date}", "W"))
          col.derivedExpression.render(col.name) should equal(s"DATE_TRUNC('week', stats_date)::DATE")
        }
        else if(_Mregex.pattern.matcher(input).matches) {
          val col = PostgresDerDimCol(s"$input", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
          col.derivedExpression.render(col.name) should equal(s"DATE_TRUNC('month', stats_date)::DATE")
        }
        else if(_Dregex.pattern.matcher(input).matches) {
          val col = PostgresDerDimCol(s"$input", DateType(), GET_INTERVAL_DATE("{stats_date}", "D"))
          col.derivedExpression.render(col.name) should equal(s"DATE_TRUNC('day', stats_date)::DATE")
        }
        else if(_YRregex.pattern.matcher(input).matches) {
          val col = PostgresDerDimCol(s"$input", DateType(), GET_INTERVAL_DATE("{stats_date}", "YR"))
          col.derivedExpression.render(col.name) should equal("TO_CHAR(stats_date, 'YYYY')")
        }
        else {
          assertThrows[IllegalArgumentException] {
            //
            val col = PostgresDerDimCol(s"$input", DateType(), GET_INTERVAL_DATE("{stats_date}", s"$input"))
            col.derivedExpression.render(col.name) should equal(s"TO_CHAR(stats_date, '$input')")
          }
        }
      }
    }
  }

  test("Bigquery GET_INTERVAL_DATE NEGATIVE test") {
    import BigqueryExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      DimCol("stats_date", DateType())
      val _IWregex = """[wW]""".r
      val _Mregex = """[mM]""".r
      val _Dregex = """[dD]""".r
      val _DAYregex = """[dD][aA][yY]""".r
      val _YRregex = """[Yy][Rr]""".r
      val inputs = Array("a", "ay", "yd", "yad", "YAD", "DAY", "DA", "SAY", "W", "AW", "dAy", "y", "d", "M", "AM", "WM", "g", "EE", "yR")
      for (input <- inputs) {
        if (_DAYregex.pattern.matcher(input).matches) {
          val col = BigqueryDerDimCol(s"$input", DateType(), GET_INTERVAL_DATE("{stats_date}", "DAY"))
          col.derivedExpression.render(col.name) should equal(s"EXTRACT(DAY FROM stats_date)")
        }
        else if (_IWregex.pattern.matcher(input).matches) {
          val col = BigqueryDerDimCol(s"$input", DateType(), GET_INTERVAL_DATE("{stats_date}", "W"))
          col.derivedExpression.render(col.name) should equal(s"DATE_TRUNC(stats_date, WEEK)")
        }
        else if (_Mregex.pattern.matcher(input).matches) {
          val col = BigqueryDerDimCol(s"$input", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
          col.derivedExpression.render(col.name) should equal(s"DATE_TRUNC(stats_date, MONTH)")
        }
        else if (_Dregex.pattern.matcher(input).matches) {
          val col = BigqueryDerDimCol(s"$input", DateType(), GET_INTERVAL_DATE("{stats_date}", "D"))
          col.derivedExpression.render(col.name) should equal(s"DATE_TRUNC(stats_date, DAY)")
        }
        else if (_YRregex.pattern.matcher(input).matches) {
          val col = BigqueryDerDimCol(s"$input", DateType(), GET_INTERVAL_DATE("{stats_date}", "YR"))
          col.derivedExpression.render(col.name) should equal(s"EXTRACT(YEAR FROM stats_date)")
        }
        else {
          assertThrows[IllegalArgumentException] {
            val col = BigqueryDerDimCol(s"$input", DateType(), GET_INTERVAL_DATE("{stats_date}", s"$input"))
            col.derivedExpression.render(col.name) should equal(s"DATE_TRUNC(stats_date, $input)")
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
      assert(PRESTO_TIMESTAMP_TO_FORMATTED_DATE("{created_date}", "YYYY-MM-dd").isUDF, "Presto timestamp UDF should return true")
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
      PrestoDerFactCol("De 11", DecType(), "{impressions}" * "1000" * "1")
      PrestoDerFactCol("De 12", DecType(), "{impressions}" * "1000" / "1")
      FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), PrestoCustomRollup(SUM("{avg_pos}" * "{impressions}") * SUM("{impressions}")))
      FactCol("avg_pos2", DecType(3, "0.0", "0.1", "500"), PrestoCustomRollup(SUM("{avg_pos2}" * "{impressions}") / SUM("{impressions}")))
      FactCol("avg_pos3", DecType(3, "0.0", "0.1", "500"), PrestoCustomRollup(SUM("{avg_pos3}" * "{impressions}") ++ SUM("{impressions}")))
      val col = PrestoDerFactCol("De 3", DecType(), "{De 1}" /- "{De 2}")
      col.derivedExpression.sourceColumns.contains("De 1") should equal(true)
      col.derivedExpression.sourceColumns.contains("De 2") should equal(true)
      col.derivedExpression.render(col.name, columnPrefix = Option("tableAlias."), expandDerivedExpression = false) should equal(
        """CASE WHEN tableAlias."De 2" = 0 THEN 0.0 ELSE CAST(tableAlias."De 1" AS DOUBLE) / tableAlias."De 2" END"""
      )
      col.derivedExpression.render(col.name, columnPrefix = Option("tableAlias."), expandDerivedExpression = true) should equal(
        """CASE WHEN (tableAlias."impressions" * 1000) = 0 THEN 0.0 ELSE CAST((tableAlias."clicks" * 1000) AS DOUBLE) / (tableAlias."impressions" * 1000) END"""
      )

      val renderedColumnAliasMap : scala.collection.Map[String, String] = Map("De 1" -> """mang_de1""", "De 2" -> """mang_de2""")
      col.derivedExpression.render(col.name, renderedColumnAliasMap, expandDerivedExpression = false) should equal(
        """CASE WHEN mang_de2 = 0 THEN 0.0 ELSE CAST(mang_de1 AS DOUBLE) / mang_de2 END"""
      )
      col.derivedExpression.render(col.name, renderedColumnAliasMap, expandDerivedExpression = true) should equal(
        """CASE WHEN (impressions * 1000) = 0 THEN 0.0 ELSE CAST((clicks * 1000) AS DOUBLE) / (impressions * 1000) END"""
      )
    }
  }

  test("Presto DAY_OF_WEEK test") {
    import PrestoExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      DimCol("stats_date", DateType())
      val col = PrestoDerDimCol("Day of Week", StrType(), DAY_OF_WEEK	("stats_date", "%Y%m%d"))
      col.derivedExpression.render(col.name) should equal("date_format(date_parse(stats_date, '%Y%m%d'), '%W')")
    }
    assert(!SUM("stats_date").isUDF, "Presto Expr is not supposed to be a UDF")
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
    assert(!SUM("stats_date").isUDF, "Hive Expr is not supposed to be a UDF")
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

  test("Postgres Column MAX/MIN test") {
    import PostgresExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      FactCol("input_column", IntType())
      val minCol = PostgresDerFactCol("Min Col", IntType(), MIN	("input_column"))
      val maxCol = PostgresDerFactCol("Max Col", IntType(), MAX	("input_column"))
      minCol.derivedExpression.render(minCol.name) should equal("MIN(input_column)")
      maxCol.derivedExpression.render(maxCol.name) should equal("MAX(input_column)")
    }
  }

  test("Bigquery Column MAX/MIN test") {
    import BigqueryExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      FactCol("input_column", IntType())
      val minCol = BigqueryDerFactCol("Min Col", IntType(), MIN	("input_column"))
      val maxCol = BigqueryDerFactCol("Max Col", IntType(), MAX	("input_column"))
      minCol.derivedExpression.render(minCol.name) should equal("MIN(input_column)")
      maxCol.derivedExpression.render(maxCol.name) should equal("MAX(input_column)")
    }
  }

  test("Create oracle NVL and parse parameters") {
    import OracleExpression._
    implicit val cc = new ColumnContext
    val nvlVal = NVL("{col_name}", "{default_str}")
    assert(!nvlVal.hasRollupExpression)
    assert(!nvlVal.hasNumericOperation)
    assert(nvlVal.asString.contains("col_name"))
  }

  test("Create Postgres NVL and parse parameters") {
    import PostgresExpression._
    val nvlVal = NVL("{col_name}", "{default_str}")
    assert(!nvlVal.hasRollupExpression)
    assert(!nvlVal.hasNumericOperation)
    assert(nvlVal.asString.contains("col_name"))
  }

  test("Create Bigquery NVL and parse parameters") {
    import BigqueryExpression._
    val nvlVal = NVL("{col_name}", "{default_str}")
    assert(!nvlVal.hasRollupExpression)
    assert(!nvlVal.hasNumericOperation)
    assert(nvlVal.asString.contains("col_name"))
  }

  test("Create hive NVL and parse parameters") {
    import HiveExpression._
    val nvlVal = NVL("{col_name}", "{default_str}")
    assert(!nvlVal.hasRollupExpression)
    assert(!nvlVal.hasNumericOperation)
    assert(nvlVal.asString.contains("col_name"))
  }

  test("Create presto NVL and parse parameters") {
    import PrestoExpression._
    val nvlVal = NVL("{col_name}", "{default_str}")
    assert(!nvlVal.hasRollupExpression)
    assert(!nvlVal.hasNumericOperation)
    assert(nvlVal.asString.contains("col_name"))
  }

  test("Create oracle TRUNC and parse parameters") {
    import OracleExpression._
    val truncVal = TRUNC("{col_name}")
    assert(!truncVal.hasRollupExpression)
    assert(!truncVal.hasNumericOperation)
    assert(truncVal.asString.contains("col_name"))
  }

  test("Create Postgres TRUNC and parse parameters") {
    import PostgresExpression._
    val truncVal = TRUNC("{col_name}")
    assert(!truncVal.hasRollupExpression)
    assert(!truncVal.hasNumericOperation)
    assert(truncVal.asString.contains("col_name"))
  }

  test("Create Bigquery TRUNC and parse parameters") {
    import BigqueryExpression._
    val truncVal = TRUNC("{col_name}")
    assert(!truncVal.hasRollupExpression)
    assert(!truncVal.hasNumericOperation)
    assert(truncVal.asString.contains("col_name"))
  }

  test("Create oracle COALESCE and parse parameters") {
    import OracleExpression._
    val coalesceVal = COALESCE("{col_name}", "''")
    assert(!coalesceVal.hasRollupExpression)
    assert(!coalesceVal.hasNumericOperation)
    assert(coalesceVal.asString.contains("col_name"))
  }

  test("Create Postgres COALESCE and parse parameters") {
    import PostgresExpression._
    val coalesceVal = COALESCE("{col_name}", "''")
    assert(!coalesceVal.hasRollupExpression)
    assert(!coalesceVal.hasNumericOperation)
    assert(coalesceVal.asString.contains("col_name"))
  }

  test("Create Bigquery COALESCE and parse parameters") {
    import BigqueryExpression._
    val coalesceVal = COALESCE("{col_name}", "''")
    assert(!coalesceVal.hasRollupExpression)
    assert(!coalesceVal.hasNumericOperation)
    assert(coalesceVal.asString.contains("col_name"))
  }

  test("Create hive COALESCE and parse parameters") {
    import HiveExpression._
    val coalesceVal = COALESCE("{col_name}", "''")
    assert(!coalesceVal.hasRollupExpression)
    assert(!coalesceVal.hasNumericOperation)
    assert(coalesceVal.asString.contains("col_name"))
  }

  test("Create presto COALESCE and parse parameters") {
    import PrestoExpression._
    val coalesceVal = COALESCE("{col_name}", "''")
    assert(!coalesceVal.hasRollupExpression)
    assert(!coalesceVal.hasNumericOperation)
    assert(coalesceVal.asString.contains("col_name"))
  }

  test("Create oracle TO_CHAR and parse parameters") {
    import OracleExpression._
    val tocharVal = TO_CHAR("{col_name}", "''")
    assert(!tocharVal.hasRollupExpression)
    assert(!tocharVal.hasNumericOperation)
    assert(tocharVal.asString.contains("col_name"))
  }

  test("Create Postgres TO_CHAR and parse parameters") {
    import PostgresExpression._
    val tocharVal = TO_CHAR("{col_name}", "''")
    assert(!tocharVal.hasRollupExpression)
    assert(!tocharVal.hasNumericOperation)
    assert(tocharVal.asString.contains("col_name"))
  }

  test("Create oracle ROUND and parse parameters") {
    import OracleExpression._
    val roundVal = ROUND("{col_name}", 1)
    assert(!roundVal.hasRollupExpression)
    assert(!roundVal.hasNumericOperation)
    assert(roundVal.asString.contains("col_name"))
  }

  test("Create Postgres ROUND and parse parameters") {
    import PostgresExpression._
    val roundVal = ROUND("{col_name}", 1)
    assert(!roundVal.hasRollupExpression)
    assert(!roundVal.hasNumericOperation)
    assert(roundVal.asString.contains("col_name"))
  }

  test("Create Bigquery ROUND and parse parameters") {
    import BigqueryExpression._
    val roundVal = ROUND("{col_name}", 1)
    assert(!roundVal.hasRollupExpression)
    assert(!roundVal.hasNumericOperation)
    assert(roundVal.asString.contains("col_name"))
  }

  test("Create hive ROUND and parse parameters") {
    import HiveExpression._
    val roundVal = ROUND("{col_name}", 1)
    assert(!roundVal.hasRollupExpression)
    assert(!roundVal.hasNumericOperation)
    assert(roundVal.asString.contains("col_name"))
  }

  test("Create presto ROUND and parse parameters") {
    import PrestoExpression._
    val roundVal = ROUND("{col_name}", 1)
    assert(!roundVal.hasRollupExpression)
    assert(!roundVal.hasNumericOperation)
    assert(roundVal.asString.contains("col_name"))
  }

  test("Create presto TRIM and parse parameters") {
    import PrestoExpression._
    val roundVal = TRIM("{col_name}")
    assert(!roundVal.hasRollupExpression)
    assert(!roundVal.hasNumericOperation)
    assert(roundVal.asString.contains("col_name"))
  }

  test("Create presto MAX and parse parameters") {
    import PrestoExpression._
    val roundVal = MAX("{col_name}")
    assert(roundVal.hasRollupExpression)
    assert(roundVal.hasNumericOperation)
    assert(roundVal.asString.contains("col_name"))
  }

  test("Presto REGEX_EXTRACT test") {
    import PrestoExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      //register dependent column
      DimCol("stats_date", DateType())
      val col1 = PrestoDerDimCol("Click Exp ID", StrType(), REGEX_EXTRACT("internal_bucket_id", "(cl-)(.*?)(,|$)", 2, replaceMissingValue = true, "-3"))
      col1.derivedExpression.render(col1.name) should equal("CASE WHEN LENGTH(regexp_extract(internal_bucket_id, '(cl-)(.*?)(,|$)', 2)) > 0 THEN regexp_extract(internal_bucket_id, '(cl-)(.*?)(,|$)', 2) ELSE '-3' END")
      val col2 = PrestoDerDimCol("Default Exp ID", StrType(), REGEX_EXTRACT("internal_bucket_id", "(df-)(.*?)(,|$)", 2, replaceMissingValue = false, ""))
      col2.derivedExpression.render(col2.name) should equal("regexp_extract(internal_bucket_id, '(df-)(.*?)(,|$)', 2)")
    }
  }

  test("All column types with all expressions, dataTypes, & rollups should render JSON properly.") {
    import com.yahoo.maha.core.DruidDerivedFunction._
    import com.yahoo.maha.core.DruidPostResultFunction._
    ColumnContext.withColumnContext {
      implicit cc: ColumnContext =>
        import DruidExpression._
        DimCol("clicks", IntType())
        FactCol("impressions", IntType())
        DimCol("account_id", IntType())
        DimCol("adv_id", IntType(), alias = Option("account_id"))
        DimCol("date", DateType("yyyyMMdd"))

        FactCol("additive", IntType())

        DruidDerFactCol("derived_clicks_count", IntType(), "{clicks}" ++ "{impressions}")
        FactCol("rollup_clicks", IntType(), DruidFilteredRollup(EqualityFilter("adv_id", "10"), "derived_clicks_count", SumRollup))
        DruidDerFactCol("derived_rollup", IntType(), "{impressions}" ++ "{rollup_clicks}")
        FactCol("filtered_derived_filter", IntType(), DruidFilteredListRollup(List(EqualityFilter("impressions", "1"), EqualityFilter("rollup_clicks", "2")), "derived_rollup", SumRollup))
        DruidDerFactCol("mega_col", IntType(), "{additive}" ++ "{filtered_derived_filter}")
        val additiveRollup = FactCol("new_id", IntType(), DruidCustomRollup("{new_id}" ++ "{derived_rollup}"))
        val finalDerived = DruidDerFactCol("self_call", IntType(3, (Map(1 -> "2"), "1")), "{self_call}" ++ "{new_id}")
        val strCol = DimCol("blue_id", StrType())
        val dateCol = DruidFuncDimCol("date_id", DateType("yyyyMMdd"), DATETIME_FORMATTER("{date}", 0, 3))
        val tsCol = DimCol("ts_id", TimestampType())
        val passthroughCol = DimCol("pt_col", PassthroughType())
        val prCol = DruidPostResultFuncDimCol("pr_col", IntType(), postResultFunction = START_OF_THE_WEEK("{date_id}"))

        val allExpectedStrings: List[String] = List(
          s"""{"DimCol":{"DimensionColumn":{"name":"blue_id","alias":"","dataType":{"StrType":{"jsonDataType":"String","constraint":"None","hasStaticMapping":false,"hasUniqueStaticMapping":false,"reverseStaticMapping":"Map()"},"length":0,"staticMapping":null,"default":""},"annotations":"Set()","filterOperationOverrides":"Set()","columnContext":"""", """"},"isForeignKey":false},"name":"blue_id","dataType":{"StrType":{"jsonDataType":"String","constraint":"None","hasStaticMapping":false,"hasUniqueStaticMapping":false,"reverseStaticMapping":"Map()"},"length":0,"staticMapping":null,"default":""},"aliasOrName":"","annotations":"Set()","filterOperationOverrides":"Set()"}"""
          ,s"""{"DerivedFunctionColumn":{"DimensionColumn":{"name":"date_id","alias":"","dataType":{"DateType":{"jsonDataType":"Date","constraint":"yyyyMMdd","hasStaticMapping":false,"hasUniqueStaticMapping":false,"reverseStaticMapping":"Map()"},"format":"yyyyMMdd"},"annotations":"Set()","filterOperationOverrides":"Set()","columnContext":"""", """"},"isForeignKey":false},"derivedFunction":{"function_type":"DATETIME_FORMATTER","fieldName":"{date}","index":0,"length":3}}"""
          ,s"""{"DimCol":{"DimensionColumn":{"name":"pt_col","alias":"","dataType":{"PassthroughType":{"jsonDataType":"Null","constraint":"None","hasStaticMapping":false,"hasUniqueStaticMapping":false,"reverseStaticMapping":"Map()"},"format":"None"},"annotations":"Set()","filterOperationOverrides":"Set()","columnContext":"""", """"},"isForeignKey":false},"name":"pt_col","dataType":{"PassthroughType":{"jsonDataType":"Null","constraint":"None","hasStaticMapping":false,"hasUniqueStaticMapping":false,"reverseStaticMapping":"Map()"},"format":"None"},"aliasOrName":"","annotations":"Set()","filterOperationOverrides":"Set()"}"""
          ,s"""{"FactCol":{"FactColumn":{"name":"new_id","alias":"","dataType":{"IntType":{"jsonDataType":"Number","constraint":"None","hasStaticMapping":false,"hasUniqueStaticMapping":false,"reverseStaticMapping":"Map()"},"length":0,"staticMapping":null,"default":-1,"min":-1,"max":-1},"annotations":"Set()","filterOperationOverrides":"Set()","columnContext":"""", """"},"hasRollupWithEngineRequirement":true},"name":"new_id","dataType":{"IntType":{"jsonDataType":"Number","constraint":"None","hasStaticMapping":false,"hasUniqueStaticMapping":false,"reverseStaticMapping":"Map()"},"length":0,"staticMapping":null,"default":-1,"min":-1,"max":-1},"rollupExpression":{"expressionName":"DruidCustomRollup","hasDerivedExpression":true,"sourcePrimitiveColumns":"Set(impressions, account_id, clicks)"},"aliasOrName":"","annotations":"Set()","filterOperationOverrides":"Set()"}"""
          ,s"""{"PostResultColumn":{"DimensionColumn":{"name":"pr_col","alias":"","dataType":{"IntType":{"jsonDataType":"Number","constraint":"None","hasStaticMapping":false,"hasUniqueStaticMapping":false,"reverseStaticMapping":"Map()"},"length":0,"staticMapping":null,"default":-1,"min":-1,"max":-1},"annotations":"Set()","filterOperationOverrides":"Set()","columnContext":"""", """"},"isForeignKey":false},"postResultFunction":{"postResultFunction":"START_OF_THE_WEEK","expression":"{date_id}"}}"""
          ,s"""{"DimCol":{"DimensionColumn":{"name":"ts_id","alias":"","dataType":{"TimestampType":{"jsonDataType":"Date","constraint":"None","hasStaticMapping":false,"hasUniqueStaticMapping":false,"reverseStaticMapping":"Map()"},"format":"None"},"annotations":"Set()","filterOperationOverrides":"Set()","columnContext":"""", """"},"isForeignKey":false},"name":"ts_id","dataType":{"TimestampType":{"jsonDataType":"Date","constraint":"None","hasStaticMapping":false,"hasUniqueStaticMapping":false,"reverseStaticMapping":"Map()"},"format":"None"},"aliasOrName":"","annotations":"Set()","filterOperationOverrides":"Set()"}"""
          ,s"""{"DerivedColumn":{"FactColumn":{"name":"self_call","alias":"","dataType":{"IntType":{"jsonDataType":"Enum","constraint":"2","hasStaticMapping":true,"hasUniqueStaticMapping":true,"reverseStaticMapping":"Map(2 -> Set(1))"},"length":3,"staticMapping":{"tToStringMap":"1 -> 2","default":"1"},"default":-1,"min":-1,"max":-1},"annotations":"Set()","filterOperationOverrides":"Set()","columnContext":"""", """"},"hasRollupWithEngineRequirement":false},"derivedExpression":{"expression":{"expression":"Arithmetic","hasNumericOperation":true,"hasRollupExpression":false},"sourcePrimitiveColumns":"Set(impressions, account_id, clicks)"}}"""
        )

        val allCols: Set[Column] = Set(additiveRollup, finalDerived, strCol, dateCol, tsCol, passthroughCol, prCol)

        val allJSONs: Set[JObject] = allCols.map(col => col.asJSON)

        import org.json4s._
        import org.json4s.jackson.JsonMethods._
        implicit val formats = DefaultFormats
        val allCompactJSONs: String = allJSONs.map(col => compact(col)).mkString(",")
        //println(allCompactJSONs.split(",").mkString("\n,"))
        assert(allExpectedStrings.forall(json => allCompactJSONs.contains(json)))
    }
  }
}
