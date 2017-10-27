// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core.DruidDerivedFunction.GET_INTERVAL_DATE
import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core.dimension.{ConstDimCol, DimCol, DruidFuncDimCol, PubCol}
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core._
import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.lookup.LongRangeLookup
import com.yahoo.maha.core.registry.RegistryBuilder
import com.yahoo.maha.core.request.AsyncRequest

/**
 * Created by hiral on 2/18/16.
 */
trait BaseQueryContextTest {
  this: BaseQueryGeneratorTest =>

  CoreSchema.register()

  val dimOnlyQueryJson = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Campaign Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Campaign Status", "order": "ASC"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

  val factOnlyQueryJson = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Day"},
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Ad Group ID"},
                              {"field": "Source"},
                              {"field": "Pricing Type"},
                              {"field": "Destination URL"},
                              {"field": "Impressions"},
                              {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Clicks", "order": "ASC"}
                          ],
                          "forceDimensionDriven": false,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""
  val combinedQueryJson = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Day"},
                              {"field": "Advertiser ID"},
                              {"field": "Ad Group Status"},
                              {"field": "Ad Group ID"},
                              {"field": "Source"},
                              {"field": "Pricing Type"},
                              {"field": "Destination URL"},
                              {"field": "Impressions"},
                              {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "ASC"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

  val adjustmentViewJson = s"""{ "cube": "a_stats",
                              |   "selectFields": [
                              |      {
                              |         "field": "Advertiser ID"
                              |      },
                              |      {
                              |         "field": "Day"
                              |      },
                              |      {
                              |         "field": "Is Adjustment"
                              |      },
                              |      {
                              |         "field": "Impressions"
                              |      },
                              |      {
                              |         "field": "Spend"
                              |      }
                              |   ],
                              |   "filterExpressions": [
                              |      {
                              |         "field": "Advertiser ID",
                              |         "operator": "=",
                              |         "value": "1035663"
                              |      },
                              |      {
                              |         "field": "Is Adjustment",
                              |         "operator": "=",
                              |         "value": "Y"
                              |      },
                              |      {
                              |         "field": "Day",
                              |         "operator": "Between",
                              |         "from": "$fromDate",
                              |         "to": "$toDate"
                              |      }
                              |   ]
                              |}
      """.stripMargin

  def pubfactAdjustmentStatsView(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {

    val tableOne  = {
      ColumnContext.withColumnContext {
        implicit dc: ColumnContext =>
          Fact.newFactForView(
            "account_stats", DailyGrain, DruidEngine, Set(AdvertiserSchema, ResellerSchema),
            Set(
              DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
              , ConstDimCol("is_adjustment", StrType(1), "N")
              , DimCol("stats_date", DateType("YYYY-MM-DD"))
              , DruidFuncDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
              , DruidFuncDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "w"))
            ),
            Set(
              ConstFactCol("CTR", IntType(3, 1), "0")
              ,FactCol("impressions", IntType(3, 1))
              , FactCol("clicks", IntType(3, 0, 1, 800))
              , FactCol("spend", DecType(0, "0.0"))
            )
          )
      }
    }

    val tableTwo  = {
      ColumnContext.withColumnContext {
        implicit dc: ColumnContext =>
          Fact.newFactForView(
            "account_adjustments", DailyGrain, DruidEngine, Set(AdvertiserSchema, ResellerSchema),
            Set(
              DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
              , ConstDimCol("is_adjustment", StrType(1), "Y")
              , DimCol("stats_date", DateType("YYYY-MM-DD"))
              , DruidFuncDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
              , DruidFuncDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "w"))
            ),
            Set(
              ConstFactCol("CTR", IntType(3, 1), "0")
              , FactCol("impressions", IntType(3, 1))
              , FactCol("clicks", IntType(3, 0, 1, 800))
              , FactCol("spend", DecType(0, "0.0"))
            )
          )
      }
    }
    val view = UnionView("account_a_stats", Seq(tableOne, tableTwo))


    ColumnContext.withColumnContext {
      implicit dc: ColumnContext =>
        Fact.newUnionView(view, DailyGrain, DruidEngine, Set(AdvertiserSchema, ResellerSchema),
          Set(
            DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("stats_date", DateType("YYYY-MM-DD"))
            , DimCol("is_adjustment", StrType(1))
            , DruidFuncDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
            , DruidFuncDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "w"))
          ),
          Set(
            FactCol("CTR", IntType(3, 1))
            , FactCol("impressions", IntType(3, 1))
            , FactCol("clicks", IntType(3, 0, 1, 800))
            , FactCol("spend", DecType(0, "0.0"))
          )
        )
    }
      .toPublicFact("a_stats",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("is_adjustment", "Is Adjustment", Equality),
          PubCol("Month", "Month", Equality),
          PubCol("Week", "Week", Equality)
        ),
        Set(
          PublicFactCol("CTR", "CTR", InBetweenEquality),
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("spend", "Spend", Set.empty)
        ), Set(),  getMaxDaysWindow, getMaxDaysLookBack
      )
  }
  
  override protected[this] def registerFacts(forcedFilters: Set[ForcedFilter], registryBuilder: RegistryBuilder): Unit = {
    val builder = ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      import HiveExpression._
      Fact.newFact(
        "fact_hive", DailyGrain, HiveEngine, Set(AdvertiserSchema)
        , Set(
          DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("country_woeid", IntType(), annotations = Set(ForeignKey("woeid")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("start_time", IntType())
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("stats_date", DateType("YYYY-MM-dd"))
          , DimCol("column_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned")))
          , DimCol("column2_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned_with_singleton")))

        )
        , Set(
          FactCol("impressions", IntType(3, 0))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          , HiveDerFactCol("Average CPC", DecType(), "{spend}" / "{clicks}")
          , HiveDerFactCol("CTR", DecType(), "{clicks}" /- "{impressions}")
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), HiveCustomRollup(SUM("{avg_pos}" * "{impressions}") /- "{impressions}"))
        )
        , annotations = Set()
        , forceFilters = Set(ForceFilter(EqualityFilter("Source", "2", isForceFilter = true)))
        , costMultiplierMap = Map(AsyncRequest -> CostMultiplier(LongRangeLookup.full(3)))
      )
    }

    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      import OracleExpression._
      builder.withAlternativeEngine(
        "fact_oracle", "fact_hive", OracleEngine
        , overrideDimCols = Set(
          DimCol("stats_date", DateType("YYYY-MM-DD"))
        )
        , overrideFactCols = Set(
          FactCol("impressions", IntType(3, 1))
          , OracleDerFactCol("Average CPC", DecType(), "{spend}" / "{clicks}")
          , OracleDerFactCol("CTR", DecType(), "{clicks}" /- "{impressions}")
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), OracleCustomRollup(SUM("{avg_pos}" * "{impressions}") /- "{impressions}"))
        )
        , costMultiplierMap = Map(AsyncRequest -> CostMultiplier(LongRangeLookup.full(2)))
        , forceFilters = Set(ForceFilter(EqualityFilter("Source", "2", isForceFilter = true)))
      )
    }

    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      import DruidExpression._
      builder.withAlternativeEngine(
        "fact_druid", "fact_hive", DruidEngine,
        overrideFactCols = Set(
          FactCol("impressions", IntType(3, 1))
          , DruidDerFactCol("Average CPC", DecType(), "{spend}" / "{clicks}")
          , DruidDerFactCol("CTR", DecType(), "{clicks}" /- "{impressions}")
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), DruidCustomRollup("{avg_pos}" * "{impressions}" /- "{impressions}"))
        )
        , costMultiplierMap = Map(AsyncRequest -> CostMultiplier(LongRangeLookup.full(1)))
      )
    }


    val cube = builder.toPublicFact("k_stats",
      Set(
        PubCol("stats_date", "Day", InBetweenEquality),
        PubCol("ad_group_id", "Ad Group ID", InEquality),
        PubCol("campaign_id", "Campaign ID", InEquality),
        PubCol("advertiser_id", "Advertiser ID", InEquality),
        PubCol("country_woeid", "Country WOEID", InEquality),
        PubCol("stats_source", "Source", Equality),
        PubCol("price_type", "Pricing Type", In),
        PubCol("landing_page_url", "Destination URL", Set.empty),
        PubCol("column_id", "Column ID", Equality),
        PubCol("column2_id", "Column2 ID", Equality)
        //PubCol("Ad Group Start Date Full", "Ad Group Start Date Full", InEquality)
      ),
      Set(
        PublicFactCol("impressions", "Impressions", InBetweenEquality),
        PublicFactCol("clicks", "Clicks", InBetweenEquality),
        PublicFactCol("spend", "Spend", Set.empty),
        PublicFactCol("avg_pos", "Average Position", Set.empty),
        PublicFactCol("max_bid", "Max Bid", Set.empty),
        PublicFactCol("Average CPC", "Average CPC", InBetweenEquality),
        PublicFactCol("CTR", "CTR", InBetweenEquality)
      ),
      Set.empty,
      getMaxDaysWindow, getMaxDaysLookBack
    )
    registryBuilder.register(cube)
    registryBuilder.register(pubfactAdjustmentStatsView())
  }
}
