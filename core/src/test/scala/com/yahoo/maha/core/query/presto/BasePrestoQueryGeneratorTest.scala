// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.presto

import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core._
import com.yahoo.maha.core.ddl.PrestoDDLAnnotation
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.lookup.LongRangeLookup
import com.yahoo.maha.core.query.{BaseQueryGeneratorTest, SharedDimSchema}
import com.yahoo.maha.core.registry.{Registry, RegistryBuilder}
import com.yahoo.maha.core.request.{AsyncRequest, SyncRequest}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

/**
 * Created by hiral on 1/19/16.
 */
trait BasePrestoQueryGeneratorTest
  extends FunSuite with Matchers with BeforeAndAfterAll with BaseQueryGeneratorTest with SharedDimSchema {

  override protected def beforeAll(): Unit = {
    PrestoQueryGenerator.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer, TestPrestoUDFRegistrationFactory())
  }

  override protected[this] def getDefaultRegistry(forcedFilters: Set[ForcedFilter] = Set.empty): Registry = {
    val registryBuilder = new RegistryBuilder
    registerFacts(forcedFilters, registryBuilder)
    registerDims(registryBuilder)
    registryBuilder.build()
  }

  override protected[this] def registerFacts(forcedFilters: Set[ForcedFilter], registryBuilder: RegistryBuilder): Unit = {
    registryBuilder.register(s_stats_fact(forcedFilters))
    registryBuilder.register(aga_stats_fact(forcedFilters))
    registryBuilder.register(ce_stats(forcedFilters))
    registryBuilder.register(bidReco())
  }

  protected[this] def s_stats_fact(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import PrestoExpression._
    import com.yahoo.maha.core.BasePrestoExpressionTest._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "s_stats_fact", DailyGrain, PrestoEngine, Set(AdvertiserSchema),
        Set(
          DimCol("campaign_id", StrType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("account_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("keyword_id", IntType())
          , DimCol("keyword", StrType(), annotations = Set(EscapingRequired))
          , DimCol("search_term", StrType(50,default = "None"))
          , DimCol("delivered_match_type", IntType(3, (Map(1 -> "Exact", 2 -> "Broad", 3 -> "Phrase"), "UNKNOWN")))
          , DimCol("stats_source", IntType(3))
          , DimCol("source_name", IntType(3), alias = Option("stats_source"))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("device_id", IntType(3, (Map(11 -> "Desktop", 22 -> "Tablet", 33 -> "SmartPhone", -1 -> "UNKNOWN"), "UNKNOWN")))
          , DimCol("network_type", StrType(100, (Map("TEST_PUBLISHER" -> "Test Publisher", "CONTENT_S" -> "Content Secured", "EXTERNAL" -> "External Partners" ,  "INTERNAL" -> "Internal Properties"), "NONE")))
          , DimCol("start_time", IntType())
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("stats_date", DateType("YYYY-MM-dd"))
          , DimCol("column_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned")))
          , DimCol("column2_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned_with_singleton")))
          , PrestoDerDimCol("Ad Group Start Date Full", StrType(), TIMESTAMP_TO_FORMATTED_DATE("{start_time}", "YYYY-MM-dd HH:mm:ss"))

        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          , PrestoDerFactCol("Average CPC", DecType(), "{spend}" /- "{clicks}", rollupExpression = NoopRollup)
          , PrestoDerFactCol("Average CPC Cents", DecType(), "{Average CPC}" * "100", rollupExpression = NoopRollup)
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), PrestoCustomRollup(SUM("{avg_pos}" * "{impressions}") /- SUM("{impressions}")))
        )
      )
    }
      .toPublicFact("s_stats",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("account_id", "Advertiser ID", InEquality),
          PubCol("keyword_id", "Keyword ID", InEquality),
          PubCol("keyword", "Keyword", InEquality),
          PubCol("search_term", "Search Term", InEquality),
          PubCol("delivered_match_type", "Delivered Match Type", InEquality),
          PubCol("stats_source", "Source", Equality, incompatibleColumns = Set("Source Name")),
          PubCol("source_name", "Source Name", Equality, incompatibleColumns = Set("Source")),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty),
          PubCol("column_id", "Column ID", Equality),
          PubCol("column2_id", "Column2 ID", Equality),
          PubCol("Ad Group Start Date Full", "Ad Group Start Date Full", InEquality),
          PubCol("network_type", "Network ID", InEquality),
          PubCol("device_id", "Device ID", InEquality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("spend", "Spend", Set.empty),
          PublicFactCol("avg_pos", "Average Position", Set.empty),
          PublicFactCol("max_bid", "Max Bid", Set.empty),
          PublicFactCol("Average CPC", "Average CPC", InBetweenEquality),
          PublicFactCol("Average CPC Cents", "Average CPC Cents", InBetweenEquality)
        ),
        Set(),
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }

  protected[this] def aga_stats_fact(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import PrestoExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "aga_fact", DailyGrain, PrestoEngine, Set(AdvertiserSchema),
        Set(
          DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("account_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3, (Map(1 -> "Native", 2 -> "Search"), "UNKNOWN")))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("stats_date", DateType("YYYY-MM-dd"))
          , DimCol("stats_hour", StrType())
          , DimCol("age", StrType())
          , DimCol("gender", StrType())
        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("conversions", IntType())
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          , FactCol("ad_extn_spend", DecType())
          , PrestoDerFactCol("average_cpc", DecType(), "{spend}" /- "{clicks}")
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), PrestoCustomRollup(SUM("{avg_pos}" * "{impressions}") /- SUM("{impressions}")))
        )
        , forceFilters =  Set(ForceFilter(EqualityFilter("Source", "2", isForceFilter = true, isOverridable = true)))
      )
    }
      .toPublicFact("u_stats",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("account_id", "Advertiser ID", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("stats_hour", "Hour", BetweenEquality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("age", "Age", InEquality),
          PubCol("gender", "Gender", InEquality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("conversions", "Conversion", InBetweenEquality),
          PublicFactCol("spend", "Spend", Set.empty),
          PublicFactCol("avg_pos", "Average Position", Set.empty),
          PublicFactCol("max_bid", "Max Bid", Set.empty),
          PublicFactCol("ad_extn_spend", "Ad Extn Spend", InBetweenEquality),
          PublicFactCol("average_cpc", "Average CPC", InBetweenEquality)
        ),
        Set(),
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }

  def bidReco(): PublicFact = {
    val builder : FactBuilder = {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        import PrestoExpression._
        Fact.newFact(
          "bidreco_complete", DailyGrain, PrestoEngine, Set(AdvertiserSchema),
          Set(
            DimCol("ad_group_id", IntType(10), annotations = Set(ForeignKey("ad_group")))
            , DimCol("account_id", IntType(10), annotations = Set(ForeignKey("advertiser")))
            , DimCol("campaign_id", IntType(10), annotations = Set(ForeignKey("campaign")))
            , DimCol("supply_group", StrType(50))
            , DimCol("bid_strategy", IntType(1, (Map(1 -> "Max Click", 2 -> "Inflection Point"), "NONE")))
            , DimCol("status", StrType(255), annotations = Set(EscapingRequired))
            , DimCol("last_updated", IntType(10))
            , DimCol("current_bid", DecType(0, "0.0"))
            , DimCol("modified_bid", DecType(0, "0.0"))
            , PrestoDerDimCol("bid_modifier", DecType(0, "0.0"), PrestoDerivedExpression("({modified_bid}" - "{current_bid})" / "{current_bid}" * "100"))
            , PrestoDerDimCol("Day", DateType("YYYYMMdd"), PrestoDerivedExpression("SUBSTRING({load_time}, 1, 8)"))
            , PrestoPartDimCol("load_time", StrType(), partitionLevel = FirstPartitionLevel)
            , PrestoPartDimCol("shard", StrType(10,default="all"), partitionLevel = SecondPartitionLevel,
              annotations = Set(PrestoShardingExpression(PrestoDerivedExpression("{shard} = PMOD({account_id}, 31)")))
            )
          ),
          Set(
            FactCol("recommended_bid", DecType(0, "0.0"))
            , FactCol("actual_clicks", IntType(10, 0))
            , FactCol("forecasted_clicks", IntType(10, 0))
            , FactCol("actual_impressions", IntType(10, 0))
            , FactCol("forecasted_impr", IntType(10, 0))
            , FactCol("spend", DecType(0, "0.0"))
            , FactCol("budget", DecType(0, "0.0"))
            , FactCol("forecasted_budget", DecType(0, "0.0"))
            , PrestoDerFactCol("forecasted_spend", DecType(0, "0.0"), PrestoDerivedExpression("{spend}" * "{forecasted_clicks}" / "{actual_clicks}" * "{recommended_bid}" / "{modified_bid}"), rollupExpression = SumRollup)
            , PrestoDerFactCol("max_rollup_spend", DecType(0, "0.0"), PrestoDerivedExpression("{spend}" * "{forecasted_clicks}" / "{actual_clicks}" * "{recommended_bid}" / "{modified_bid}"), rollupExpression = MaxRollup)
            , PrestoDerFactCol("min_rollup_spend", DecType(0, "0.0"), PrestoDerivedExpression("{spend}" * "{forecasted_clicks}" / "{actual_clicks}" * "{recommended_bid}" / "{modified_bid}"), rollupExpression = MinRollup)
            , PrestoDerFactCol("avg_rollup_spend", DecType(0, "0.0"), PrestoDerivedExpression("{spend}" * "{forecasted_clicks}" / "{actual_clicks}" * "{recommended_bid}" / "{modified_bid}"), rollupExpression = AverageRollup)
            , PrestoDerFactCol("custom_rollup_spend", DecType(0, "0.0"), PrestoDerivedExpression("{spend}" * "{forecasted_clicks}" / "{actual_clicks}" * "{recommended_bid}" / "{modified_bid}"), rollupExpression = PrestoCustomRollup(""))
            , PrestoDerFactCol("noop_rollup_spend", DecType(0, "0.0"), PrestoDerivedExpression("{spend}" * "{forecasted_clicks}" / "{actual_clicks}" * "{recommended_bid}" / "{modified_bid}"), rollupExpression = NoopRollup)
          )
          , ddlAnnotation = Option(
            PrestoDDLAnnotation(Map(),
              columnOrdering =
                IndexedSeq(
                  "ad_group_id",
                  "campaign_id",
                  "account_id",
                  "supply_group",
                  "bid_strategy",
                  "status",
                  "current_bid",
                  "modified_bid",
                  "recommended_bid",
                  "actual_clicks",
                  "forecasted_clicks",
                  "actual_impressions",
                  "forecasted_impr",
                  "spend",
                  "budget",
                  "forecasted_budget",
                  "last_updated"
                ))), annotations = Set(PrestoPartitioningScheme("frequency"))
        )
      }
    }

    val publicFact = builder.toPublicFact(
      "bid_reco",
      Set(
        PubCol("ad_group_id", "Ad Group ID",InNotInEquality, required = true)
        , PubCol("account_id", "Advertiser ID", InNotInEquality, required = true)
        , PubCol("campaign_id", "Campaign ID", InNotInEquality)
        , PubCol("supply_group", "Supply Group Name", InNotInEquality)
        , PubCol("bid_strategy", "Bid Strategy", InNotInEquality, required = true)
        , PubCol("current_bid", "Current Base Bid", InNotInEquality)
        , PubCol("modified_bid", "Modified Bid", InNotInEquality)
        , PubCol("bid_modifier", "Bid Modifier", InBetweenEquality)
        , PubCol("status", "Status", InNotInEquality)
        , PubCol("Day", "Day", BetweenEquality)
      ),
      Set(
        PublicFactCol("recommended_bid", "Recommended Bid", InBetweenEquality)
        , PublicFactCol("actual_clicks", "Clicks", InBetweenEquality)
        , PublicFactCol("forecasted_clicks", "Forecasted Clicks", InBetweenEquality)
        , PublicFactCol("actual_impressions", "Impressions", InBetweenEquality)
        , PublicFactCol("forecasted_impr", "Forecasted Impressions", InBetweenEquality)
        , PublicFactCol("budget", "Budget", InBetweenEquality)
        , PublicFactCol("spend", "Spend", InBetweenEquality)
        , PublicFactCol("forecasted_spend", "Forecasted Spend", InBetweenEquality)
        , PublicFactCol("max_rollup_spend", "Max Rollup Spend", InBetweenEquality)
        , PublicFactCol("min_rollup_spend", "Min Rollup Spend", InBetweenEquality)
        , PublicFactCol("avg_rollup_spend", "Avg Rollup Spend", InBetweenEquality)
        , PublicFactCol("custom_rollup_spend", "Custom Rollup Spend", InBetweenEquality)
        , PublicFactCol("noop_rollup_spend", "Noop Rollup Spend", InBetweenEquality)
      ),
      Set(EqualityFilter("Status","Valid", isForceFilter = true)),
      Map(
        (SyncRequest, DailyGrain) -> 400, (AsyncRequest, DailyGrain) -> 400
      ),
      Map(
        (SyncRequest, DailyGrain) -> 400, (AsyncRequest, DailyGrain) -> 400
      )
    )
    publicFact
  }

  protected[this] def ce_stats(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import PrestoExpression._
    import com.yahoo.maha.core.BasePrestoExpressionTest._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "ce_stats_fact", DailyGrain, PrestoEngine, Set(AdvertiserSchema),
        Set(
          DimCol("stats_date", DateType())
          , DimCol("stats_hour", IntType(2)) //comment : "0..23"
          , DimCol("account_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("caller_name", StrType(), annotations = Set(EscapingRequired))
          , DimCol("caller_area_code", StrType(), annotations = Set(EscapingRequired))
          , DimCol("caller_number", StrType(), annotations = Set(EscapingRequired))
          , DimCol("call_start_time", IntType(0,0))
          , DimCol("call_end_time", IntType(0,0))
          , DimCol("call_status", StrType(), annotations = Set(EscapingRequired))
          , DimCol("call_duration", IntType(0,0))
          , PrestoDerDimCol("call_start_time_utc", StrType(), GET_UTC_TIME_FROM_EPOCH("{call_start_time}","YYYY-MM-dd HH:mm:ss"))
          , PrestoDerDimCol("call_end_time_utc", StrType(), GET_UTC_TIME_FROM_EPOCH("{call_end_time}","YYYY-MM-dd HH:mm:ss"))
          , PrestoDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
          , PrestoDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "W"))
          , PrestoPartDimCol("frequency", StrType(), partitionLevel = FirstPartitionLevel)
          , PrestoPartDimCol("utc_date", StrType(), partitionLevel = SecondPartitionLevel)
          , PrestoPartDimCol("utc_hour", StrType(), partitionLevel = ThirdPartitionLevel)
        ),
        Set(
        )
        , ddlAnnotation = Option(PrestoDDLAnnotation(Map()
          , columnOrdering =
            IndexedSeq(
              "stats_date",
              "stats_hour",
              "account_id",
              "campaign_id",
              "ad_group_id",
              "caller_name",
              "caller_area_code",
              "caller_number",
              "call_start_time",
              "call_end_time",
              "call_start_time_utc",
              "call_end_time_utc",
              "call_status",
              "call_duration"
            )))
        , costMultiplierMap = Map(AsyncRequest -> CostMultiplier(LongRangeLookup.full(1.2)))
      )
    }
      .toPublicFact("ce_stats",
        Set(
          PubCol("account_id", "Advertiser ID", InEquality, filteringRequired = true)
          , PubCol("campaign_id", "Campaign ID", InEquality)
          , PubCol("ad_group_id", "Ad Group ID", InEquality)
          , PubCol("stats_date", "Day", BetweenEquality)
          , PubCol("Month", "Month", InBetweenEquality)
          , PubCol("Week", "Week", InBetweenEquality)
          , PubCol("caller_name", "Caller Name", Set.empty)
          , PubCol("caller_area_code", "Caller Area Code",  Set.empty)
          , PubCol("caller_number", "Caller Number",  Set.empty)
          , PubCol("call_start_time_utc", "Call Start Time",  Set.empty)
          , PubCol("call_end_time_utc", "Call End Time",  Set.empty)
          , PubCol("call_status", "Call Status",  Set.empty)
          , PubCol("call_duration", "Call Duration",  Set.empty)
        ),
        Set(),
        Set(),
        Map((AsyncRequest, DailyGrain) -> 400), Map((AsyncRequest, DailyGrain) -> 400)
      )
  }
}
