// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.bigquery

import com.yahoo.maha.core.BaseBigqueryExpressionTest.GET_A_BY_B
import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core._
import com.yahoo.maha.core.ddl.BigqueryDDLAnnotation
import com.yahoo.maha.core.dimension.{DimCol, _}
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.lookup.LongRangeLookup
import com.yahoo.maha.core.query.{BaseQueryGeneratorTest, SharedDimSchema}
import com.yahoo.maha.core.registry.{Registry, RegistryBuilder}
import com.yahoo.maha.core.request.{AsyncRequest, SyncRequest}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

trait BaseBigqueryQueryGeneratorTest
  extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll
    with BaseQueryGeneratorTest
    with SharedDimSchema {

  override protected def beforeAll(): Unit = {
    BigqueryQueryGenerator.register(
      queryGeneratorRegistry,
      BigqueryPartitionColumnRenderer,
      TestBigqueryUDFRegistrationFactory()
    )
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
    registryBuilder.register(pubfact1(forcedFilters))
    registryBuilder.register(pubfact2(forcedFilters))
    registryBuilder.register(pubfact3())
  }

  protected[this] def s_stats_fact(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import BigqueryExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "s_stats_fact", DailyGrain, BigqueryEngine, Set(AdvertiserSchema),
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
          , DimCol("network_type", StrType(100, (Map("TEST_PUBLISHER" -> "Test Publisher", "CONTENT_S" -> "Content Secured", "EXTERNAL" -> "External Partners" ,  "INTERNAL" -> "Internal Properties"), "NONE")))
          , DimCol("start_time", IntType())
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("stats_date", DateType("%Y-%m-%d"))
          , DimCol("ad_format_id", IntType(3, (SharedDefinitions.adFormatIdToNameMap, "Other")))
          , DimCol("ad_format_type", IntType(8, (SharedDefinitions.adFormatIdtoTypeMap, "N/A")), alias = Option("ad_format_id"))
          , DimCol("ad_format_sub_type", IntType(8, (SharedDefinitions.adFormatIdtoSubTypeMap, "N/A")), alias = Option("ad_format_id"))
          , DimCol("device_id", IntType(8, (Map(5199520 -> "SmartPhone", 5199503 -> "Tablet", 5199421 -> "Desktop", -1 -> "UNKNOWN"), "UNKNOWN")))
          , DimCol("device_type", IntType(8, (Map(5199520 -> "SmartPhone", 5199503 -> "Tablet", 5199421 -> "Desktop", -1 -> "UNKNOWN"), "UNKNOWN")), alias = Option("device_id"))
          , DimCol("column_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned")))
          , DimCol("column2_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned_with_singleton")))
          , BigqueryDerDimCol("Ad Group Start Date Full", StrType(), TIMESTAMP_TO_FORMATTED_DATE("{start_time}", "%Y-%m-%d %H:%M:%S"))

        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          , FactCol("weighted_position", DecType(0, "0.0"))
          , BigqueryDerFactCol("Average CPC", DecType(), "{spend}" /- "{clicks}", rollupExpression = NoopRollup)
          , BigqueryDerFactCol("Average CPC Cents", DecType(), "{Average CPC}" * "100", rollupExpression = NoopRollup)
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), BigqueryCustomRollup(SUM("{weighted_position}" * "{impressions}") /- SUM("{impressions}")))
          , ConstFactCol("constantFact", IntType(), "0")
          , FactCol("Count", IntType(), rollupExpression = CountRollup)
        ),underlyingTableName = Some("s_stats_fact_underlying")
      )
    }
      .toPublicFact("s_stats",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("ad_group_id", "Ad Group ID", InEqualityFieldEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("account_id", "Advertiser ID", InEqualityFieldEquality),
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
          PubCol("ad_format_id", "Ad Format Name", InNotInEqualityNotEqualsLikeNullNotNull),
          PubCol("ad_format_type", "Ad Format Type", InNotInEqualityNotEqualsLikeNullNotNull),
          PubCol("ad_format_sub_type", "Ad Format Sub Type", InNotInEqualityNotEqualsLikeNullNotNull),
          PubCol("device_id", "Device ID", InNotInEqualityNotEqualsLikeNullNotNull, incompatibleColumns = Set("Device Type")),
          PubCol("device_type", "Device Type", InNotInEqualityNotEqualsLikeNullNotNull, incompatibleColumns = Set("Device ID"))

        ),
        Set(
          PublicFactCol("impressions", "Impressions", InNotInBetweenEqualityNotEqualsGreaterLesser),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("spend", "Spend", FieldEquality),
          PublicFactCol("avg_pos", "Average Position", Set.empty),
          PublicFactCol("constantFact", "Constant Fact", Set.empty),
          PublicFactCol("max_bid", "Max Bid", FieldEquality),
          PublicFactCol("Average CPC", "Average CPC", InBetweenEquality),
          PublicFactCol("Average CPC Cents", "Average CPC Cents", InBetweenEquality),
          PublicFactCol("Count", "Count", InBetweenEquality)
        ),
        Set(),
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }

  protected[this] def aga_stats_fact(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import BigqueryExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "aga_fact", DailyGrain, BigqueryEngine, Set(AdvertiserSchema),
        Set(
          DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("account_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3, (Map(1 -> "Native", 2 -> "Search"), "UNKNOWN")))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("stats_date", DateType("%Y-%m-%d"))
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
          , FactCol("weighted_position", DecType(0, "0.0"))
          , BigqueryDerFactCol("average_cpc", DecType(), "{spend}" /- "{clicks}")
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), BigqueryCustomRollup(SUM("{weighted_position}" * "{impressions}") /- SUM("{impressions}")))
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
    val builder: FactBuilder = {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        import BigqueryExpression._
        Fact.newFact(
          "bidreco_complete", DailyGrain, BigqueryEngine, Set(AdvertiserSchema),
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
            , ConstDimCol("test_constant_col", DecType(), "test_constant_col_value")
            , BigqueryDerDimCol("bid_modifier", DecType(0, "0.0"), BigqueryDerivedExpression("({modified_bid}" - "{current_bid})" / "{current_bid}" * "100"))
            , BigqueryDerDimCol("Day", DateType("YYYYMMdd"), BigqueryDerivedExpression("SUBSTR({load_time}, 1, 8)"))
            , BigqueryPartDimCol("load_time", StrType(), partitionLevel = FirstPartitionLevel)
            , BigqueryPartDimCol("shard", StrType(10,default="all"), partitionLevel = SecondPartitionLevel,
              annotations = Set(BigqueryShardingExpression(BigqueryDerivedExpression("{shard} = PMOD({account_id}, 31)")))
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
            , BigqueryDerFactCol("forecasted_spend", DecType(0, "0.0"), BigqueryDerivedExpression("{spend}" * "{forecasted_clicks}" / "{actual_clicks}" * "{recommended_bid}" / "{modified_bid}"), rollupExpression = SumRollup)
            , BigqueryDerFactCol("max_rollup_spend", DecType(0, "0.0"), BigqueryDerivedExpression("{spend}" * "{forecasted_clicks}" / "{actual_clicks}" * "{recommended_bid}" / "{modified_bid}"), rollupExpression = MaxRollup)
            , BigqueryDerFactCol("min_rollup_spend", DecType(0, "0.0"), BigqueryDerivedExpression("{spend}" * "{forecasted_clicks}" / "{actual_clicks}" * "{recommended_bid}" / "{modified_bid}"), rollupExpression = MinRollup)
            , BigqueryDerFactCol("avg_rollup_spend", DecType(0, "0.0"), BigqueryDerivedExpression("{spend}" * "{forecasted_clicks}" / "{actual_clicks}" * "{recommended_bid}" / "{modified_bid}"), rollupExpression = AverageRollup)
            , BigqueryDerFactCol("custom_rollup_spend", DecType(0, "0.0"), BigqueryDerivedExpression("{spend}" * "{forecasted_clicks}" / "{actual_clicks}" * "{recommended_bid}" / "{modified_bid}"), rollupExpression = BigqueryCustomRollup(""))
            , BigqueryDerFactCol("noop_rollup_spend", DecType(0, "0.0"), BigqueryDerivedExpression("{spend}" * "{forecasted_clicks}" / "{actual_clicks}" * "{recommended_bid}" / "{modified_bid}"), rollupExpression = NoopRollup)
            , BigqueryDerFactCol("Average Bid", DecType(8, 2, "0.0"), GET_A_BY_B("{recommended_bid}", "{actual_impressions}"), rollupExpression = NoopRollup)
            , BigqueryDerFactCol("Bid Mod", DecType(8, 5, "0.0"), GET_A_BY_B("{recommended_bid}",  "{actual_clicks}"), rollupExpression = NoopRollup)
            , BigqueryDerFactCol("Bid Modifier Fact", DecType(8, 5), COALESCE("CASE WHEN {Bid Mod} = 0 THEN 1 WHEN IS_NAN({Bid Mod}) THEN 1 ELSE {Bid Mod} END", "1"), rollupExpression = NoopRollup )
            , BigqueryDerFactCol("Modified Bid Fact", DecType(8, 5), COALESCE("CASE WHEN {Bid Modifier Fact} = 0 THEN 1 WHEN IS_NAN({Bid Modifier Fact}) THEN 1 ELSE {Bid Modifier Fact} END", "1") * "{Average Bid}")
          )
          , ddlAnnotation = Option(
            BigqueryDDLAnnotation(Map(),
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
                  "last_updated",
                  "test_constant_col"
                ))), annotations = Set(BigqueryPartitioningScheme("frequency"))
        )
      }
    }

    val publicFact = builder.toPublicFact(
      "bid_reco",
      Set(
        PubCol("ad_group_id", "Ad Group ID",InNotInEquality)
        , PubCol("account_id", "Advertiser ID", InNotInEquality, required = true)
        , PubCol("campaign_id", "Campaign ID", InNotInEquality)
        , PubCol("supply_group", "Supply Group Name", InNotInEquality)
        , PubCol("bid_strategy", "Bid Strategy", InNotInEquality, required = true)
        , PubCol("current_bid", "Current Base Bid", InNotInEquality)
        , PubCol("modified_bid", "Modified Bid", InNotInEquality)
        , PubCol("bid_modifier", "Bid Modifier", InBetweenEquality)
        , PubCol("status", "Status", InNotInEquality)
        , PubCol("Day", "Day", BetweenEquality)
        , PubCol("test_constant_col", "Test Constant Col", Set.empty)
        , PubCol("load_time", "Load Time", Set.empty) // Projecting Part Col, to cover tests
      ),
      Set(
        PublicFactCol("recommended_bid", "Recommended Bid", InBetweenEquality)
        , PublicFactCol("actual_clicks", "Clicks", InBetweenEquality)
        , PublicFactCol("forecasted_clicks", "Forecasted Clicks", InBetweenEquality)
        , PublicFactCol("actual_impressions", "Impressions", InNotInBetweenEqualityNotEqualsGreaterLesser)
        , PublicFactCol("forecasted_impr", "Forecasted Impressions", InBetweenEquality)
        , PublicFactCol("budget", "Budget", InBetweenEquality)
        , PublicFactCol("spend", "Spend", InBetweenEquality)
        , PublicFactCol("forecasted_spend", "Forecasted Spend", InBetweenEquality)
        , PublicFactCol("max_rollup_spend", "Max Rollup Spend", InBetweenEquality)
        , PublicFactCol("min_rollup_spend", "Min Rollup Spend", InBetweenEquality)
        , PublicFactCol("avg_rollup_spend", "Avg Rollup Spend", InBetweenEquality)
        , PublicFactCol("custom_rollup_spend", "Custom Rollup Spend", InBetweenEquality)
        , PublicFactCol("noop_rollup_spend", "Noop Rollup Spend", InBetweenEquality)
        , PublicFactCol("Average Bid", "Average Bid", InNotInBetweenEqualityNotEqualsGreaterLesser)
        , PublicFactCol("Bid Modifier Fact", "Bid Modifier Fact", InNotInBetweenEqualityNotEqualsGreaterLesser)
        , PublicFactCol("Modified Bid Fact", "Modified Bid Fact", InNotInBetweenEqualityNotEqualsGreaterLesser)
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
    import BigqueryExpression._
    import com.yahoo.maha.core.BaseBigqueryExpressionTest._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "ce_stats_fact", DailyGrain, BigqueryEngine, Set(AdvertiserSchema),
        Set(
          DimCol("stats_date", DateType())
          , DimCol("stats_hour", IntType(2))
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
          , BigqueryDerDimCol("call_start_time_utc", StrType(), GET_UTC_TIME_FROM_EPOCH("{call_start_time}","%Y-%m-%d %H:%M:%S"))
          , BigqueryDerDimCol("call_end_time_utc", StrType(), GET_UTC_TIME_FROM_EPOCH("{call_end_time}","%Y-%m-%d %H:%M:%S"))
          , BigqueryDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
          , BigqueryDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "W"))
          , BigqueryPartDimCol("frequency", StrType(), partitionLevel = FirstPartitionLevel)
          , BigqueryPartDimCol("utc_date", StrType(), partitionLevel = SecondPartitionLevel)
          , BigqueryPartDimCol("utc_hour", StrType(), partitionLevel = ThirdPartitionLevel)
        ),
        Set(
        )
        , ddlAnnotation = Option(BigqueryDDLAnnotation(Map()
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

  def pubfact1(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    val campaignStats  = {
      import com.yahoo.maha.core.BigqueryExpression._
      ColumnContext.withColumnContext {
        implicit dc: ColumnContext =>
          Fact.newFactForView(
            "campaign_stats", DailyGrain, BigqueryEngine, Set(AdvertiserSchema, ResellerSchema),
            Set(
              DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
              , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
              , DimCol("stats_date", DateType("%Y-%m-%d"))
              , BigqueryDerDimCol("Month", DateType(), "{stats_date}")
              , BigqueryDerDimCol("Week", DateType(), "{stats_date}")
            ),
            Set(
              FactCol("impressions", IntType(3, 1))
              , FactCol("clicks", IntType(3, 0, 1, 800))
              , BigqueryDerFactCol("spend", DecType(0, "0.0"), BigqueryDerivedExpression("{impressions}"))
            )
          )
      }
    }

    val campaignAdjustment  = {
      import com.yahoo.maha.core.BigqueryExpression._
      ColumnContext.withColumnContext {
        implicit dc: ColumnContext =>
          Fact.newFactForView(
            "campaign_adjustments", DailyGrain, BigqueryEngine, Set(AdvertiserSchema, ResellerSchema),
            Set(
              DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
              , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
              , DimCol("stats_date", DateType("%Y-%m-%d"))
              , BigqueryDerDimCol("Month", DateType(), "{stats_date}")
              , BigqueryDerDimCol("Week", DateType(), "{stats_date}")
            ),
            Set(
              FactCol("impressions", IntType(3, 1))
              , FactCol("clicks", IntType(3, 0, 1, 800))
              , BigqueryDerFactCol("spend", DecType(0, "0.0"), BigqueryDerivedExpression("{impressions}"))
            )
          )
      }
    }

    val campaignAdjView = UnionView("campaign_adjustment_view", Seq(campaignStats, campaignAdjustment))

    val accountStats = campaignStats.copyWith("account_stats", Set("campaign_id"), Map.empty)
    val accountAdjustment = campaignAdjustment.copyWith("account_adjustment", Set("campaign_id"), Map.empty)

    val accountAdjustmentView = UnionView("account_adjustment_view", Seq(accountStats, accountAdjustment))

    ColumnContext.withColumnContext {
      import com.yahoo.maha.core.BigqueryExpression._
      implicit dc: ColumnContext =>
        Fact.newUnionView(campaignAdjView, DailyGrain, BigqueryEngine, Set(AdvertiserSchema, ResellerSchema),
          Set(
            DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("stats_date", DateType("%Y-%m-%d"))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
            , BigqueryDerDimCol("Month", DateType(), "{stats_date}")
            , BigqueryDerDimCol("Week", DateType(), "{stats_date}")
          ),
          Set(
            FactCol("impressions", IntType(3, 1))
            , FactCol("clicks", IntType(3, 0, 1, 800))
            , BigqueryDerFactCol("spend", DecType(0, "0.0"), BigqueryDerivedExpression("{impressions}"))
          )
        )
    }
      .newViewTableRollUp(accountAdjustmentView,"campaign_adjustment_view", Set("campaign_id"))
      .toPublicFact("a_stats",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("Month", "Month", Equality),
          PubCol("Week", "Week", Equality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("spend", "Spend", Set.empty)
        ), Set(),  getMaxDaysWindow, getMaxDaysLookBack
      )
  }

  def pubfact2(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import BigqueryExpression._
    import UDFBigqueryExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "ad_fact1", DailyGrain, BigqueryEngine, Set(AdvertiserSchema, ResellerSchema),
        Set(
          DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("restaurant_id", IntType(), alias = Option("advertiser_id"), annotations = Set(ForeignKey("restaurant")))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("sm_string", StrType(3, (Map("Y" -> "Yes", "N" -> "No"), "NONE")))
          , DimCol("start_time", IntType())
          , DimCol("stats_date", DateType("%Y-%m-%d"))
          , DimCol("show_flag", IntType())
          , BigqueryDerDimCol("Month", DateType(), TEST_DATE_UDF("{stats_date}", "M"))
          , BigqueryDerDimCol("Week", DateType(), TEST_DATE_UDF("{stats_date}", "W"))
        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("s_impressions", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("engagement_count", IntType(10))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          , FactCol("CTR", DecType(), BigqueryCustomRollup(SUM("{clicks}" /- "{impressions}")))
          , FactCol("weighted_position", DecType(0, "0.0"))
          , BigqueryDerFactCol("Engagement Rate", DecType(), "100" * TEST_MATH_UDF("{engagement_count}", "{impressions}"), rollupExpression = NoopRollup)
          , BigqueryDerFactCol("Paid Engagement Rate", DecType(), "100" * TEST_MATH_UDAF("{engagement_count}", "0", "0", "{clicks}", "{impressions}"), rollupExpression = NoopRollup)
          , BigqueryDerFactCol("Average CPC", DecType(), "{spend}" /- "{clicks}")
          , BigqueryDerFactCol("Average CPC Cents", DecType(), "{Average CPC}" * "100")
          , BigqueryDerFactCol("N Spend", DecType(), DECODE("{stats_source}", "1", "{spend}", "0.0"))
          , BigqueryDerFactCol("N Clicks", DecType(), DECODE("{stats_source}", "1", "{clicks}", "0.0"))
          , BigqueryDerFactCol("N Average CPC", DecType(), "{N Spend}" /- "{N Clicks}")
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), BigqueryCustomRollup(SUM("{weighted_position}" * "{impressions}") /- SUM("{impressions}")))
          , BigqueryDerFactCol("impression_share", IntType(), DECODE(MAX("{show_flag}"), "1", ROUND(SUM("{impressions}") /- SUM("{s_impressions}"), 4), "NULL"), rollupExpression = NoopRollup)
          , BigqueryDerFactCol("impression_share_rounded", IntType(), ROUND("{impression_share}", 5), rollupExpression = NoopRollup)
          , BigqueryDerFactCol("Click Rate", IntType(), SUM("{clicks}") /- SUM("{impressions}"), rollupExpression = NoopRollup)
        ),
        annotations = Set(
        )
      )
    }
      .toPublicFact("performance_stats",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("ad_id", "Ad ID", InEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("advertiser_id", "Advertiser ID", InNotInEqualityNotEqualsLikeNullNotNull),
          PubCol("restaurant_id", "Restaurant ID", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("sm_string", "Static Mapping String", In),
          PubCol("Month", "Month", Equality),
          PubCol("Week", "Week", Equality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("spend", "Spend", Set.empty),
          PublicFactCol("avg_pos", "Average Position", Set.empty),
          PublicFactCol("max_bid", "Max Bid", Set.empty),
          PublicFactCol("Engagement Rate", "Engagement Rate", InBetweenEquality),
          PublicFactCol("Paid Engagement Rate", "Paid Engagement Rate", InBetweenEquality),
          PublicFactCol("Average CPC", "Average CPC", InBetweenEquality),
          PublicFactCol("Average CPC Cents", "Average CPC Cents", InBetweenEquality),
          PublicFactCol("CTR", "CTR", InBetweenEquality),
          PublicFactCol("N Spend", "N Spend", InBetweenEquality),
          PublicFactCol("Click Rate", "Click Rate", Set.empty),
          PublicFactCol("N Clicks", "N Clicks", InBetweenEquality),
          PublicFactCol("N Average CPC", "N Average CPC", InBetweenEquality),
          PublicFactCol("impression_share_rounded", "Impression Share", InBetweenEquality)
        ),
        forcedFilters,
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }

  def pubfact3(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import BigqueryExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "s_stats_fact", MinuteGrain, BigqueryEngine, Set(AdvertiserSchema),
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
          , DimCol("stats_date", TimestampType())
          , DimCol("column_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned")))
          , DimCol("column2_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned_with_singleton")))
          , BigqueryDerDimCol("Ad Group Start Date Full", StrType(), TIMESTAMP_TO_FORMATTED_DATE("{start_time}", "%Y-%m-%d %H:%M:%S"))

        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          , FactCol("weighted_position", DecType(0, "0.0"))
          , BigqueryDerFactCol("Average CPC", DecType(), "{spend}" /- "{clicks}", rollupExpression = NoopRollup)
          , BigqueryDerFactCol("Average CPC Cents", DecType(), "{Average CPC}" * "100", rollupExpression = NoopRollup)
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), BigqueryCustomRollup(SUM("{weighted_position}" * "{impressions}") /- SUM("{impressions}")))
          , ConstFactCol("constantFact", IntType(), "0")
          , FactCol("Count", IntType(), rollupExpression = CountRollup)
        ),underlyingTableName = Some("s_stats_fact_underlying")
      )
    }
      .toPublicFact("s_stats_minute",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("ad_group_id", "Ad Group ID", InEqualityFieldEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("account_id", "Advertiser ID", InEqualityFieldEquality),
          PubCol("keyword_id", "Keyword ID", InEquality),
          PubCol("keyword", "Keyword", InEquality),
          PubCol("search_term", "Search Term", InEquality),
          PubCol("delivered_match_type", "Delivered Match Type", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("source_name", "Source Name", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty),
          PubCol("column_id", "Column ID", Equality),
          PubCol("column2_id", "Column2 ID", Equality),
          PubCol("Ad Group Start Date Full", "Ad Group Start Date Full", InEquality),
          PubCol("network_type", "Network ID", InEquality),
          PubCol("device_id", "Device ID", InEquality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InNotInBetweenEqualityNotEqualsGreaterLesser),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("spend", "Spend", FieldEquality),
          PublicFactCol("avg_pos", "Average Position", Set.empty),
          PublicFactCol("constantFact", "Constant Fact", Set.empty),
          PublicFactCol("max_bid", "Max Bid", FieldEquality),
          PublicFactCol("Average CPC", "Average CPC", InBetweenEquality),
          PublicFactCol("Average CPC Cents", "Average CPC Cents", InBetweenEquality),
          PublicFactCol("Count", "Count", InBetweenEquality)
        ),
        Set(),
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }

  case object TestDateUDFRegistration extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION dateUDF as 'com.yahoo.maha.query.bigquery.udf.TestDateUDF';"
  }

  case object TestDecodeUDFRegistration extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION decodeUDF as 'com.yahoo.maha.query.bigquery.udf.TestDecodeUDF';"
  }

  case object TestMathUDFRegistration extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION mathUDF as 'com.yahoo.maha.query.bigquery.udf.TestMathUDF';"
  }

  case object TestMathUDAFRegistration extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION mathUDAF as 'com.yahoo.maha.query.bigquery.udf.TestMathUDAF';"
  }

  object UDFBigqueryExpression {
    import BigqueryExpression._

    implicit val uDFRegistrationFactory = DefaultUDFRegistrationFactory

    uDFRegistrationFactory.register(TestDateUDFRegistration)
    uDFRegistrationFactory.register(TestDecodeUDFRegistration)
    uDFRegistrationFactory.register(TestMathUDFRegistration)
    uDFRegistrationFactory.register(TestMathUDAFRegistration)

    case class TEST_DATE_UDF(s: BigqueryExp, fmt: String) extends UDFBigqueryExpression(TestDateUDFRegistration) {
      def hasRollupExpression = s.hasRollupExpression

      def hasNumericOperation = s.hasNumericOperation

      def asString: String = s"dateUDF(${s.asString}, '$fmt')"
    }

    case class TEST_MATH_UDF(args: BigqueryExp*) extends UDFBigqueryExpression(TestDecodeUDFRegistration) {
      val hasRollupExpression = args.exists(_.hasRollupExpression)
      val hasNumericOperation = args.exists(_.hasNumericOperation)
      val argStrs = args.map(_.asString).mkString(", ")
      def asString: String = s"mathUDF($argStrs)"
    }

    case class TEST_MATH_UDAF(args: BigqueryExp*) extends UDFBigqueryExpression(TestDecodeUDFRegistration) {
      val hasRollupExpression = args.exists(_.hasRollupExpression)
      val hasNumericOperation = args.exists(_.hasNumericOperation)
      val argStrs = args.map(_.asString).mkString(", ")
      def asString: String = s"mathUDAF($argStrs)"
    }
  }
}

object SharedDefinitions {
  val adFormatIdToNameMap = Map(
    2 -> "Single image",
    3 -> "Single image",
    4 -> "Single image",
    5 -> "Single image",
    6 -> "Single image",
    7 -> "Video",
    8 -> "Video with HTML Endcard",
    9 -> "Carousel",
    35 -> "Product Ad",
    97 -> "DPA Collection Ad",
    98 -> "DPA View More",
    99 -> "DPA Extended Carousel",
    100 -> "DPA Single Image Ad",
    101 -> "DPA Carousel Ad"
  )

  val adFormatIdtoSubTypeMap = Map(
    35 -> "Product Ad",
    97 -> "DPA Collection Ad",
    98 -> "DPA View More",
    99 -> "DPA Extended Carousel",
    100 -> "DPA Single Image Ad",
    101 -> "DPA Carousel Ad"
  )

  val adFormatIdtoTypeMap = Map(
    35 -> "Product Ad",
    97 -> "DPA Collection Ad",
    98 -> "DPA View More",
    99 -> "DPA Extended Carousel",
    100 -> "DPA Single Image Ad",
    101 -> "DPA Carousel Ad"
  )
}
