// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.hive

import com.yahoo.maha.core.BaseExpressionTest.GET_A_BY_B
import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core._
import com.yahoo.maha.core.ddl.HiveDDLAnnotation
import com.yahoo.maha.core.dimension.{PubCol, _}
import com.yahoo.maha.core.fact.{PublicFactCol, _}
import com.yahoo.maha.core.lookup.LongRangeLookup
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.query.presto.SharedDefinitions
import com.yahoo.maha.core.registry.RegistryBuilder
import com.yahoo.maha.core.request.{AsyncRequest, SyncRequest}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Created by hiral on 1/19/16.
 */
trait BaseHiveQueryGeneratorTest
  extends AnyFunSuite with Matchers with BeforeAndAfterAll with BaseQueryGeneratorTest with SharedDimSchema {

  override protected def beforeAll(): Unit = {
    HiveQueryGenerator.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer, TestUDFRegistrationFactory())
    HiveQueryGeneratorV2.register(queryGeneratorRegistry, TestPartitionRenderer, TestUDFRegistrationFactory())
  }

  override protected[this] def registerFacts(forcedFilters: Set[ForcedFilter], registryBuilder: RegistryBuilder): Unit = {
    registryBuilder.register(s_stats_fact(forcedFilters))
    registryBuilder.register(aga_stats_fact(forcedFilters))
    registryBuilder.register(ce_stats(forcedFilters))
    registryBuilder.register(bidReco())
    registryBuilder.register(pubfact2(forcedFilters))
    registryBuilder.register(pubfact3(forcedFilters))
    registryBuilder.register(pubfact4(forcedFilters))
  }

  protected[this] def s_stats_fact(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import HiveExpression._
    import com.yahoo.maha.core.BaseExpressionTest._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "s_stats_fact", DailyGrain, HiveEngine, Set(AdvertiserSchema),
        Set(
          DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
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
          , DimCol("ad_format_id", IntType(3, (SharedDefinitions.adFormatIdToNameMap, "Other")))
          , DimCol("ad_format_type", IntType(8, (SharedDefinitions.adFormatIdtoTypeMap, "N/A")), alias = Option("ad_format_id"))
          , DimCol("ad_format_sub_type", IntType(8, (SharedDefinitions.adFormatIdtoSubTypeMap, "N/A")), alias = Option("ad_format_id"))
          , DimCol("column_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned")))
          , DimCol("column2_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned_with_singleton")))
          , HiveDerDimCol("Ad Group Start Date Full", StrType(), TIMESTAMP_TO_FORMATTED_DATE("{start_time}", "YYYY-MM-dd HH:mm:ss"))
          , HiveDerDimAggregateCol("Keyword Count",IntType(), COUNT_DISTINCT("{keyword_id}"))
          , HiveDerDimAggregateCol("Keyword Count Scaled", IntType(), COUNT("{keyword_id} * {stats_source} * 10"))
          , DimCol("internal_bucket_id", StrType())
          , HiveDerDimCol("click_exp_id", StrType(), REGEX_EXTRACT("internal_bucket_id", "(cl-)(.*?)(,|$)", 2, replaceMissingValue = true, "-3"))
          , HiveDerDimCol("device_name", StrType(), DECODE_DIM("device_id", "1", "'DeviceA'", "2", "'DeviceB'", "'UNKNOWN'"))
          , DimCol("binarycol", StrType(isBinary = true))
        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          , FactCol("weighted_position", DecType(0, "0.0"))
          , HiveDerFactCol("max_price_type", IntType(), "{price_type}", rollupExpression = MaxRollup)
          , HiveDerFactCol("Average CPC", DecType(), "{spend}" /- "{clicks}", rollupExpression = NoopRollup)
          , HiveDerFactCol("Average CPC Cents", DecType(), "{Average CPC}" * "100", rollupExpression = NoopRollup)
          , HiveDerFactCol("CTR", DecType(), "{clicks}" /- "{impressions}", rollupExpression = HiveCustomRollup(SUM("{clicks}") /- SUM("{impressions}")))
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), HiveCustomRollup(SUM("{weighted_position}" * "{impressions}") /- SUM("{impressions}")))
          , HiveDerFactCol("derived_impressions", IntType(), SUM(DECODE("{device_id}", "1", "{impressions}", "0")), rollupExpression = NoopRollup)
          , HiveDerFactCol("derived_clicks", IntType(), SUM(DECODE("{delivered_match_type}", "1", "{clicks}", "0")), rollupExpression = NoopRollup)
          , HiveDerFactCol("derived_CTR", DecType(), "{derived_clicks}" /- "{derived_impressions}", rollupExpression = NoopRollup)
          , HiveDerFactCol("binaryColDecode", IntType(), DECODE("{ad_group_id}", "1", "{binarycol}", "null"))
          , HiveDerFactCol("binaryColDecodeNoopRollup", IntType(), DECODE("{ad_group_id}", "1", GET_A_BY_B("{binarycol}"), "null"), rollupExpression = NoopRollup)
        ), underlyingTableName = Some("s_stats_fact_underlying")
      )
    }
      .toPublicFact("s_stats",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("ad_id", "Ad ID", EqualityFieldEquality),
          PubCol("ad_group_id", "Ad Group ID", InEqualityFieldEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("account_id", "Advertiser ID", InNotInEqualityNotEqualsLikeNullNotNull),
          PubCol("keyword_id", "Keyword ID", InEquality),
          PubCol("Keyword Count", "Keyword Count", Set.empty),
          PubCol("Keyword Count Scaled", "Keyword Count Scaled", Set.empty),
          PubCol("keyword", "Keyword", InEquality),
          PubCol("search_term", "Search Term", InEquality),
          PubCol("delivered_match_type", "Delivered Match Type", InEquality),
          PubCol("stats_source", "Source", Equality, incompatibleColumns = Set("Source Name")),
          PubCol("source_name", "Source Name", Equality, incompatibleColumns = Set("Source")),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty),
          PubCol("ad_format_id", "Ad Format Name", InNotInEqualityNotEqualsLikeNullNotNull),
          PubCol("ad_format_type", "Ad Format Type", InNotInEqualityNotEqualsLikeNullNotNull),
          PubCol("ad_format_sub_type", "Ad Format Sub Type", InNotInEqualityNotEqualsLikeNullNotNull),
          PubCol("column_id", "Column ID", Equality),
          PubCol("column2_id", "Column2 ID", Equality),
          PubCol("Ad Group Start Date Full", "Ad Group Start Date Full", InEquality),
          PubCol("network_type", "Network ID", InEquality),
          PubCol("device_id", "Device ID", InEquality),
          PubCol("internal_bucket_id", "Internal Bucket ID", InEquality),
          PubCol("click_exp_id", "Click Exp ID", InEquality),
          PubCol("device_name", "Device Name", InEquality),
          PubCol("binarycol", "Binary Col", Set.empty)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InNotInBetweenEqualityNotEqualsGreaterLesser),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("spend", "Spend", FieldEquality),
          PublicFactCol("avg_pos", "Average Position", Set.empty),
          PublicFactCol("max_bid", "Max Bid", FieldEquality),
          PublicFactCol("Average CPC", "Average CPC", InBetweenEquality),
          PublicFactCol("Average CPC Cents", "Average CPC Cents", InBetweenEquality),
          PublicFactCol("CTR", "CTR", InNotInBetweenEqualityNotEqualsGreaterLesser),
          PublicFactCol("max_price_type", "Max Price Type", Equality),
          PublicFactCol("derived_impressions", "Derived Impressions", Equality),
          PublicFactCol("derived_clicks", "Derived Clicks", Equality),
          PublicFactCol("derived_CTR", "Derived CTR", InNotInBetweenEqualityNotEqualsGreaterLesser),
          PublicFactCol("binaryColDecode", "Decoded Binary Col", InBetweenEquality),
          PublicFactCol("binaryColDecodeNoopRollup", "Decoded NoopRollup Binary Col", InBetweenEquality)
        ),
        Set(),
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }

  protected[this] def aga_stats_fact(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import HiveExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "aga_fact", DailyGrain, HiveEngine, Set(AdvertiserSchema),
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
          , FactCol("weighted_position", DecType(0, "0.0"))
          , HiveDerFactCol("average_cpc", DecType(), "{spend}" /- "{clicks}")
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), HiveCustomRollup(SUM("{weighted_position}" * "{impressions}") /- SUM("{impressions}")))
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
        import HiveExpression._
        Fact.newFact(
          "bidreco_complete", DailyGrain, HiveEngine, Set(AdvertiserSchema),
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
            , HiveDerDimCol("bid_modifier", DecType(0, "0.0"), HiveDerivedExpression("({modified_bid}" - "{current_bid})" / "{current_bid}" * "100"))
            , HiveDerDimCol("Day", DateType("YYYYMMdd"), HiveDerivedExpression("SUBSTRING({load_time}, 1, 8)"))
            , HivePartDimCol("load_time", StrType(), partitionLevel = FirstPartitionLevel)
            , HivePartDimCol("shard", StrType(10,default="all"), partitionLevel = SecondPartitionLevel,
              annotations = Set(HiveShardingExpression(HiveDerivedExpression("{shard} = PMOD({account_id}, 31)")))
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
            , HiveDerFactCol("forecasted_spend", DecType(0, "0.0"), HiveDerivedExpression("{spend}" * "{forecasted_clicks}" / "{actual_clicks}" * "{recommended_bid}" / "{modified_bid}"), rollupExpression = SumRollup)
            , HiveDerFactCol("max_rollup_spend", DecType(0, "0.0"), HiveDerivedExpression("{spend}" * "{forecasted_clicks}" / "{actual_clicks}" * "{recommended_bid}" / "{modified_bid}"), rollupExpression = MaxRollup)
            , HiveDerFactCol("min_rollup_spend", DecType(0, "0.0"), HiveDerivedExpression("{spend}" * "{forecasted_clicks}" / "{actual_clicks}" * "{recommended_bid}" / "{modified_bid}"), rollupExpression = MinRollup)
            , HiveDerFactCol("avg_rollup_spend", DecType(0, "0.0"), HiveDerivedExpression("{spend}" * "{forecasted_clicks}" / "{actual_clicks}" * "{recommended_bid}" / "{modified_bid}"), rollupExpression = AverageRollup)
            , HiveDerFactCol("custom_rollup_spend", DecType(0, "0.0"), HiveDerivedExpression("{spend}" * "{forecasted_clicks}" / "{actual_clicks}" * "{recommended_bid}" / "{modified_bid}"), rollupExpression = HiveCustomRollup(""))
            , HiveDerFactCol("noop_rollup_spend", DecType(0, "0.0"), HiveDerivedExpression("{spend}" * "{forecasted_clicks}" / "{actual_clicks}" * "{recommended_bid}" / "{modified_bid}"), rollupExpression = NoopRollup)
            , HiveDerFactCol("Average Bid", DecType(8, 2, "0.0"), GET_A_BY_B("{recommended_bid}", "{actual_impressions}"), rollupExpression = NoopRollup)
            , HiveDerFactCol("Bid Mod", DecType(8, 5, "0.0"), GET_A_BY_B("{recommended_bid}",  "{actual_clicks}"), rollupExpression = NoopRollup)
            , HiveDerFactCol("Bid Modifier Fact", DecType(8, 5), COALESCE("CASE WHEN {Bid Mod} = 0 THEN 1 WHEN IS_NAN({Bid Mod}) THEN 1 ELSE {Bid Mod} END", "1"), rollupExpression = NoopRollup )
            , HiveDerFactCol("Modified Bid Fact", DecType(8, 5), COALESCE("CASE WHEN {Bid Modifier Fact} = 0 THEN 1 WHEN IS_NAN({Bid Modifier Fact}) THEN 1 ELSE {Bid Modifier Fact} END", "1") * "{Average Bid}")
          )
          , ddlAnnotation = Option(
            HiveDDLAnnotation(Map(),
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
                ))), annotations = Set(HivePartitioningScheme("frequency"))
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
        , PubCol("bid_strategy", "Bid Strategy", InNotInEquality)
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
    import HiveExpression._
    import com.yahoo.maha.core.BaseExpressionTest._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "ce_stats_fact", DailyGrain, HiveEngine, Set(AdvertiserSchema),
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
          , HiveDerDimCol("call_start_time_utc", StrType(), GET_UTC_TIME_FROM_EPOCH("{call_start_time}","YYYY-MM-dd HH:mm:ss"))
          , HiveDerDimCol("call_end_time_utc", StrType(), GET_UTC_TIME_FROM_EPOCH("{call_end_time}","YYYY-MM-dd HH:mm:ss"))
          , HiveDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
          , HiveDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "W"))
          , HivePartDimCol("frequency", StrType(), partitionLevel = FirstPartitionLevel)
          , HivePartDimCol("utc_date", StrType(), partitionLevel = SecondPartitionLevel)
          , HivePartDimCol("utc_hour", StrType(), partitionLevel = ThirdPartitionLevel)
        ),
        Set(
        )
        , ddlAnnotation = Option(HiveDDLAnnotation(Map()
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

  def pubfact2(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import HiveExpression._
    import UDFHiveExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "ad_fact1", DailyGrain, HiveEngine, Set(AdvertiserSchema, ResellerSchema),
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
          , DimCol("stats_date", DateType("YYYY-MM-dd"))
          , DimCol("show_flag", IntType())
          , HiveDerDimCol("Month", DateType(), TEST_DATE_UDF("{stats_date}", "M"))
          , HiveDerDimCol("Week", DateType(), TEST_DATE_UDF("{stats_date}", "W"))
        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("s_impressions", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("engagement_count", IntType(10))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          //          , FactCol("Average CPC", DecType(), OracleCustomRollup("{spend}" / "{clicks}"))
          , FactCol("CTR", DecType(), HiveCustomRollup(SUM("{clicks}" /- "{impressions}")))
          , FactCol("weighted_position", DecType(0, "0.0"))
          , HiveDerFactCol("Engagement Rate", DecType(), "100" * TEST_MATH_UDF("{engagement_count}", "{impressions}"), rollupExpression = NoopRollup)
          , HiveDerFactCol("Paid Engagement Rate", DecType(), "100" * TEST_MATH_UDAF("{engagement_count}", "0", "0", "{clicks}", "{impressions}"), rollupExpression = NoopRollup)
          , HiveDerFactCol("Average CPC", DecType(), "{spend}" /- "{clicks}")
          , HiveDerFactCol("Average CPC Cents", DecType(), "{Average CPC}" * "100")
          , HiveDerFactCol("N Spend", DecType(), DECODE("{stats_source}", "1", "{spend}", "0.0"))
          , HiveDerFactCol("N Clicks", DecType(), DECODE("{stats_source}", "1", "{clicks}", "0.0"))
          , HiveDerFactCol("N Average CPC", DecType(), "{N Spend}" /- "{N Clicks}")
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), HiveCustomRollup(SUM("{weighted_position}" * "{impressions}") /- SUM("{impressions}")))
          , HiveDerFactCol("impression_share", IntType(), DECODE(MAX("{show_flag}"), "1", ROUND(SUM("{impressions}") /- SUM("{s_impressions}"), 4), "NULL"), rollupExpression = NoopRollup)
          , HiveDerFactCol("impression_share_rounded", IntType(), ROUND("{impression_share}", 5), rollupExpression = NoopRollup)
          , HiveDerFactCol("Click Rate", IntType(), SUM("{clicks}") /- SUM("{impressions}"), rollupExpression = NoopRollup)
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
          PubCol("advertiser_id", "Advertiser ID", InEquality),
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
    import HiveExpression._
    import com.yahoo.maha.core.BaseExpressionTest._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "s_stats_fact", MinuteGrain, HiveEngine, Set(AdvertiserSchema),
        Set(
          DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
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
          , DimCol("ad_format_id", IntType(3, (SharedDefinitions.adFormatIdToNameMap, "Other")))
          , DimCol("ad_format_type", IntType(8, (SharedDefinitions.adFormatIdtoTypeMap, "N/A")), alias = Option("ad_format_id"))
          , DimCol("ad_format_sub_type", IntType(8, (SharedDefinitions.adFormatIdtoSubTypeMap, "N/A")), alias = Option("ad_format_id"))
          , DimCol("column_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned")))
          , DimCol("column2_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned_with_singleton")))
          , HiveDerDimCol("Ad Group Start Date Full", StrType(), TIMESTAMP_TO_FORMATTED_DATE("{start_time}", "YYYY-MM-dd HH:mm:ss"))
          , HiveDerDimAggregateCol("Keyword Count",IntType(), COUNT_DISTINCT("{keyword_id}"))
          , HiveDerDimAggregateCol("Keyword Count Scaled", IntType(), COUNT("{keyword_id} * {stats_source} * 10"))
          , DimCol("internal_bucket_id", StrType())
          , HiveDerDimCol("click_exp_id", StrType(), REGEX_EXTRACT("internal_bucket_id", "(cl-)(.*?)(,|$)", 2, replaceMissingValue = true, "-3"))
          , HiveDerDimCol("col_test", StrType(), COL("CASE WHEN ({keyword_id}) is not null then ({keyword}) ELSE ({search_term}) END"))
          , HiveDerDimCol("col_modifiable_test", StrType(), COL_W_REPLACEMENTS("CASE WHEN ({keyword_id}) is not null then ({keyword}) ELSE ({search_term}) END"))
          , HivePartDimCol("frequency", StrType(), partitionLevel = FirstPartitionLevel)
          , HivePartDimCol("utc_date", StrType(), partitionLevel = SecondPartitionLevel)
          , HivePartDimCol("utc_hour", StrType(), partitionLevel = ThirdPartitionLevel)
        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          , FactCol("weighted_position", DecType(0, "0.0"))
          , HiveDerFactCol("max_price_type", IntType(), "{price_type}", rollupExpression = MaxRollup)
          , HiveDerFactCol("Average CPC", DecType(), "{spend}" /- "{clicks}", rollupExpression = NoopRollup)
          , HiveDerFactCol("Average CPC Cents", DecType(), "{Average CPC}" * "100", rollupExpression = NoopRollup)
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), HiveCustomRollup(SUM("{weighted_position}" * "{impressions}") /- SUM("{impressions}")))
          , HiveDerFactCol("col_test_metric", DecType(), COL("CASE WHEN SUM(spend) > 0 THEN SUM(clicks) / 10 ELSE SUM(impressions) END"))
          , HiveDerFactCol("col_modifiable_test_metric", DecType(), COL_W_REPLACEMENTS("CASE WHEN SUM(spend) > 0 THEN SUM(clicks) / 10 ELSE SUM(impressions) END"))
        )
      )
    }
      .toPublicFact("s_stats_minute",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("ad_id", "Ad ID", EqualityFieldEquality),
          PubCol("ad_group_id", "Ad Group ID", InEqualityFieldEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("account_id", "Advertiser ID", InEquality),
          PubCol("keyword_id", "Keyword ID", InEquality),
          PubCol("Keyword Count", "Keyword Count", Set.empty),
          PubCol("Keyword Count Scaled", "Keyword Count Scaled", Set.empty),
          PubCol("keyword", "Keyword", InEquality),
          PubCol("search_term", "Search Term", InEquality),
          PubCol("delivered_match_type", "Delivered Match Type", InEquality),
          PubCol("stats_source", "Source", Equality, incompatibleColumns = Set("Source Name")),
          PubCol("source_name", "Source Name", Equality, incompatibleColumns = Set("Source")),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty),
          PubCol("ad_format_id", "Ad Format Name", InNotInEqualityNotEqualsLikeNullNotNull),
          PubCol("ad_format_type", "Ad Format Type", InNotInEqualityNotEqualsLikeNullNotNull),
          PubCol("ad_format_sub_type", "Ad Format Sub Type", InNotInEqualityNotEqualsLikeNullNotNull),
          PubCol("column_id", "Column ID", Equality),
          PubCol("column2_id", "Column2 ID", Equality),
          PubCol("Ad Group Start Date Full", "Ad Group Start Date Full", InEquality),
          PubCol("network_type", "Network ID", InEquality),
          PubCol("device_id", "Device ID", InEquality),
          PubCol("internal_bucket_id", "Internal Bucket ID", InEquality),
          PubCol("click_exp_id", "Click Exp ID", InEquality),
          PubCol("col_test", "Test COL Function", InEquality),
          PubCol("col_modifiable_test", "Test Modifiable COL Function", InEquality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InNotInBetweenEqualityNotEqualsGreaterLesser),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("spend", "Spend", FieldEquality),
          PublicFactCol("avg_pos", "Average Position", Set.empty),
          PublicFactCol("max_bid", "Max Bid", FieldEquality),
          PublicFactCol("Average CPC", "Average CPC", InBetweenEquality),
          PublicFactCol("Average CPC Cents", "Average CPC Cents", InBetweenEquality),
          PublicFactCol("max_price_type", "Max Price Type", Equality),
          PublicFactCol("col_test_metric", "Test Metric COL", InEquality),
          PublicFactCol("col_modifiable_test_metric", "Test Mod Metric COL", InEquality)
        ),
        Set(),
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }

  def pubfact4(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import HiveExpression._
    import com.yahoo.maha.core.BaseExpressionTest._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "s_stats_timezone_fact", MinuteGrain, HiveEngine, Set(AdvertiserSchema),
        Set(
          DimCol("account_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , HiveDerDimCol("Day", DateType(), TIME_FORMAT_WITH_TIMEZONE("{utc_date}", "yyyyMMdd"))
          , HiveDerDimCol("Hour", DateType(), TIME_FORMAT_WITH_TIMEZONE("{utc_hour}", "yyyyMMddHH"))
          , HiveDerDimCol("derived_day", DateType(), COALESCE("{utc_date}", "'2023-01-01'"))
          , HiveDerDimCol("Derived Day", DateType(), TIME_FORMAT_WITH_TIMEZONE("{derived_day}", "yyyyMMdd"))
          , HivePartDimCol("frequency", StrType(), partitionLevel = FirstPartitionLevel)
          , HivePartDimCol("utc_date", StrType(), partitionLevel = SecondPartitionLevel)
          , HivePartDimCol("utc_hour", StrType(), partitionLevel = ThirdPartitionLevel)
        ),
        Set(
          FactCol("impressions", IntType(3, 1))
        )
      )
    }
      .toPublicFact("s_stats_timezone",
        Set(
          PubCol("Day", "Day", InBetweenEquality),
          PubCol("Hour", "Hour", InBetweenEquality),
          PubCol("Derived Day", "Derived Day", InBetweenEquality),
          PubCol("account_id", "Advertiser ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InNotInBetweenEqualityNotEqualsGreaterLesser)
        ),
        Set(),
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }

  case object TestDateUDFRegistration extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION dateUDF as 'com.yahoo.maha.query.hive.udf.TestDateUDF';"
  }

  case object TestDecodeUDFRegistration extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION decodeUDF as 'com.yahoo.maha.query.hive.udf.TestDecodeUDF';"
  }

  case object TestMathUDFRegistration extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION mathUDF as 'com.yahoo.maha.query.hive.udf.TestMathUDF';"
  }

  case object TestMathUDAFRegistration extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION mathUDAF as 'com.yahoo.maha.query.hive.udf.TestMathUDAF';"
  }

  object UDFHiveExpression {

    import HiveExpression._

    implicit val uDFRegistrationFactory = DefaultUDFRegistrationFactory

    uDFRegistrationFactory.register(TestDateUDFRegistration)
    uDFRegistrationFactory.register(TestDecodeUDFRegistration)
    uDFRegistrationFactory.register(TestMathUDFRegistration)
    uDFRegistrationFactory.register(TestMathUDAFRegistration)


    case class TEST_DATE_UDF(s: HiveExp, fmt: String) extends UDFHiveExpression(TestDateUDFRegistration) {
      def hasRollupExpression = s.hasRollupExpression

      def hasNumericOperation = s.hasNumericOperation

      def asString: String = s"dateUDF(${s.asString}, '$fmt')"
    }

    case class DECODE(args: HiveExp*) extends UDFHiveExpression(TestDecodeUDFRegistration) {
      val hasRollupExpression = args.exists(_.hasRollupExpression)
      val hasNumericOperation = args.exists(_.hasNumericOperation)
      if (args.length < 3) throw new IllegalArgumentException("Usage: DECODE( expression , search , result [, search , result]... [, default] )")
      val argStrs = args.map(_.asString).mkString(", ")
      def asString: String = s"decodeUDF($argStrs)"
    }

    case class TEST_MATH_UDF(args: HiveExp*) extends UDFHiveExpression(TestDecodeUDFRegistration) {
      val hasRollupExpression = args.exists(_.hasRollupExpression)
      val hasNumericOperation = args.exists(_.hasNumericOperation)
      val argStrs = args.map(_.asString).mkString(", ")
      def asString: String = s"mathUDF($argStrs)"
    }

    case class TEST_MATH_UDAF(args: HiveExp*) extends UDFHiveExpression(TestDecodeUDFRegistration) {
      val hasRollupExpression = args.exists(_.hasRollupExpression)
      val hasNumericOperation = args.exists(_.hasNumericOperation)
      val argStrs = args.map(_.asString).mkString(", ")
      def asString: String = s"mathUDAF($argStrs)"
    }

  }
}
