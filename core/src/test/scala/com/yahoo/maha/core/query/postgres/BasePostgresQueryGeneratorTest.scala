// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.postgres

import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.lookup.LongRangeLookup
import com.yahoo.maha.core.query.druid.{DruidQueryGenerator, SyncDruidQueryOptimizer}
import com.yahoo.maha.core.query.{BaseQueryGeneratorTest, SharedDimSchema}
import com.yahoo.maha.core.registry.RegistryBuilder
import com.yahoo.maha.core.request.{AsyncRequest, SyncRequest}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Created by hiral on 1/15/16.
 */
trait BasePostgresQueryGeneratorTest
  extends AnyFunSuite with Matchers with BeforeAndAfterAll with BaseQueryGeneratorTest with SharedDimSchema {

  override protected def defaultFactEngine: Engine = PostgresEngine

  override protected def beforeAll(): Unit = {
    PostgresQueryGenerator.register(queryGeneratorRegistry,DefaultPartitionColumnRenderer)
    DruidQueryGenerator.register(queryGeneratorRegistry, queryOptimizer = new SyncDruidQueryOptimizer(timeout = 5000))
  }

  override protected[this] def registerFacts(forcedFilters: Set[ForcedFilter], registryBuilder: RegistryBuilder): Unit = {
    registryBuilder.register(pubfact(forcedFilters))
    registryBuilder.register(pubfactV1(forcedFilters))
    registryBuilder.register(pubfact2(forcedFilters))
    registryBuilder.register(pubfact3(forcedFilters))
    registryBuilder.register(pubfact4(forcedFilters))
    registryBuilder.register(pubfact5(forcedFilters))
    registryBuilder.register(pubfact6(forcedFilters))
    registryBuilder.register(pubfact7(forcedFilters))
    registryBuilder.register(pubfact8(forcedFilters))
    registryBuilder.register(pubfact9(forcedFilters))
    registryBuilder.register(pubFactCombined(forcedFilters))
    registryBuilder.register(pubfact11(forcedFilters))

  }

  def pubfact(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import PostgresExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "fact1", DailyGrain, PostgresEngine, Set(AdvertiserSchema, ResellerSchema),
        Set(
          DimCol("keyword_id", IntType(), annotations = Set(ForeignKey("keyword")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("source_name", IntType(3, (Map(1 -> "Native", 2 -> "Search", -1 -> "UNKNOWN"), "UNKNOWN")), alias = Option("stats_source"))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("device_id", IntType(3, (Map(1 -> "Desktop", 2 -> "Tablet", 3 -> "SmartPhone", -1 -> "UNKNOWN"), "UNKNOWN")))
          , DimCol("network_type", StrType(100, (Map("TEST_PUBLISHER" -> "Test Publisher", "CONTENT_SYNDICATION" -> "Content Syndication", "EXTERNAL" -> "Yahoo Partners" ,  "INTERNAL" -> "Yahoo Properties"), "NONE")))
          , DimCol("start_time", IntType())
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("target_page_url", StrType(), annotations = Set(CaseInsensitive))
          , DimCol("stats_date", DateType("YYYY-MM-DD"))
          , DimCol("column_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned")))
          , DimCol("column2_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned_with_singleton")))
          , PostgresDerDimCol("Ad Group Start Date Full", StrType(), TIMESTAMP_TO_FORMATTED_DATE("{start_time}", "YYYY-MM-dd HH:mm:ss"))
          , PostgresDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
          , PostgresDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "W"))
        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          , FactCol("Average CPC", DecType(), PostgresCustomRollup(SUM("{spend}") / SUM("{clicks}")))
          , FactCol("CTR", DecType(), PostgresCustomRollup(SUM("{clicks}" /- "{impressions}")))
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), PostgresCustomRollup(SUM("{avg_pos}" * "{impressions}") /- SUM("{impressions}")))
          , FactCol("Count", IntType(), rollupExpression = CountRollup, alias = Option("count_col"))
          , FactCol("Avg", IntType(), rollupExpression = AverageRollup, alias = Option("avg_col"))
          , FactCol("Max", IntType(), rollupExpression = MaxRollup, alias = Option("max_col"))
          , FactCol("Min", IntType(), rollupExpression = MinRollup, alias = Option("min_col"))
        ),
        annotations = Set(
          PostgresFactStaticHint("PARALLEL_INDEX(cb_campaign_k_stats 4)"),
          PostgresFactDimDrivenHint("PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4)")
        )
      )
    }
      .newRollUp("fact2"
        , "fact1"
        , discarding = Set("ad_id")
        , columnAliasMap = Map("price_type" -> "pricing_type", "source_name" -> "stats_source")
        , overrideAnnotations = Set(
          PostgresFactConditionalHint(FactCondition(Option(true)), "CONDITIONAL_HINT1")
          , PostgresFactConditionalHint(FactCondition(Option(true), Option(false)), "CONDITIONAL_HINT2")
          , PostgresFactConditionalHint(FactCondition(Option(true), Option(false), Option(true)), "CONDITIONAL_HINT3")
          , PostgresFactConditionalHint(FactCondition(Option(true), Option(false), Option(false), Option(false)), "CONDITIONAL_HINT4")
          , PostgresFactConditionalHint(FactCondition(None, minRowsEstimate = Option(900L))
            , "CONDITIONAL_HINT5")
        ), availableOnwardsDate = Some("2010-01-01")
      ).toPublicFact("k_stats",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("keyword_id", "Keyword ID", InEquality),
          PubCol("ad_id", "Ad ID", InEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("network_type", "Network Type", InEquality),
          PubCol("stats_source", "Source", EqualityFieldEquality, incompatibleColumns = Set("Source Name")),
          PubCol("source_name", "Source Name", InNotInBetweenEqualityNotEqualsGreaterLesser, incompatibleColumns = Set("Source")),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", FieldEquality, isImageColumn = true),
          PubCol("target_page_url", "Source URL", FieldEquality),
          PubCol("column_id", "Column ID", Equality),
          PubCol("column2_id", "Column2 ID", Equality),
          PubCol("Ad Group Start Date Full", "Ad Group Start Date Full", InEquality),
          PubCol("Month", "Month", Equality),
          PubCol("Week", "Week", Equality),
          PubCol("device_id", "Device ID", InEquality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InNotInBetweenEqualityNotEqualsGreaterLesser),
          PublicFactCol("impressions", "Total Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", InBetweenEqualityFieldEquality),
          PublicFactCol("spend", "Spend", FieldEquality),
          PublicFactCol("avg_pos", "Average Position", FieldEquality),
          PublicFactCol("max_bid", "Max Bid", Set.empty),
          PublicFactCol("Average CPC", "Average CPC", InBetweenEquality),
          PublicFactCol("CTR", "CTR", InBetweenEquality),
          PublicFactCol("Count", "Count", InBetweenEquality),
          PublicFactCol("Avg", "Avg", InBetweenEquality),
          PublicFactCol("Max", "Max", InBetweenEquality),
          PublicFactCol("Min", "Min", InBetweenEquality)
        ),
        Set(EqualityFilter("Source", "2", true, true)),
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }

  def pubfactV1(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    val builder = ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      import HiveExpression._
      Fact.newFact(
        "fact_hive", DailyGrain, HiveEngine, Set(AdvertiserSchema)
        , Set(
          DimCol("id", IntType(), annotations = Set(ForeignKey("keyword")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
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
      import PostgresExpression._
      builder.withAlternativeEngine(
        "fact_postgres", "fact_hive", PostgresEngine
        , overrideDimCols = Set(
          DimCol("stats_date", DateType("YYYY-MM-DD"))
        )
        , overrideFactCols = Set(
          FactCol("impressions", IntType(3, 1))
          , PostgresDerFactCol("Average CPC", DecType(), "{spend}" / "{clicks}")
          , PostgresDerFactCol("CTR", DecType(), "{clicks}" /- "{impressions}")
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), PostgresCustomRollup(SUM("{avg_pos}" * "{impressions}") /- "{impressions}"))
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


    builder.toPublicFact("k_stats",
      Set(
        PubCol("id", "Keyword ID", InEquality),
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
      getMaxDaysWindow, getMaxDaysLookBack, revision = 1
    )
  }

  def pubfact2(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import PostgresExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "ad_fact1", DailyGrain, PostgresEngine, Set(AdvertiserSchema, ResellerSchema),
        Set(
          DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("restaurant_id", IntType(), alias = Option("advertiser_id"), annotations = Set(ForeignKey("restaurant")))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("start_time", IntType())
          , DimCol("stats_date", DateType("YYYY-MM-DD"))
          , DimCol("show_flag", IntType())
          , PostgresDerDimCol("Business Name", StrType(), DECODE_DIM("{stats_source}", "1", "'Native'", "2", "'Search'", "'Unknown'"))
          , PostgresDerDimCol("Business Name 2", StrType(), DECODE_DIM("{stats_source}", "1", "'Expensive'", "2", "'Cheap'", "'Unknown'"))
          , PostgresDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
          , PostgresDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "W"))
        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("s_impressions", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
//          , FactCol("Average CPC", DecType(), PostgresCustomRollup("{spend}" / "{clicks}"))
          , FactCol("CTR", DecType(), PostgresCustomRollup(SUM("{clicks}" /- "{impressions}")))
          , FactCol("User Count", DecType(), alias=Option("user_count"))
          , PostgresDerFactCol("Average CPC", DecType(), "{spend}" /- "{clicks}")
          , PostgresDerFactCol("Average CPC Cents", DecType(), "{Average CPC}" * "100")
          , PostgresDerFactCol("N Spend", DecType(), DECODE("{stats_source}", "1", "{spend}", "0.0"))
          , PostgresDerFactCol("N Clicks", DecType(), DECODE("{stats_source}", "1", "{clicks}", "0.0"))
          , PostgresDerFactCol("N Average CPC", DecType(), "{N Spend}" /- "{N Clicks}")
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), PostgresCustomRollup(SUM("{avg_pos}" * "{impressions}") /- SUM("{impressions}")))
          , PostgresDerFactCol("impression_share", IntType(), DECODE(MAX("{show_flag}"), "1", ROUND(SUM("{impressions}") /- SUM("{s_impressions}"), 4), "NULL"), rollupExpression = NoopRollup)
          , PostgresDerFactCol("impression_share_rounded", IntType(), ROUND("{impression_share}", 5), rollupExpression = NoopRollup)
          , FactCol("Count", IntType(), rollupExpression = CountRollup)
          , FactCol("Custom", IntType(6, 0, 0, 10), rollupExpression = PostgresCustomRollup(SUM("{clicks}" * "{max_bid}")), alias = Option("custom_col"))
          , FactCol("Avg", IntType(6, 0, 0, 100000), rollupExpression = AverageRollup, alias = Option("avg_col"))
          , FactCol("Max", IntType(), rollupExpression = MaxRollup, alias = Option("max_col"))
          , FactCol("Min", IntType(), rollupExpression = MinRollup, alias = Option("min_col"))
        ),
        annotations = Set(
          PostgresFactStaticHint("PARALLEL_INDEX(cb_ad_stats 4)"),
          PostgresFactDimDrivenHint("PUSH_PRED PARALLEL_INDEX(cb_ad_stats 4)")
        )
      )
    }
      .toPublicFact("performance_stats",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("ad_id", "Ad ID", InEqualityFieldEquality),
          PubCol("ad_group_id", "Ad Group ID", InEqualityFieldEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("restaurant_id", "Restaurant ID", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("Business Name", "Business Name", InEqualityFieldEquality),
          PubCol("Business Name 2", "Business Name 2", InEqualityFieldEquality),
          PubCol("Month", "Month", Equality),
          PubCol("Week", "Week", Equality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEqualityFieldEquality),
          PublicFactCol("impressions", "Total Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", InBetweenEqualityFieldEquality),
          PublicFactCol("spend", "Spend", InBetweenEqualityFieldEquality),
          PublicFactCol("User Count", "User Count", InBetweenEqualityFieldEquality),
          PublicFactCol("avg_pos", "Average Position", Set.empty),
          PublicFactCol("max_bid", "Max Bid", Set.empty),
          PublicFactCol("Average CPC", "Average CPC", InBetweenEquality),
          PublicFactCol("Average CPC Cents", "Average CPC Cents", InBetweenEquality),
          PublicFactCol("CTR", "CTR", InBetweenEquality),
          PublicFactCol("N Spend", "N Spend", InBetweenEqualityFieldEquality),
          PublicFactCol("N Clicks", "N Clicks", InBetweenEquality),
          PublicFactCol("N Average CPC", "N Average CPC", InBetweenEquality),
          PublicFactCol("impression_share_rounded", "Impression Share", InBetweenEquality),
          PublicFactCol("Count", "Count", InBetweenEquality),
          PublicFactCol("Custom", "Custom", InBetweenEquality),
          PublicFactCol("Avg", "Avg", InBetweenEquality),
          PublicFactCol("Max", "Max", InBetweenEquality),
          PublicFactCol("spend", "Duplicate Spend", InBetweenEquality),
          PublicFactCol("Min", "Min", InBetweenEquality)
        ),
        forcedFilters,
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }
  // New Partitioning Scheme
  def pubfact3(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import PostgresExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "k_stats_new_partitioning", DailyGrain, PostgresEngine, Set(AdvertiserSchema, ResellerSchema),
        Set(
          DimCol("keyword_id", IntType(), annotations = Set(ForeignKey("keyword")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("start_time", IntType())
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("stats_date", DateType("YYYY-MM-DD"))
          , DimCol("column_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned")))
          , DimCol("column2_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned_with_singleton")))
          , PostgresDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
          , PostgresDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "W"))
          , PostgresPartDimCol("frequency", StrType(), partitionLevel = FirstPartitionLevel)
          , PostgresPartDimCol("utc_date", DateType("YYYY-MM-DD"), partitionLevel = SecondPartitionLevel)
        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          , FactCol("Average CPC", DecType(), PostgresCustomRollup(SUM("{spend}") / SUM("{clicks}")))
          , FactCol("CTR", DecType(), PostgresCustomRollup(SUM("{clicks}" /- "{impressions}")))
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), PostgresCustomRollup(SUM("{avg_pos}" * "{impressions}") /- SUM("{impressions}")))
        ),
        annotations = Set(
          PostgresFactStaticHint("PARALLEL_INDEX(cb_campaign_k_stats 4)"),
          PostgresFactDimDrivenHint("PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4)"),
          PostgresPartitioningScheme("frequency")
        )
      )
    }
      .newRollUp("k_stats_fact1", "k_stats_new_partitioning", discarding = Set("ad_id"), columnAliasMap = Map("price_type" -> "pricing_type"), availableOnwardsDate = Some("2010-01-01"))
      .toPublicFact("k_stats_new",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("frequency", "Frequency", InEquality),
          PubCol("keyword_id", "Keyword ID", InEquality),
          PubCol("ad_id", "Ad ID", InEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty),
          PubCol("column_id", "Column ID", Equality),
          PubCol("column2_id", "Column2 ID", Equality),
          PubCol("Month", "Month", Equality),
          PubCol("Week", "Week", Equality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("impressions", "Total Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("spend", "Spend", Set.empty),
          PublicFactCol("avg_pos", "Average Position", Set.empty),
          PublicFactCol("max_bid", "Max Bid", Set.empty),
          PublicFactCol("Average CPC", "Average CPC", InBetweenEquality),
          PublicFactCol("CTR", "CTR", InBetweenEquality)
        ),
        Set(EqualityFilter("Source", "2", isForceFilter = true)),
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }

  // New Partitioning Scheme
  def pubfact4(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import PostgresExpression._

    val tableOne  = {
      ColumnContext.withColumnContext {
        implicit dc: ColumnContext =>
          Fact.newFactForView(
            "k_stats_new_partitioning_one", DailyGrain, PostgresEngine, Set(AdvertiserSchema, ResellerSchema),
            Set(
              DimCol("keyword_id", IntType(), annotations = Set(ForeignKey("keyword")))
              , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
              , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
              , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
              , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
              , ConstDimCol("stats_source", IntType(3), "1")
              , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
              , DimCol("start_time", IntType())
              , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
              , DimCol("stats_date", DateType("YYYY-MM-DD"))
              , DimCol("column_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned")))
              , DimCol("column2_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned_with_singleton")))
              , PostgresDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
              , PostgresDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "W"))
              , PostgresPartDimCol("frequency", StrType(), partitionLevel = FirstPartitionLevel)
              , PostgresPartDimCol("utc_date", DateType("YYYY-MM-DD"), partitionLevel = SecondPartitionLevel)
            ),
            Set(
              FactCol("impressions", IntType(3, 1))
              , FactCol("clicks", IntType(3, 0, 1, 800))
              , ConstFactCol("constantFact", IntType(3, 0, 1, 800), "0")
              , FactCol("spend", DecType(0, "0.0"))
              , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
              , FactCol("Average CPC", DecType(), PostgresCustomRollup(SUM("{spend}") / SUM("{clicks}")))
              , FactCol("CTR", DecType(), PostgresCustomRollup(SUM("{clicks}" /- "{impressions}")))
              , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), PostgresCustomRollup(SUM("{avg_pos}" * "{impressions}") /- SUM("{impressions}")))
            ),
            annotations = Set(
              PostgresFactStaticHint("PARALLEL_INDEX(cb_campaign_k_stats 4)"),
              PostgresFactDimDrivenHint("PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4)"),
              PostgresPartitioningScheme("frequency")
            )
          )
      }
    }

    val tableTwo  = {
      ColumnContext.withColumnContext {
        implicit dc: ColumnContext =>
          Fact.newFactForView(
            "k_stats_new_partitioning_two", DailyGrain, PostgresEngine, Set(AdvertiserSchema, ResellerSchema),
            Set(
              DimCol("keyword_id", IntType(), annotations = Set(ForeignKey("keyword")))
              , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
              , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
              , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
              , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
              , ConstDimCol("stats_source", IntType(3), "2")
              , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
              , DimCol("start_time", IntType())
              , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
              , DimCol("stats_date", DateType("YYYY-MM-DD"))
              , DimCol("column_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned")))
              , DimCol("column2_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned_with_singleton")))
              , PostgresDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
              , PostgresDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "W"))
              , PostgresPartDimCol("frequency", StrType(), partitionLevel = FirstPartitionLevel)
              , PostgresPartDimCol("utc_date", DateType("YYYY-MM-DD"), partitionLevel = SecondPartitionLevel)
            ),
            Set(
              FactCol("impressions", IntType(3, 1))
              , FactCol("clicks", IntType(3, 0, 1, 800))
              , ConstFactCol("constantFact", IntType(3, 0, 1, 800), "0")
              , FactCol("spend", DecType(0, "0.0"))
              , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
              , FactCol("Average CPC", DecType(), PostgresCustomRollup(SUM("{spend}") / SUM("{clicks}")))
              , FactCol("CTR", DecType(), PostgresCustomRollup(SUM("{clicks}" /- "{impressions}")))
              , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), PostgresCustomRollup(SUM("{avg_pos}" * "{impressions}") /- SUM("{impressions}")))
            ),
            annotations = Set(
              PostgresFactStaticHint("PARALLEL_INDEX(cb_campaign_k_stats 4)"),
              PostgresFactDimDrivenHint("PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4)"),
              PostgresPartitioningScheme("frequency")
            )
          )
      }
    }
    val view = UnionView("k_stats_new_partitioning", Seq(tableOne, tableTwo))


    ColumnContext.withColumnContext {
      implicit dc: ColumnContext =>
      Fact.newUnionView(view, DailyGrain, PostgresEngine, Set(AdvertiserSchema, ResellerSchema),
        Set(
          DimCol("keyword_id", IntType(), annotations = Set(ForeignKey("keyword")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("start_time", IntType())
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("stats_date", DateType("YYYY-MM-DD"))
          , DimCol("column_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned")))
          , DimCol("column2_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned_with_singleton")))
          , PostgresDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
          , PostgresDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "W"))
          , PostgresPartDimCol("frequency", StrType(), partitionLevel = FirstPartitionLevel)
          , PostgresPartDimCol("utc_date", DateType("YYYY-MM-DD"), partitionLevel = SecondPartitionLevel)
        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("constantFact", IntType(3, 0, 1, 800))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          , FactCol("Average CPC", DecType(), PostgresCustomRollup(SUM("{spend}") / SUM("{clicks}")))
          , FactCol("CTR", DecType(), PostgresCustomRollup(SUM("{clicks}" /- "{impressions}")))
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), PostgresCustomRollup(SUM("{avg_pos}" * "{impressions}") /- SUM("{impressions}")))
        ),
        annotations = Set(
          PostgresFactStaticHint("PARALLEL_INDEX(cb_campaign_k_stats 4)"),
          PostgresFactDimDrivenHint("PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4)"),
          PostgresPartitioningScheme("frequency")
        )
      )
    }
      .newRollUp("k_stats_fact1", "k_stats_new_partitioning", discarding = Set("ad_id"), columnAliasMap = Map("price_type" -> "pricing_type"), availableOnwardsDate = Some("2010-01-01"))
      .toPublicFact("keyword_view_test",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("keyword_id", "Keyword ID", InEquality),
          PubCol("ad_id", "Ad ID", InEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty),
          PubCol("column_id", "Column ID", Equality),
          PubCol("column2_id", "Column2 ID", Equality),
          PubCol("Month", "Month", Equality),
          PubCol("Week", "Week", Equality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("impressions", "Total Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("spend", "Spend", Set.empty),
          PublicFactCol("avg_pos", "Average Position", Set.empty),
          PublicFactCol("max_bid", "Max Bid", Set.empty),
          PublicFactCol("Average CPC", "Average CPC", InBetweenEquality),
          PublicFactCol("CTR", "CTR", InBetweenEquality)
        ),
        Set(EqualityFilter("Source", "2", isForceFilter = true)),
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }

  def pubfact5(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {

    val campaignStats  = {
      import com.yahoo.maha.core.PostgresExpression._
      ColumnContext.withColumnContext {
        implicit dc: ColumnContext =>
          Fact.newFactForView(
            "campaign_stats", DailyGrain, PostgresEngine, Set(AdvertiserSchema, ResellerSchema),
            Set(
              DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
              , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
              , DimCol("stats_date", DateType("YYYY-MM-DD"))
              , PostgresDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
              , PostgresDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "w"))
            ),
            Set(
              FactCol("impressions", IntType(3, 1))
              , FactCol("clicks", IntType(3, 0, 1, 800))
              , FactCol("spend", DecType(0, "0.0"))
            )
          )
      }
    }

    val campaignAdjustment  = {
      import com.yahoo.maha.core.PostgresExpression._
      ColumnContext.withColumnContext {
        implicit dc: ColumnContext =>
          Fact.newFactForView(
            "campaign_adjustments", DailyGrain, PostgresEngine, Set(AdvertiserSchema, ResellerSchema),
            Set(
              DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
              , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
              , DimCol("stats_date", DateType("YYYY-MM-DD"))
              , PostgresDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
              , PostgresDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "w"))
            ),
            Set(
              FactCol("impressions", IntType(3, 1))
              , FactCol("clicks", IntType(3, 0, 1, 800))
              , FactCol("spend", DecType(0, "0.0"))
            )
          )
      }
    }

    val campaignAdjView = UnionView("campaign_adjustment_view", Seq(campaignStats, campaignAdjustment))

    val accountStats = campaignStats.copyWith("account_stats", Set("campaign_id"), Map.empty)
    val accountAdjustment = campaignAdjustment.copyWith("account_adjustment", Set("campaign_id"), Map.empty)

    val accountAdjustmentView = UnionView("account_adjustment_view", Seq(accountStats, accountAdjustment))

    ColumnContext.withColumnContext {
      import com.yahoo.maha.core.PostgresExpression._
      implicit dc: ColumnContext =>
        Fact.newUnionView(campaignAdjView, DailyGrain, PostgresEngine, Set(AdvertiserSchema, ResellerSchema),
          Set(
            DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("stats_date", DateType("YYYY-MM-DD"))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
            , PostgresDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
            , PostgresDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "w"))
          ),
          Set(
            FactCol("impressions", IntType(3, 1))
            , FactCol("clicks", IntType(3, 0, 1, 800))
            , FactCol("spend", DecType(0, "0.0"))
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

  def pubfact6(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {

    val publisherStats  = {
      ColumnContext.withColumnContext {
        implicit dc: ColumnContext =>
          Fact.newFact(
            "v_publisher_stats", DailyGrain, PostgresEngine, Set(PublisherSchema),
            Set(
              DimCol("publisher_id", IntType())
              , DimCol("date_sid", IntType(), annotations = Set(DayColumn("YYYYMMDD")))
            ),
            Set(
              FactCol("impressions", IntType(3, 1))
              , FactCol("clicks", IntType(3, 0, 1, 800))
              , FactCol("spend", DecType(0, "0.0"))
            )
          )
      }
    }

    publisherStats.toPublicFact("publisher_stats_int",
        Set(
          PubCol("date_sid", "Day", InBetweenEquality)
          , PubCol("publisher_id", "Publisher ID", InEquality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("spend", "Spend", Set.empty)
        ), Set(),  getMaxDaysWindow, getMaxDaysLookBack
      )
  }

  def pubfact7(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {

    val publisherStats  = {
      ColumnContext.withColumnContext {
        implicit dc: ColumnContext =>
          Fact.newFact(
            "v_publisher_stats_str", DailyGrain, PostgresEngine, Set(PublisherSchema),
            Set(
              DimCol("publisher_id", IntType())
              , DimCol("date_sid", StrType(), annotations = Set(DayColumn("YYYYMMDD")))
            ),
            Set(
              FactCol("impressions", IntType(3, 1))
              , FactCol("clicks", IntType(3, 0, 1, 800))
              , FactCol("spend", DecType(0, "0.0"))
            )
          )
      }
    }

    publisherStats.toPublicFact("publisher_stats_str",
      Set(
        PubCol("date_sid", "Day", InBetweenEquality)
        , PubCol("publisher_id", "Publisher ID", InEquality)
      ),
      Set(
        PublicFactCol("impressions", "Impressions", InBetweenEquality),
        PublicFactCol("clicks", "Clicks", InBetweenEquality),
        PublicFactCol("spend", "Spend", Set.empty)
      ), Set(),  getMaxDaysWindow, getMaxDaysLookBack
    )
  }

  def pubfact8(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import PostgresExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "fact1", DailyGrain, PostgresEngine, Set(AdvertiserSchema, ResellerSchema),
        Set(
          DimCol("keyword_id", IntType(), annotations = Set(ForeignKey("keyword")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("source_name", IntType(3, (Map(1 -> "Native", 2 -> "Search", -1 -> "UNKNOWN"), "UNKNOWN")), alias = Option("stats_source"))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("device_id", IntType(3, (Map(1 -> "Desktop", 2 -> "Tablet", 3 -> "SmartPhone", -1 -> "UNKNOWN"), "UNKNOWN")))
          , DimCol("network_type", StrType(100, (Map("TEST_PUBLISHER" -> "Test Publisher", "CONTENT_SYNDICATION" -> "Content Syndication", "EXTERNAL" -> "Yahoo Partners" ,  "INTERNAL" -> "Yahoo Properties"), "NONE")))
          , DimCol("start_time", IntType())
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("stats_date", DateType("YYYY-MM-DD"))
          , DimCol("column_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned")))
          , DimCol("column2_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned_with_singleton")))
          , PostgresDerDimCol("Ad Group Start Date Full", StrType(), TIMESTAMP_TO_FORMATTED_DATE("{start_time}", "YYYY-MM-dd HH:mm:ss"))
          , PostgresDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
          , PostgresDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "W"))
        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          , FactCol("Average CPC", DecType(), PostgresCustomRollup(SUM("{spend}") / SUM("{clicks}")))
          , FactCol("CTR", DecType(), PostgresCustomRollup(SUM("{clicks}" /- "{impressions}")))
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), PostgresCustomRollup(SUM("{avg_pos}" * "{impressions}") /- SUM("{impressions}")))
        ),
        annotations = Set(
          PostgresFactStaticHint("PARALLEL_INDEX(cb_campaign_k_stats 4)"),
          PostgresFactDimDrivenHint("PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4)"),
          PostgresPartitioningScheme("frequency")
        )
      )
    }
      .newRollUp("fact2", "fact1", discarding = Set("ad_id"), columnAliasMap = Map("price_type" -> "pricing_type", "source_name" -> "stats_source"), availableOnwardsDate = Some("2010-01-01"))
      .toPublicFact("k_stats_new_freq",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("keyword_id", "Keyword ID", InEquality),
          PubCol("ad_id", "Ad ID", InEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("network_type", "Network Type", InEquality),
          PubCol("stats_source", "Source", Equality, incompatibleColumns = Set("Source Name")),
          PubCol("source_name", "Source Name", InEquality, incompatibleColumns = Set("Source")),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty),
          PubCol("column_id", "Column ID", Equality),
          PubCol("column2_id", "Column2 ID", Equality),
          PubCol("Ad Group Start Date Full", "Ad Group Start Date Full", InEquality),
          PubCol("Month", "Month", Equality),
          PubCol("Week", "Week", Equality),
          PubCol("device_id", "Device ID", InEquality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("impressions", "Total Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("spend", "Spend", Set.empty),
          PublicFactCol("avg_pos", "Average Position", Set.empty),
          PublicFactCol("max_bid", "Max Bid", Set.empty),
          PublicFactCol("Average CPC", "Average CPC", InBetweenEquality),
          PublicFactCol("CTR", "CTR", InBetweenEquality)
        ),
        Set(EqualityFilter("Source", "2", true, true)),
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }

  def pubfact9(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {

    val publisherStats  = {
      ColumnContext.withColumnContext {
        implicit dc: ColumnContext =>
          Fact.newFact(
            "v_publisher_stats2", HourlyGrain, PostgresEngine, Set(PublisherSchema),
            Set(
              DimCol("publisher_id", IntType())
              , DimCol("date_sid", IntType(), annotations = Set(DayColumn("YYYYMMDD")))
            ),
            Set(
              FactCol("impressions", IntType(3, 1))
              , FactCol("clicks", IntType(3, 0, 1, 800))
              , FactCol("spend", DecType(0, "0.0"))
            )
          )
      }
    }

    publisherStats.toPublicFact("publisher_stats_int2",
      Set(
        PubCol("date_sid", "Day", InBetweenEquality)
        , PubCol("publisher_id", "Publisher ID", InEquality)
      ),
      Set(
        PublicFactCol("impressions", "Impressions", InBetweenEqualityNullNotNull),
        PublicFactCol("clicks", "Clicks", InEqualityNotEquals),
        PublicFactCol("spend", "Spend", Set.empty)
      ), Set(NotEqualToFilter("Clicks", "777", true, true), IsNotNullFilter("Impressions", true, true)),  getMaxDaysWindow, getMaxDaysLookBack
    )
  }

  /**
    * Fact for MultiEngine multi RowList Dim Join tests
    */
  def pubFactCombined(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    val classStats = {
      ColumnContext.withColumnContext {
        implicit dc: ColumnContext =>
          Fact.newFact(
            name = "f_class_stats", DailyGrain, DruidEngine, Set(AdvertiserSchema),
            Set(
              DimCol("class_id", IntType(), annotations = Set(ForeignKey("combined_class")))
              , DimCol("class_name", IntType(10, (Map(1 -> "Classy", 2 -> "Classier", 3 -> "Classiest"), "Unknown")))
              , DimCol("date", DateType("YYYY-MM-DD"))
            ),
            Set(
              FactCol("num_students", IntType())
            )
          )
      }
    }

    classStats.toPublicFact("class_stats"
    , Set(
        PubCol("class_id", "Class ID", Equality)
        , PubCol("class_name", "Class Name", Equality)
        , PubCol("date", "Day", InBetweenEquality)
      )
    , Set(
        PublicFactCol("num_students", "Students", Equality)
      ), Set.empty, getMaxDaysWindow, getMaxDaysLookBack)
  }

  def pubfact11(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import PostgresExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "fact1", DailyGrain, PostgresEngine, Set(AdvertiserSchema, ResellerSchema),
        Set(
          DimCol("keyword_id", IntType(), annotations = Set(ForeignKey("keyword")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("source_name", IntType(3, (Map(1 -> "Native", 2 -> "Search", -1 -> "UNKNOWN"), "UNKNOWN")), alias = Option("stats_source"))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("device_id", IntType(3, (Map(1 -> "Desktop", 2 -> "Tablet", 3 -> "SmartPhone", -1 -> "UNKNOWN"), "UNKNOWN")))
          , DimCol("network_type", StrType(100, (Map("TEST_PUBLISHER" -> "Test Publisher", "CONTENT_SYNDICATION" -> "Content Syndication", "EXTERNAL" -> "Yahoo Partners" ,  "INTERNAL" -> "Yahoo Properties"), "NONE")))
          , DimCol("start_time", IntType())
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("target_page_url", StrType(), annotations = Set(CaseInsensitive))
          , DimCol("stats_date", TimestampType())
          , DimCol("column_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned")))
          , DimCol("column2_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned_with_singleton")))
          , PostgresDerDimCol("Ad Group Start Date Full", StrType(), TIMESTAMP_TO_FORMATTED_DATE("{start_time}", "YYYY-MM-dd HH:mm:ss"))
          , PostgresDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
          , PostgresDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "W"))
        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          , FactCol("Average CPC", DecType(), PostgresCustomRollup(SUM("{spend}") / SUM("{clicks}")))
          , FactCol("CTR", DecType(), PostgresCustomRollup(SUM("{clicks}" /- "{impressions}")))
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), PostgresCustomRollup(SUM("{avg_pos}" * "{impressions}") /- SUM("{impressions}")))
          , FactCol("Count", IntType(), rollupExpression = CountRollup, alias = Option("count_col"))
          , FactCol("Avg", IntType(), rollupExpression = AverageRollup, alias = Option("avg_col"))
          , FactCol("Max", IntType(), rollupExpression = MaxRollup, alias = Option("max_col"))
          , FactCol("Min", IntType(), rollupExpression = MinRollup, alias = Option("min_col"))
        ),
        annotations = Set(
          PostgresFactStaticHint("PARALLEL_INDEX(cb_campaign_k_stats 4)"),
          PostgresFactDimDrivenHint("PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4)")
        )
      )
    }
      .newRollUp("fact2"
        , "fact1"
        , discarding = Set("ad_id")
        , columnAliasMap = Map("price_type" -> "pricing_type", "source_name" -> "stats_source")
        , overrideAnnotations = Set(
          PostgresFactConditionalHint(FactCondition(Option(true)), "CONDITIONAL_HINT1")
          , PostgresFactConditionalHint(FactCondition(Option(true), Option(false)), "CONDITIONAL_HINT2")
          , PostgresFactConditionalHint(FactCondition(Option(true), Option(false), Option(true)), "CONDITIONAL_HINT3")
          , PostgresFactConditionalHint(FactCondition(Option(true), Option(false), Option(false), Option(false)), "CONDITIONAL_HINT4")
          , PostgresFactConditionalHint(FactCondition(None, minRowsEstimate = Option(900L))
            , "CONDITIONAL_HINT5")
        ), availableOnwardsDate = Some("2010-01-01")
      ).toPublicFact("k_stats_minute",
      Set(
        PubCol("stats_date", "Day", InBetweenEquality),
        PubCol("keyword_id", "Keyword ID", InEquality),
        PubCol("ad_id", "Ad ID", InEquality),
        PubCol("ad_group_id", "Ad Group ID", InEquality),
        PubCol("campaign_id", "Campaign ID", InEquality),
        PubCol("advertiser_id", "Advertiser ID", InEquality),
        PubCol("network_type", "Network Type", InEquality),
        PubCol("stats_source", "Source", EqualityFieldEquality, incompatibleColumns = Set("Source Name")),
        PubCol("source_name", "Source Name", InNotInBetweenEqualityNotEqualsGreaterLesser, incompatibleColumns = Set("Source")),
        PubCol("price_type", "Pricing Type", In),
        PubCol("landing_page_url", "Destination URL", FieldEquality, isImageColumn = true),
        PubCol("target_page_url", "Source URL", FieldEquality),
        PubCol("column_id", "Column ID", Equality),
        PubCol("column2_id", "Column2 ID", Equality),
        PubCol("Ad Group Start Date Full", "Ad Group Start Date Full", InEquality),
        PubCol("Month", "Month", Equality),
        PubCol("Week", "Week", Equality),
        PubCol("device_id", "Device ID", InEquality)
      ),
      Set(
        PublicFactCol("impressions", "Impressions", InNotInBetweenEqualityNotEqualsGreaterLesser),
        PublicFactCol("impressions", "Total Impressions", InBetweenEquality),
        PublicFactCol("clicks", "Clicks", InBetweenEqualityFieldEquality),
        PublicFactCol("spend", "Spend", FieldEquality),
        PublicFactCol("avg_pos", "Average Position", FieldEquality),
        PublicFactCol("max_bid", "Max Bid", Set.empty),
        PublicFactCol("Average CPC", "Average CPC", InBetweenEquality),
        PublicFactCol("CTR", "CTR", InBetweenEquality),
        PublicFactCol("Count", "Count", InBetweenEquality),
        PublicFactCol("Avg", "Avg", InBetweenEquality),
        PublicFactCol("Max", "Max", InBetweenEquality),
        PublicFactCol("Min", "Min", InBetweenEquality)
      ),
      Set(EqualityFilter("Source", "2", true, true)),
      getMaxDaysWindow, getMaxDaysLookBack
    )
  }

}
