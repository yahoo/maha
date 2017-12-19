// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.oracle

import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.query.{BaseQueryGeneratorTest, SharedDimSchema}
import com.yahoo.maha.core.registry.RegistryBuilder
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

/**
 * Created by hiral on 1/15/16.
 */
trait BaseOracleQueryGeneratorTest
  extends FunSuite with Matchers with BeforeAndAfterAll with BaseQueryGeneratorTest with SharedDimSchema {

  override protected def beforeAll(): Unit = {
    OracleQueryGenerator.register(queryGeneratorRegistry,DefaultPartitionColumnRenderer)
  }

  override protected[this] def registerFacts(forcedFilters: Set[ForcedFilter], registryBuilder: RegistryBuilder): Unit = {
    registryBuilder.register(pubfact(forcedFilters))
    registryBuilder.register(pubfact2(forcedFilters))
    registryBuilder.register(pubfact3(forcedFilters))
    registryBuilder.register(pubfact4(forcedFilters))
    registryBuilder.register(pubfact5(forcedFilters))
    registryBuilder.register(pubfact6(forcedFilters))
    registryBuilder.register(pubfact7(forcedFilters))
  }

  def pubfact(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import OracleExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "fact1", DailyGrain, OracleEngine, Set(AdvertiserSchema, ResellerSchema),
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
          , OracleDerDimCol("Ad Group Start Date Full", StrType(), TIMESTAMP_TO_FORMATTED_DATE("{start_time}", "YYYY-MM-dd HH:mm:ss"))
          , OracleDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
          , OracleDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "W"))
        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          , FactCol("Average CPC", DecType(), OracleCustomRollup("{spend}" / "{clicks}"))
          , FactCol("CTR", DecType(), OracleCustomRollup(SUM("{clicks}" /- "{impressions}")))
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), OracleCustomRollup(SUM("{avg_pos}" * "{impressions}") /- SUM("{impressions}")))
        ),
        annotations = Set(
          OracleFactStaticHint("PARALLEL_INDEX(cb_campaign_k_stats 4)"),
          OracleFactDimDrivenHint("PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4)")
        )
      )
    }
      .newRollUp("fact2", "fact1", discarding = Set("ad_id"), columnAliasMap = Map("price_type" -> "pricing_type", "source_name" -> "stats_source"))
      .toPublicFact("k_stats",
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

  def pubfact2(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import OracleExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "ad_fact1", DailyGrain, OracleEngine, Set(AdvertiserSchema, ResellerSchema),
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
          , OracleDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
          , OracleDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "W"))
        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
//          , FactCol("Average CPC", DecType(), OracleCustomRollup("{spend}" / "{clicks}"))
          , FactCol("CTR", DecType(), OracleCustomRollup(SUM("{clicks}" /- "{impressions}")))
          , OracleDerFactCol("Average CPC", DecType(), "{spend}" /- "{clicks}")
          , OracleDerFactCol("Average CPC Cents", DecType(), "{Average CPC}" * "100")
          , OracleDerFactCol("N Spend", DecType(), DECODE("{stats_source}", "1", "{spend}", "0.0"))
          , OracleDerFactCol("N Clicks", DecType(), DECODE("{stats_source}", "1", "{clicks}", "0.0"))
          , OracleDerFactCol("N Average CPC", DecType(), "{N Spend}" /- "{N Clicks}")
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), OracleCustomRollup(SUM("{avg_pos}" * "{impressions}") /- SUM("{impressions}")))
        ),
        annotations = Set(
          OracleFactStaticHint("PARALLEL_INDEX(cb_ad_stats 4)"),
          OracleFactDimDrivenHint("PUSH_PRED PARALLEL_INDEX(cb_ad_stats 4)")
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
          PublicFactCol("Average CPC Cents", "Average CPC Cents", InBetweenEquality),
          PublicFactCol("CTR", "CTR", InBetweenEquality),
          PublicFactCol("N Spend", "N Spend", InBetweenEquality),
          PublicFactCol("N Clicks", "N Clicks", InBetweenEquality),
          PublicFactCol("N Average CPC", "N Average CPC", InBetweenEquality)
        ),
        forcedFilters,
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }
  // New Partitioning Scheme
  def pubfact3(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import OracleExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "k_stats_new_partitioning", DailyGrain, OracleEngine, Set(AdvertiserSchema, ResellerSchema),
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
          , OracleDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
          , OracleDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "W"))
          , OraclePartDimCol("frequency", StrType(), partitionLevel = FirstPartitionLevel)
          , OraclePartDimCol("utc_date", DateType("YYYY-MM-DD"), partitionLevel = SecondPartitionLevel)
        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          , FactCol("Average CPC", DecType(), OracleCustomRollup("{spend}" / "{clicks}"))
          , FactCol("CTR", DecType(), OracleCustomRollup(SUM("{clicks}" /- "{impressions}")))
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), OracleCustomRollup(SUM("{avg_pos}" * "{impressions}") /- SUM("{impressions}")))
        ),
        annotations = Set(
          OracleFactStaticHint("PARALLEL_INDEX(cb_campaign_k_stats 4)"),
          OracleFactDimDrivenHint("PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4)"),
          OraclePartitioningScheme("frequency")
        )
      )
    }
      .newRollUp("k_stats_fact1", "k_stats_new_partitioning", discarding = Set("ad_id"), columnAliasMap = Map("price_type" -> "pricing_type"))
      .toPublicFact("k_stats_new",
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

  // New Partitioning Scheme
  def pubfact4(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import OracleExpression._

    val tableOne  = {
      ColumnContext.withColumnContext {
        implicit dc: ColumnContext =>
          Fact.newFactForView(
            "k_stats_new_partitioning_one", DailyGrain, OracleEngine, Set(AdvertiserSchema, ResellerSchema),
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
              , OracleDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
              , OracleDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "W"))
              , OraclePartDimCol("frequency", StrType(), partitionLevel = FirstPartitionLevel)
              , OraclePartDimCol("utc_date", DateType("YYYY-MM-DD"), partitionLevel = SecondPartitionLevel)
            ),
            Set(
              FactCol("impressions", IntType(3, 1))
              , FactCol("clicks", IntType(3, 0, 1, 800))
              , ConstFactCol("constantFact", IntType(3, 0, 1, 800), "0")
              , FactCol("spend", DecType(0, "0.0"))
              , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
              , FactCol("Average CPC", DecType(), OracleCustomRollup("{spend}" / "{clicks}"))
              , FactCol("CTR", DecType(), OracleCustomRollup(SUM("{clicks}" /- "{impressions}")))
              , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), OracleCustomRollup(SUM("{avg_pos}" * "{impressions}") /- SUM("{impressions}")))
            ),
            annotations = Set(
              OracleFactStaticHint("PARALLEL_INDEX(cb_campaign_k_stats 4)"),
              OracleFactDimDrivenHint("PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4)"),
              OraclePartitioningScheme("frequency")
            )
          )
      }
    }

    val tableTwo  = {
      ColumnContext.withColumnContext {
        implicit dc: ColumnContext =>
          Fact.newFactForView(
            "k_stats_new_partitioning_two", DailyGrain, OracleEngine, Set(AdvertiserSchema, ResellerSchema),
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
              , OracleDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
              , OracleDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "W"))
              , OraclePartDimCol("frequency", StrType(), partitionLevel = FirstPartitionLevel)
              , OraclePartDimCol("utc_date", DateType("YYYY-MM-DD"), partitionLevel = SecondPartitionLevel)
            ),
            Set(
              FactCol("impressions", IntType(3, 1))
              , FactCol("clicks", IntType(3, 0, 1, 800))
              , ConstFactCol("constantFact", IntType(3, 0, 1, 800), "0")
              , FactCol("spend", DecType(0, "0.0"))
              , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
              , FactCol("Average CPC", DecType(), OracleCustomRollup("{spend}" / "{clicks}"))
              , FactCol("CTR", DecType(), OracleCustomRollup(SUM("{clicks}" /- "{impressions}")))
              , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), OracleCustomRollup(SUM("{avg_pos}" * "{impressions}") /- SUM("{impressions}")))
            ),
            annotations = Set(
              OracleFactStaticHint("PARALLEL_INDEX(cb_campaign_k_stats 4)"),
              OracleFactDimDrivenHint("PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4)"),
              OraclePartitioningScheme("frequency")
            )
          )
      }
    }
    val view = UnionView("k_stats_new_partitioning", Seq(tableOne, tableTwo))


    ColumnContext.withColumnContext {
      implicit dc: ColumnContext =>
      Fact.newUnionView(view, DailyGrain, OracleEngine, Set(AdvertiserSchema, ResellerSchema),
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
          , OracleDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
          , OracleDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "W"))
          , OraclePartDimCol("frequency", StrType(), partitionLevel = FirstPartitionLevel)
          , OraclePartDimCol("utc_date", DateType("YYYY-MM-DD"), partitionLevel = SecondPartitionLevel)
        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("constantFact", IntType(3, 0, 1, 800))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          , FactCol("Average CPC", DecType(), OracleCustomRollup("{spend}" / "{clicks}"))
          , FactCol("CTR", DecType(), OracleCustomRollup(SUM("{clicks}" /- "{impressions}")))
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), OracleCustomRollup(SUM("{avg_pos}" * "{impressions}") /- SUM("{impressions}")))
        ),
        annotations = Set(
          OracleFactStaticHint("PARALLEL_INDEX(cb_campaign_k_stats 4)"),
          OracleFactDimDrivenHint("PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4)"),
          OraclePartitioningScheme("frequency")
        )
      )
    }
      .newRollUp("k_stats_fact1", "k_stats_new_partitioning", discarding = Set("ad_id"), columnAliasMap = Map("price_type" -> "pricing_type"))
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
      import com.yahoo.maha.core.OracleExpression._
      ColumnContext.withColumnContext {
        implicit dc: ColumnContext =>
          Fact.newFactForView(
            "campaign_stats", DailyGrain, OracleEngine, Set(AdvertiserSchema, ResellerSchema),
            Set(
              DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
              , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
              , DimCol("stats_date", DateType("YYYY-MM-DD"))
              , OracleDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
              , OracleDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "w"))
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
      import com.yahoo.maha.core.OracleExpression._
      ColumnContext.withColumnContext {
        implicit dc: ColumnContext =>
          Fact.newFactForView(
            "campaign_adjustments", DailyGrain, OracleEngine, Set(AdvertiserSchema, ResellerSchema),
            Set(
              DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
              , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
              , DimCol("stats_date", DateType("YYYY-MM-DD"))
              , OracleDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
              , OracleDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "w"))
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
      import com.yahoo.maha.core.OracleExpression._
      implicit dc: ColumnContext =>
        Fact.newUnionView(campaignAdjView, DailyGrain, OracleEngine, Set(AdvertiserSchema, ResellerSchema),
          Set(
            DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("stats_date", DateType("YYYY-MM-DD"))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
            , OracleDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
            , OracleDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "w"))
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
            "v_publisher_stats", DailyGrain, OracleEngine, Set(PublisherSchema),
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
            "v_publisher_stats", DailyGrain, OracleEngine, Set(PublisherSchema),
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

}
