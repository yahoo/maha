package com.yahoo.maha.core.query.druid

import com.yahoo.maha.core.CoreSchema.{AdvertiserSchema, InternalSchema}
import com.yahoo.maha.core.DruidDerivedFunction._
import com.yahoo.maha.core.DruidPostResultFunction.{POST_RESULT_DECODE, START_OF_THE_MONTH, START_OF_THE_WEEK}
import com.yahoo.maha.core.FilterOperation.{Equality, In, InBetweenEquality, InEquality}
import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension.{DimCol, DruidFuncDimCol, DruidPostResultFuncDimCol, PubCol}
import com.yahoo.maha.core.fact.{PublicFactCol, _}
import com.yahoo.maha.core.query.{BaseQueryGeneratorTest, SharedDimSchema}
import com.yahoo.maha.core.query.oracle.OracleQueryGenerator
import com.yahoo.maha.core.registry.RegistryBuilder
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

/**
  * Created by hiral on 2/23/18.
  */
class BaseDruidQueryGeneratorTest extends FunSuite with Matchers with BeforeAndAfterAll with BaseQueryGeneratorTest with SharedDimSchema {

  override protected def beforeAll(): Unit = {
    OracleQueryGenerator.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer)
    DruidQueryGenerator.register(queryGeneratorRegistry, queryOptimizer = new SyncDruidQueryOptimizer(timeout = 5000))
  }

  override protected[this] def registerFacts(forcedFilters: Set[ForcedFilter], registryBuilder: RegistryBuilder): Unit = {
    registryBuilder.register(pubfact(forcedFilters))
    registryBuilder.register(pubfact_v1(forcedFilters))
    registryBuilder.register(pubfact2(forcedFilters))
    registryBuilder.register(pubfact3(forcedFilters))
    registryBuilder.register(pubfact4(forcedFilters))
    registryBuilder.register(pubfact_start_time(forcedFilters))
    registryBuilder.register(pubfact_minute_grain(forcedFilters))
  }

  private[this] def factBuilder(annotations: Set[FactAnnotation]): FactBuilder = {
    import DruidExpression._
    import ThetaSketchSetOp._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "fact1", HourlyGrain, DruidEngine, Set(AdvertiserSchema, InternalSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(ForeignKey("keyword")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("campaign_id", IntType(), alias = Option("campaign_id_alias"), annotations = Set(ForeignKey("campaign")))
          , DimCol("external_id", IntType(), annotations = Set(ForeignKey("site_externals")))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("source_name", IntType(3), alias = Option("stats_source"))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DruidFuncDimCol("Derived Pricing Type", IntType(3), DECODE_DIM("{price_type}", "7", "6"))
          , DimCol("start_time", DateType("yyyyMMddHH"))
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("Landing URL Translation", StrType(100, (Map("Valid"->"Something"), "Empty")), alias = Option("landing_page_url"))
          , DimCol("stats_date", DateType("yyyyMMdd"), Some("statsDate"))
          , DimCol("engagement_type", StrType(3))
          , DruidPostResultFuncDimCol("Month", DateType(), postResultFunction = START_OF_THE_MONTH("{stats_date}"))
          , DruidPostResultFuncDimCol("Week", DateType(), postResultFunction = START_OF_THE_WEEK("{stats_date}"))
          , DruidFuncDimCol("Day of Week", DateType(), DAY_OF_WEEK("{stats_date}"))
          , DruidFuncDimCol("My Date", DateType(), DRUID_TIME_FORMAT("YYYY-MM-dd"))
          , DimCol("show_sov_flag", IntType())
          , DruidFuncDimCol("Start Date", DateType("YYYYMMdd"), DATETIME_FORMATTER("{start_time}", 0, 8))
          , DruidFuncDimCol("Start Hour", DateType("HH"), DATETIME_FORMATTER("{start_time}", 8, 2))
          , DimCol("ageBucket", StrType())
          , DimCol("woeids", StrType())
          , DimCol("segments", StrType())

        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("sov_impressions", IntType())
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          , FactCol("min_bid", DecType(0, "0.0"), MinRollup)
          , FactCol("avg_bid", DecType(0, "0.0"), AverageRollup)
          , FactCol("avg_pos_times_impressions", DecType(0, "0.0"), MaxRollup)
          , FactCol("engagement_count", IntType(0,0))
          , ConstFactCol("const_a", IntType(0,0), "0")
          , ConstFactCol("const_b", IntType(0,0), "0")
          , DruidConstDerFactCol("Const Der Fact Col C", DecType(), "{const_a}" / "{const_b}", "0")
          , DruidDerFactCol("Average CPC", DecType(), "{spend}" / "{clicks}")
          , DruidDerFactCol("CTR", DecType(), "{clicks}" /- "{impressions}")
          , DruidDerFactCol("derived_avg_pos", DecType(3, "0.0", "0.1", "500"), "{avg_pos_times_impressions}" /- "{impressions}")
          , FactCol("Reblogs", IntType(), DruidFilteredRollup(EqualityFilter("engagement_type", "1"), "engagement_count", SumRollup))
          , FactCol("Click Rate", DecType(), rollupExpression = DruidFilteredListRollup(List(EqualityFilter("engagement_type", "1")), "clicks", SumRollup))
          , FactCol("Click Rate Success Case", DecType(10), rollupExpression = DruidFilteredListRollup(List(
            EqualityFilter("engagement_type", "1"),
            EqualityFilter("campaign_id", "1")), "clicks", SumRollup))
          , DruidDerFactCol("Reblog Rate", DecType(), "{Reblogs}" /- "{impressions}" * "100")
          , DruidPostResultDerivedFactCol("impression_share", StrType(), "{impressions}" /- "{sov_impressions}", postResultFunction = POST_RESULT_DECODE("{show_sov_flag}", "0", "N/A"))
          , FactCol("uniqueUserCount", DecType(0, "0.0"))
          , FactCol("ageBucket_unique_users", DecType(), DruidFilteredRollup(InFilter("ageBucket", List("18-20")), "uniqueUserCount", DruidThetaSketchRollup))
          , FactCol("woeids_unique_users", DecType(), DruidFilteredRollup(InFilter("woeids", List("4563")), "uniqueUserCount", DruidThetaSketchRollup))
          , FactCol("segments_unique_users", DecType(), DruidFilteredRollup(InFilter("segments", List("1234")), "uniqueUserCount", DruidThetaSketchRollup))
          , DruidDerFactCol("Total Unique User Count", DecType(), ThetaSketchEstimator(INTERSECT, List("{ageBucket_unique_users}", "{woeids_unique_users}", "{segments_unique_users}")))
        ),
        annotations = annotations
      )
    }
  }

  private[this] def factBuilder2(annotations: Set[FactAnnotation]): FactBuilder = {
    import DruidExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "fact1", HourlyGrain, DruidEngine, Set(AdvertiserSchema, InternalSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(ForeignKey("keyword")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("campaign_id", IntType(), alias = Option("campaign_id_alias"), annotations = Set(ForeignKey("campaign")))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("external_id", IntType(), annotations = Set(ForeignKey("site_externals")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DruidFuncDimCol("Derived Pricing Type", IntType(3), DECODE_DIM("{price_type}", "7", "6"))
          , DimCol("start_time", DateType("yyyyMMddHH"))
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("stats_date", DateType("yyyyMMdd"), Some("statsDate"))
          , DimCol("engagement_type", StrType(3))
          , DruidFuncDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "w"))
          , DruidFuncDimCol("Day of Week", DateType(), DAY_OF_WEEK("{stats_date}"))
          , DruidFuncDimCol("My Date", DateType(), DRUID_TIME_FORMAT("YYYY-MM-dd"))
          , DimCol("show_sov_flag", IntType())
          , DruidFuncDimCol("Day", DateType("YYYYMMdd"), DATETIME_FORMATTER("{start_time}", 0, 8))
          , DruidFuncDimCol("Hour", DateType("HH"), DATETIME_FORMATTER("{start_time}", 8, 2))
        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("sov_impressions", IntType())
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          , FactCol("min_bid", DecType(0, "0.0"), MinRollup)
          , FactCol("avg_bid", DecType(0, "0.0"), AverageRollup)
          , FactCol("avg_pos_times_impressions", DecType(0, "0.0"), MaxRollup)
          , FactCol("engagement_count", IntType(0,0))
          , DruidDerFactCol("Average CPC", DecType(), "{spend}" / "{clicks}")
          , DruidDerFactCol("CTR", DecType(), "{clicks}" /- "{impressions}")
          , DruidDerFactCol("derived_avg_pos", DecType(3, "0.0", "0.1", "500"), "{avg_pos_times_impressions}" /- "{impressions}")
          , FactCol("Reblogs", IntType(), DruidFilteredRollup(EqualityFilter("engagement_type", "1"), "engagement_count", SumRollup))
          , DruidDerFactCol("Reblog Rate", DecType(), "{Reblogs}" /- "{impressions}" * "100")
          , DruidPostResultDerivedFactCol("impression_share", StrType(), "{impressions}" /- "{sov_impressions}", postResultFunction = POST_RESULT_DECODE("{show_sov_flag}", "0", "N/A"))
        ),
        annotations = annotations
      )
    }
  }

  private[this] def factBuilder3(annotations: Set[FactAnnotation]): FactBuilder = {
    import DruidExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "fact1", MinuteGrain, DruidEngine, Set(AdvertiserSchema, InternalSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(ForeignKey("keyword")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("campaign_id", IntType(), alias = Option("campaign_id_alias"), annotations = Set(ForeignKey("campaign")))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("external_id", IntType(), annotations = Set(ForeignKey("site_externals")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DruidFuncDimCol("Derived Pricing Type", IntType(3), DECODE_DIM("{price_type}", "7", "6"))
          , DimCol("start_time", DateType("yyyyMMddHH"))
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("stats_date", DateType("yyyyMMdd"), Some("statsDate"))
          , DimCol("engagement_type", StrType(3))
          , DruidFuncDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "w"))
          , DruidFuncDimCol("Day of Week", DateType(), DAY_OF_WEEK("{stats_date}"))
          , DruidFuncDimCol("My Date", DateType(), DRUID_TIME_FORMAT("YYYY-MM-dd"))
          , DimCol("show_sov_flag", IntType())
          , DruidFuncDimCol("Day", DateType("YYYYMMdd"), DATETIME_FORMATTER("{start_time}", 0, 8))
          , DruidFuncDimCol("Hour", DateType("HH"), DATETIME_FORMATTER("{start_time}", 8, 2))
          //, DruidFuncDimCol("Minute", DateType("mm"), DATETIME_FORMATTER("{start_time}", 8, 2))
          , DruidFuncDimCol("Minute", DateType("mm"), DRUID_TIME_FORMAT("mm"))
        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("sov_impressions", IntType())
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          , FactCol("min_bid", DecType(0, "0.0"), MinRollup)
          , FactCol("avg_bid", DecType(0, "0.0"), AverageRollup)
          , FactCol("avg_pos_times_impressions", DecType(0, "0.0"), MaxRollup)
          , FactCol("engagement_count", IntType(0,0))
          , DruidDerFactCol("Average CPC", DecType(), "{spend}" / "{clicks}")
          , DruidDerFactCol("CTR", DecType(), "{clicks}" /- "{impressions}")
          , DruidDerFactCol("derived_avg_pos", DecType(3, "0.0", "0.1", "500"), "{avg_pos_times_impressions}" /- "{impressions}")
          , FactCol("Reblogs", IntType(), DruidFilteredRollup(EqualityFilter("engagement_type", "1"), "engagement_count", SumRollup))
          , DruidDerFactCol("Reblog Rate", DecType(), "{Reblogs}" /- "{impressions}" * "100")
          , DruidPostResultDerivedFactCol("impression_share", StrType(), "{impressions}" /- "{sov_impressions}", postResultFunction = POST_RESULT_DECODE("{show_sov_flag}", "0", "N/A"))
        ),
        annotations = annotations
      )
    }
  }


  private[this] def pubfact(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    factBuilder(Set.empty)
      .toPublicFact("k_stats",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("engagement_type", "engagement_type", Equality),
          PubCol("id", "Keyword ID", InEquality),
          PubCol("ad_id", "Ad ID", InEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("stats_source", "Source", Equality, incompatibleColumns = Set("Source Name")),
          PubCol("source_name", "Source Name", InEquality, incompatibleColumns = Set("Source")),
          PubCol("price_type", "Pricing Type", In),
          PubCol("Derived Pricing Type", "Derived Pricing Type", InEquality),
          PubCol("landing_page_url", "Destination URL", Set.empty),
          PubCol("Landing URL Translation", "Landing URL Translation", Set.empty),
          PubCol("Week", "Week", InBetweenEquality),
          PubCol("Month", "Month", InBetweenEquality),
          PubCol("My Date", "My Date", Equality),
          PubCol("Day of Week", "Day of Week", Equality),
          PubCol("Start Date", "Start Date", InEquality),
          PubCol("Start Hour", "Start Hour", InEquality),
          PubCol("ageBucket", "Age Bucket", InEquality),
          PubCol("woeids", "Woe ID", InEquality),
          PubCol("segments", "Segments", InEquality)
          //PubCol("Ad Group Start Date Full", "Ad Group Start Date Full", InEquality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("const_a", "const_a", InBetweenEquality),
          PublicFactCol("const_b", "const_b", InBetweenEquality),
          PublicFactCol("Const Der Fact Col C", "Const Der Fact Col C", InBetweenEquality),
          PublicFactCol("spend", "Spend", Set.empty),
          PublicFactCol("derived_avg_pos", "Average Position", Set.empty),
          PublicFactCol("max_bid", "Max Bid", Set.empty),
          PublicFactCol("min_bid", "Min Bid", Set.empty),
          PublicFactCol("avg_bid", "Average Bid", Set.empty),
          PublicFactCol("Average CPC", "Average CPC", InBetweenEquality),
          PublicFactCol("Reblogs", "Reblogs", InBetweenEquality),
          PublicFactCol("Reblog Rate", "Reblog Rate", InBetweenEquality),
          PublicFactCol("Click Rate", "Click Rate", InBetweenEquality),
          PublicFactCol("Click Rate Success Case", "Click Rate Success Case", InBetweenEquality),
          PublicFactCol("CTR", "CTR", InBetweenEquality),
          PublicFactCol("uniqueUserCount", "Unique User Count", InBetweenEquality),
          PublicFactCol("ageBucket_unique_users", "ageBucket_unique_users", InBetweenEquality),
          PublicFactCol("woeids_unique_users", "woeids_unique_users", InBetweenEquality),
          PublicFactCol("segments_unique_users", "segments_unique_users", InBetweenEquality),
          PublicFactCol("Total Unique User Count", "Total Unique User Count", InBetweenEquality)
        ),
        //Set(EqualityFilter("Source", "2")),
        Set(),
        getMaxDaysWindow, getMaxDaysLookBack, renderLocalTimeFilter = true
      )
  }

  private[this] def pubfact_v1(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    factBuilder(Set(DruidGroupByStrategyV2))
      .toPublicFact("k_stats",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("engagement_type", "engagement_type", Equality),
          PubCol("id", "Keyword ID", InEquality),
          PubCol("ad_id", "Ad ID", InEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("external_id", "External Site ID", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("Derived Pricing Type", "Derived Pricing Type", InEquality),
          PubCol("landing_page_url", "Destination URL", Set.empty),
          PubCol("Week", "Week", InBetweenEquality)
          //PubCol("Ad Group Start Date Full", "Ad Group Start Date Full", InEquality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("impression_share", "Impression Share", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("spend", "Spend", Set.empty),
          PublicFactCol("derived_avg_pos", "Average Position", Set.empty),
          PublicFactCol("max_bid", "Max Bid", Set.empty),
          PublicFactCol("min_bid", "Min Bid", Set.empty),
          PublicFactCol("avg_bid", "Average Bid", Set.empty),
          PublicFactCol("Average CPC", "Average CPC", InBetweenEquality),
          PublicFactCol("Reblogs", "Reblogs", InBetweenEquality),
          PublicFactCol("Reblog Rate", "Reblog Rate", InBetweenEquality),
          PublicFactCol("Click Rate", "Click Rate", InBetweenEquality),
          PublicFactCol("Click Rate Success Case", "Click Rate Success Case", InBetweenEquality),
          PublicFactCol("CTR", "CTR", InBetweenEquality)
        ),
        //Set(EqualityFilter("Source", "2")),
        Set(),
        getMaxDaysWindow, getMaxDaysLookBack, renderLocalTimeFilter = true, revision = 1, dimRevision = 2
      )
  }

  private[this] def pubfact2(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    factBuilder(Set.empty)
      .toPublicFact("k_stats_no_local_time",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("engagement_type", "engagement_type", Equality),
          PubCol("id", "Keyword ID", InEquality),
          PubCol("ad_id", "Ad ID", InEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("Derived Pricing Type", "Derived Pricing Type", InEquality),
          PubCol("landing_page_url", "Destination URL", Set.empty),
          PubCol("Week", "Week", InBetweenEquality)
          //PubCol("Ad Group Start Date Full", "Ad Group Start Date Full", InEquality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("spend", "Spend", Set.empty),
          PublicFactCol("derived_avg_pos", "Average Position", Set.empty),
          PublicFactCol("max_bid", "Max Bid", Set.empty),
          PublicFactCol("min_bid", "Min Bid", Set.empty),
          PublicFactCol("avg_bid", "Average Bid", Set.empty),
          PublicFactCol("Average CPC", "Average CPC", InBetweenEquality),
          PublicFactCol("Reblogs", "Reblogs", InBetweenEquality),
          PublicFactCol("Reblog Rate", "Reblog Rate", InBetweenEquality),
          PublicFactCol("Click Rate", "Click Rate", InBetweenEquality),
          PublicFactCol("Click Rate Success Case", "Click Rate Success Case", InBetweenEquality),
          PublicFactCol("CTR", "CTR", InBetweenEquality)
        ),
        //Set(EqualityFilter("Source", "2")),
        Set(),
        getMaxDaysWindow, getMaxDaysLookBack, renderLocalTimeFilter = false
      )
  }

  private[this] def pubfact3(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    factBuilder(Set(DruidGroupByStrategyV2))
      .toPublicFact("user_stats",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("ad_id", "Ad ID", InEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("Reblogs", "Reblogs", InBetweenEquality),
          PublicFactCol("Reblog Rate", "Reblog Rate", InBetweenEquality),
          PublicFactCol("Click Rate", "Click Rate", InBetweenEquality),
          PublicFactCol("Click Rate Success Case", "Click Rate Success Case", InBetweenEquality),
          PublicFactCol("CTR", "CTR", InBetweenEquality)
        ),
        Set(),
        getMaxDaysWindow, getMaxDaysLookBack, renderLocalTimeFilter = true
      )
  }

  private[this] def pubfact4(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    factBuilder(Set(DruidGroupByStrategyV2))
      .toPublicFact("user_stats_v2",
        Set(
          PubCol("My Date", "Day", InBetweenEquality),
          PubCol("ad_id", "Ad ID", InEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("Reblogs", "Reblogs", InBetweenEquality),
          PublicFactCol("Reblog Rate", "Reblog Rate", InBetweenEquality),
          PublicFactCol("Click Rate", "Click Rate", InBetweenEquality),
          PublicFactCol("Click Rate Success Case", "Click Rate Success Case", InBetweenEquality),
          PublicFactCol("CTR", "CTR", InBetweenEquality)
        ),
        Set(),
        getMaxDaysWindow, getMaxDaysLookBack, renderLocalTimeFilter = true
      )
  }

  private[this] def pubfact_start_time(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    factBuilder2(Set(DruidGroupByStrategyV1,DruidGroupByIsSingleThreaded(false)))
      .toPublicFact("k_stats_start_time",
        Set(
          PubCol("Day", "Day", InBetweenEquality),
          PubCol("Hour", "Hour", InBetweenEquality),
          PubCol("engagement_type", "engagement_type", Equality),
          PubCol("id", "Keyword ID", InEquality),
          PubCol("ad_id", "Ad ID", InEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("Derived Pricing Type", "Derived Pricing Type", InEquality),
          PubCol("landing_page_url", "Destination URL", Set.empty),
          PubCol("Week", "Week", InBetweenEquality),
          PubCol("My Date", "My Date", Equality),
          PubCol("Day of Week", "Day of Week", Equality)
          //PubCol("Ad Group Start Date Full", "Ad Group Start Date Full", InEquality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("spend", "Spend", Set.empty),
          PublicFactCol("derived_avg_pos", "Average Position", Set.empty),
          PublicFactCol("max_bid", "Max Bid", Set.empty),
          PublicFactCol("min_bid", "Min Bid", Set.empty),
          PublicFactCol("avg_bid", "Average Bid", Set.empty),
          PublicFactCol("Average CPC", "Average CPC", InBetweenEquality),
          PublicFactCol("Reblogs", "Reblogs", InBetweenEquality),
          PublicFactCol("Reblog Rate", "Reblog Rate", InBetweenEquality),
          PublicFactCol("CTR", "CTR", InBetweenEquality)
        ),
        //Set(EqualityFilter("Source", "2")),
        Set(),
        getMaxDaysWindow, getMaxDaysLookBack, renderLocalTimeFilter = true
      )
  }

  private[this] def pubfact_minute_grain(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    factBuilder3(Set(DruidGroupByStrategyV1, DruidQueryPriority(-1)))
      .toPublicFact("k_stats_minute_grain",
        Set(
          PubCol("Day", "Day", InBetweenEquality),
          PubCol("Hour", "Hour", InBetweenEquality),
          PubCol("engagement_type", "engagement_type", Equality),
          PubCol("id", "Keyword ID", InEquality),
          PubCol("ad_id", "Ad ID", InEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("Derived Pricing Type", "Derived Pricing Type", InEquality),
          PubCol("landing_page_url", "Destination URL", Set.empty),
          PubCol("Week", "Week", InBetweenEquality),
          PubCol("My Date", "My Date", Equality),
          PubCol("Day of Week", "Day of Week", Equality),
          PubCol("Minute", "Minute", InBetweenEquality)
          //PubCol("Ad Group Start Date Full", "Ad Group Start Date Full", InEquality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("spend", "Spend", Set.empty),
          PublicFactCol("derived_avg_pos", "Average Position", Set.empty),
          PublicFactCol("max_bid", "Max Bid", Set.empty),
          PublicFactCol("min_bid", "Min Bid", Set.empty),
          PublicFactCol("avg_bid", "Average Bid", Set.empty),
          PublicFactCol("Average CPC", "Average CPC", InBetweenEquality),
          PublicFactCol("Reblogs", "Reblogs", InBetweenEquality),
          PublicFactCol("Reblog Rate", "Reblog Rate", InBetweenEquality),
          PublicFactCol("CTR", "CTR", InBetweenEquality)
        ),
        //Set(EqualityFilter("Source", "2")),
        Set(),
        getMaxDaysWindow, getMaxDaysLookBack, renderLocalTimeFilter = true
      )
  }

  protected[this] def getDruidQueryGenerator : DruidQueryGenerator = {
    new DruidQueryGenerator(new SyncDruidQueryOptimizer(timeout = 5000), 40000)
  }
}
