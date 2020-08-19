// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.executor.druid

import java.net.InetSocketAddress

import cats.effect.IO
import com.yahoo.maha.core.CoreSchema.{AdvertiserLowLatencySchema, AdvertiserSchema, InternalSchema, ResellerSchema}
import com.yahoo.maha.core.DruidDerivedFunction.{DECODE_DIM, DRUID_TIME_FORMAT, GET_INTERVAL_DATE}
import com.yahoo.maha.core.DruidPostResultFunction.{POST_RESULT_DECODE, START_OF_THE_MONTH, START_OF_THE_WEEK}
import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact.{DruidConstDerFactCol, PublicFactCol, _}
import com.yahoo.maha.core.lookup.LongRangeLookup
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.query.druid.{DruidQuery, DruidQueryGenerator, SyncDruidQueryOptimizer}
import com.yahoo.maha.core.query.oracle.OracleQueryGenerator
import com.yahoo.maha.core.registry.RegistryBuilder
import com.yahoo.maha.core.request.{DebugValue, Parameter, ReportingRequest, RowCountQuery, SyncRequest}
import com.yahoo.maha.executor.MockOracleQueryExecutor
import org.apache.druid.common.config.NullHandling
import org.apache.druid.query.Result
import org.apache.druid.query.scan.ScanResultValue
import org.http4s.server.blaze.BlazeBuilder
import org.json4s.JsonAST._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import org.mockito.Mockito._
import org.mockito.Matchers._

/**
  * Created by hiral on 2/2/16.
  */

class TestAuthHeaderProvider extends AuthHeaderProvider {
  override def getAuthHeaders = Map("c" -> "d")
}

class DruidQueryExecutorTest extends FunSuite with Matchers with BeforeAndAfterAll with BaseQueryGeneratorTest with SharedDimSchema with TestWebService {

  var server: org.http4s.server.Server[IO] = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    val builder = BlazeBuilder[IO].mountService(service, "/mock").bindSocketAddress(new InetSocketAddress("localhost", 6667))
    server = builder.start.unsafeRunSync()
    info("Started blaze server")
  }

  override def afterAll {
    info("Stopping blaze server")
    server.shutdown
  }

  override protected[this] def registerFacts(forcedFilters: Set[ForcedFilter], registryBuilder: RegistryBuilder): Unit = {
    registryBuilder.register(pubfact(forcedFilters))
    registryBuilder.register(pubfact2())
    registryBuilder.register(pubfact3())
    registryBuilder.register(pubfact4())
  }

  val siteNameMap = Map(
    1 -> "SITE_1"
    , 2 -> "SITE_2"
    , 3 -> "SITE_3"
    , 4 -> "SITE_4"
    , 5 -> "SITE_5"
    , 6 -> "SITE_6"
    , 7 -> "SITE_7"
    , 8 -> "SITE_8"
    , 9 -> "SITE_9"
    , 10 -> "SITE_10"
    , 11 -> "SITE_11"
    , 12 -> "SITE_12"
    , 13 -> "SITE_13"
    , 14 -> "SITE_14"
    , 15 -> "SITE_15"
  )

  private[this] def pubfact3(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import DruidExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "dr_wg_stats", HourlyGrain, DruidEngine, Set(InternalSchema),
        Set(
          DimCol("Day", DateType("YYYYMMdd"), alias = Option("stats_date"))
          , DimCol("Hour", DateType("HH"), alias = Option("stats_hour"))
          , DimCol("utc_time", StrType())
          , DimCol("account_id", IntType(10), annotations = Set(ForeignKey("advertiser")))
          , DimCol("campaign_id", IntType(10), annotations = Set(ForeignKey("campaign")))
          , DimCol("section_id", IntType(10), annotations = Set(ForeignKey("sections")))
          , DimCol("supply_group_id", StrType(40))
          , DimCol("age_group", IntType(10))
          , DimCol("rule_id", IntType(10))
          , DimCol("device", IntType(8, (Map(1 -> "SmartPhone", 2 -> "Tablet", 3 -> "Desktop", -1 -> "UNKNOWN"), "UNKNOWN")))
          , DimCol("bidding_strategy", StrType(32), annotations = Set(EscapingRequired))
          , DimCol("install_type", IntType(10))
          , DimCol("campaign_objective", StrType(40))
          , DimCol("publisher_id", IntType(10), annotations = Set(ForeignKey("publishers")))
          , DimCol("internal_bucket_id", StrType())
          , DimCol("winss", StrType())
          , DimCol("os", StrType())
          , DimCol("gender", StrType())
          , DimCol("traffic_type", StrType())
        ),
        Set(
          FactCol("impressions", IntType(10, 0))
          , FactCol("clicks", IntType(10, 0))
          , FactCol("post_install_inapp_conv", IntType(10, 0))
          , FactCol("post_click_inapp_conv", IntType(10, 0))
          , FactCol("post_imp_conv", IntType(10, 0))
          , FactCol("post_click_conv", IntType(10, 0))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("spend_usd", DecType(0, "0.0"))
          , FactCol("click_probability", DecType(0, "0.0"))
          , FactCol("conversions_probability", DecType(0, "0.0"))
          , FactCol("ecpa_goal_usd", DecType(0, "0.0"))
          , FactCol("rank_score", DecType(0, "0.0"))
          , FactCol("alpha", DecType(0, "0.0"))
          , FactCol("mappi_boost", DecType(0, "0.0"))
          , FactCol("marketplace_floor", DecType(0, "0.0"))
          , DruidDerFactCol("Total in App Conv", DecType(), "{post_click_inapp_conv}" ++ "{post_install_inapp_conv}")
          , DruidDerFactCol("CTR", DecType(), "{clicks}" /- "{impressions}" * "100")
          , DruidDerFactCol("Average CPM", DecType(), "{spend}" /- "{impressions}" * "1000")
          , DruidDerFactCol("Average CPC", DecType(), "{spend}" /- "{clicks}")
          , DruidDerFactCol("Average CVR", DecType(), "{post_click_conv}" /- "{clicks}" * "100")
          , DruidDerFactCol("Average PICVR", DecType(), "{post_install_inapp_conv}" /- "{clicks}" * "100")
          , DruidDerFactCol("Average CPA", DecType(), "{spend}" /- "{post_click_conv}")
          , DruidDerFactCol("Average PICPA", DecType(), "{spend}" /- "{post_install_inapp_conv}")
          , DruidDerFactCol("Average PCPI_CPA", DecType(), "{spend}" /- "{Total in App Conv}")
          , DruidDerFactCol("Average ECPA Goal Usd", DecType(), "{ecpa_goal_usd}" /- "{impressions}")
          , DruidDerFactCol("Average Rank Score", DecType(), "{rank_score}" /- "{impressions}")
          , DruidDerFactCol("Average Alpha", DecType(), "{alpha}" /- "{impressions}")
          , DruidDerFactCol("Average Mappi Boost", DecType(), "{mappi_boost}" /- "{impressions}")
          , DruidDerFactCol("Average Market Place Floor", DecType(), "{marketplace_floor}" /- "{impressions}")
          , DruidDerFactCol("Effective Bid", DecType(), "{Average Rank Score}" /- "{click_probability}")
          , DruidDerFactCol("Efficiency", DecType(), "{Average CPA}" /- "{Average ECPA Goal Usd}")
          , DruidDerFactCol("Pi Efficiency", DecType(), "{Average PICPA}" /- "{Average ECPA Goal Usd}")
        )
        , costMultiplierMap = Map(SyncRequest -> CostMultiplier(LongRangeLookup.full(1.0)))
        , annotations = Set(DruidQueryPriority(-1), DruidGroupByStrategyV2)
      )
    }
      .toPublicFact("wg_stats",
        Set(
          PubCol("account_id", "Advertiser ID", InNotInEquality),
          PubCol("campaign_id", "Campaign ID", InNotInEquality),
          PubCol("section_id", "Section ID", InEqualityNotEquals),
          PubCol("supply_group_id", "Supply Group", InEquality),
          PubCol("age_group", "Age Group", InEquality),
          PubCol("rule_id", "Rule ID", InEqualityNotEquals),
          PubCol("device", "Device", InEqualityNotEquals),
          PubCol("bidding_strategy", "Bidding Strategy", InEqualityNotEquals),
          PubCol("install_type", "Install Type", InEqualityNotEquals),
          PubCol("Day", "Day", BetweenEquality), // 'filteringRequired': 'true' ==> forced filters???
          PubCol("Hour", "Hour", BetweenEquality),
          PubCol("publisher_id", "Publisher ID", InEqualityNotEquals),
          PubCol("internal_bucket_id", "Internal Bucket ID", InEqualityNotEquals),
          PubCol("campaign_objective", "Campaign Objective", InNotInEquality),
          PubCol("winss", "Winss", InNotInEquality),
          PubCol("os", "Os", InNotInEquality),
          PubCol("gender", "Gender", InNotInEquality),
          PubCol("traffic_type", "Traffic Type", InNotInEquality)
        )
        , Set(
          PublicFactCol("impressions", "Impressions", Set.empty),
          PublicFactCol("clicks", "Clicks", Set.empty),
          PublicFactCol("post_install_inapp_conv", "Post Install App Conversions", Set.empty),
          PublicFactCol("post_click_inapp_conv", "Post Click App Conversions", Set.empty),
          PublicFactCol("post_imp_conv", "Post Impression Conversions", Set.empty),
          PublicFactCol("post_click_conv", "Post Click Conversions", Set.empty),
          PublicFactCol("spend", "Spend", Set.empty),
          PublicFactCol("spend_usd", "Spend Usd", Set.empty),
          PublicFactCol("rank_score", "Rank score", Set.empty),
          PublicFactCol("click_probability", "PClicks", Set.empty),
          PublicFactCol("conversions_probability", "PConversions", Set.empty),
          PublicFactCol("CTR", "CTR", Set.empty),
          PublicFactCol("Average CPM", "Average CPM", Set.empty),
          PublicFactCol("Average CPC", "Average CPC", Set.empty),
          PublicFactCol("Average CVR", "Average CVR", Set.empty),
          PublicFactCol("Average PICVR", "Average PICVR", Set.empty),
          PublicFactCol("Average CPA", "Average CPA", Set.empty),
          PublicFactCol("Average PICPA", "Average PICPA", Set.empty),
          PublicFactCol("Average PCPI_CPA", "Average PCPI_CPA", Set.empty),
          PublicFactCol("Average ECPA Goal Usd", "Average ECPA Goal Usd", Set.empty),
          PublicFactCol("Effective Bid", "Effective Bid", Set.empty),
          PublicFactCol("Efficiency", "Efficiency", Set.empty),
          PublicFactCol("Total in App Conv", "Total in App Conv", Set.empty),
          PublicFactCol("Pi Efficiency", "Pi Efficiency", Set.empty)

        )
        , Set()
        , Map((SyncRequest, HourlyGrain) -> 14, (SyncRequest, DailyGrain) -> 31)
        , Map((SyncRequest, HourlyGrain) -> 14, (SyncRequest, DailyGrain) -> 31)
        , dimRevision = 1
      )
  }

  private[this] def pubfact(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import DruidExpression._
    val factBuilder = ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "fact_s_term", DailyGrain, DruidEngine, Set(AdvertiserSchema, AdvertiserLowLatencySchema),
        Set(
          DimCol("id", IntType(), annotations = Set(ForeignKey("keyword")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("country_woeid", IntType(), annotations = Set(ForeignKey("woeid")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("start_time", IntType())
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("stats_date", DateType("YYYYMMdd"))
          , DimCol("show_sov_flag", IntType())
          , DimCol("device_id", IntType())
          , DimCol("site_id", IntType(10, (siteNameMap, "Others")))
          , DruidFuncDimCol("device_type", StrType(), DECODE_DIM("{device_id}", "1", "'Desktop'", "'SmartPhone'"))
          , DruidPostResultFuncDimCol("Week", DateType(), postResultFunction = START_OF_THE_WEEK("{stats_date}"))
          , DruidPostResultFuncDimCol("Month", DateType(), postResultFunction = START_OF_THE_MONTH("{stats_date}"))
        ),
        Set(
          FactCol("impresssions", IntType(3, 1))
          , FactCol("sov_impresssions", IntType())
          , FactCol("conversions", IntType())
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          , FactCol("min_bid", DecType(0, "0.0"), MinRollup)
          , FactCol("avg_pos_times_impressions", DecType(0, "0.0"), MaxRollup)
          , FactCol("weighted_bid_modifier", DecType(8, 2, "0.0"))
          , FactCol("weighted_bid_usd", DecType(0, 2, "0.0"))
          , FactCol("bid_mod_impressions", IntType(10, 0))
          , DruidDerFactCol("Average Bid", DecType(8, "0.0"), "{weighted_bid_usd}" /- "{impresssions}")
          , DruidDerFactCol("Average CPC", DecType(), "{spend}" / "{clicks}")
          , DruidDerFactCol("CTR", DecType(), "{clicks}" /- "{impresssions}")
          , DruidDerFactCol("Bid Mod", DecType(8, 5, "0.0"), "{weighted_bid_modifier}" /- "{bid_mod_impressions}")
          , DruidDerFactCol("derived_avg_pos", DecType(8, 2, "0.0", "0.1", "500"), "{avg_pos_times_impressions}" /- "{impresssions}")
          , DruidDerFactCol("Bid Modifier", DecType(0, 5, "0.0"), "{weighted_bid_modifier}" /- "{bid_mod_impressions}")
          , DruidPostResultDerivedFactCol("Modified Bid", DecType(8, 5, "0.0"), "{Bid Modifier}" * "{Average Bid}", postResultFunction = POST_RESULT_DECODE("{Bid Mod}", "0", "{Average Bid}"))
          , DruidPostResultDerivedFactCol("Impression Share", StrType(), "{impresssions}" /- "{sov_impresssions}", postResultFunction = POST_RESULT_DECODE("{show_sov_flag}", "0", "N/A"))
        ),
        annotations = Set()
      )
    }
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      factBuilder.withAlternativeEngine("fd_fact_s_term", "fact_s_term", OracleEngine, Set.empty, Set.empty,
        schemas = Some(Set(AdvertiserSchema)))
    }

    factBuilder.toPublicFact("k_stats",
      Set(
        PubCol("stats_date", "Day", InBetweenEquality),
        PubCol("id", "Keyword ID", InEquality),
        PubCol("ad_id", "Ad ID", InEquality),
        PubCol("ad_group_id", "Ad Group ID", InEquality),
        PubCol("campaign_id", "Campaign ID", InEquality),
        PubCol("advertiser_id", "Advertiser ID", InEquality),
        PubCol("country_woeid", "Country WOEID", InEquality),
        PubCol("stats_source", "Source", Equality),
        PubCol("price_type", "Pricing Type", In),
        PubCol("landing_page_url", "Destination URL", Set.empty),
        PubCol("Week", "Week", InBetweenEquality),
        PubCol("device_type", "Device Type", InNotInEquality),
        PubCol("site_id", "External Site Name", InBetweenEquality),
        PubCol("Month", "Month", InBetweenEquality)
      ),
      Set(
        PublicFactCol("impresssions", "Impressions", InBetweenEquality),
        PublicFactCol("Impression Share", "Impression Share", InBetweenEquality),
        PublicFactCol("conversions", "Conversions", InBetweenEquality),
        PublicFactCol("clicks", "Clicks", InBetweenEquality),
        PublicFactCol("spend", "Spend", Set.empty),
        PublicFactCol("derived_avg_pos", "Average Position", Set.empty),
        PublicFactCol("max_bid", "Max Bid", Set.empty),
        PublicFactCol("min_bid", "Min Bid", Set.empty),
        PublicFactCol("Average Bid", "Average Bid", Set.empty),
        PublicFactCol("Average CPC", "Average CPC", InBetweenEquality),
        PublicFactCol("Bid Modifier", "Bid Modifier", InBetweenEquality),
        PublicFactCol("Modified Bid", "Modified Bid", InBetweenEquality),
        PublicFactCol("CTR", "CTR", InBetweenEquality)
      ),
      Set(),
      getMaxDaysWindow, getMaxDaysLookBack, renderLocalTimeFilter = false
    )
  }

  def pubfact2(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {

    val tableOne = {
      ColumnContext.withColumnContext {
        import DruidExpression._
        implicit dc: ColumnContext =>
          Fact.newFactForView(
            "account_stats", DailyGrain, DruidEngine, Set(AdvertiserSchema, ResellerSchema),
            Set(
              DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
              , ConstDimCol("is_adjustment", StrType(1), "N")
              , DimCol("stats_date", DateType("YYYY-MM-DD"))
              , DimCol("test_flag", IntType())
              , DimCol("Test Flag", IntType(), alias = Option("{test_flag}"))
              , DruidFuncDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
              , DruidFuncDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "w"))
            ),
            Set(
              FactCol("impressions", IntType(3, 1))
              , FactCol("clicks", IntType(3, 0, 1, 800))
              , FactCol("spend", DecType(0, "0.0"))
              , DruidConstDerFactCol("Der Fact Col A", DecType(), "{clicks}" / "{impressions}", "1")
            )
          )
      }
    }

    val tableTwo = {
      ColumnContext.withColumnContext {
        import DruidExpression._
        implicit dc: ColumnContext =>
          Fact.newFactForView(
            "a_adjustments", DailyGrain, DruidEngine, Set(AdvertiserSchema, ResellerSchema),
            Set(
              DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
              , ConstDimCol("is_adjustment", StrType(1), "Y")
              , DimCol("stats_date", DateType("YYYY-MM-DD"))
              , DimCol("test_flag", IntType())
              , DimCol("Test Flag", IntType(), alias = Option("{test_flag}"))
              , DruidFuncDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
              , DruidFuncDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "w"))
            ),
            Set(
              FactCol("impressions", IntType(3, 1))
              , FactCol("clicks", IntType(3, 0, 1, 800))
              , FactCol("spend", DecType(0, "0.0"))
              , DruidConstDerFactCol("Der Fact Col A", DecType(), "{clicks}" / "{impressions}", "0")
            )
          )
      }
    }
    val view = UnionView("account_a_stats", Seq(tableOne, tableTwo))


    ColumnContext.withColumnContext {
      import DruidExpression._
      implicit dc: ColumnContext =>
        Fact.newUnionView(view, DailyGrain, DruidEngine, Set(AdvertiserSchema, ResellerSchema),
          Set(
            DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("stats_date", DateType("YYYY-MM-DD"))
            , DimCol("test_flag", IntType())
            , DimCol("Test Flag", IntType(), alias = Option("{test_flag}"))
            , DimCol("is_adjustment", StrType(1))
            , DruidFuncDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
            , DruidFuncDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "w"))
          ),
          Set(
            FactCol("impressions", IntType(3, 1))
            , FactCol("clicks", IntType(3, 0, 1, 800))
            , FactCol("spend", DecType(0, "0.0"))
            , DruidDerFactCol("Der Fact Col A", DecType(), "{clicks}" / "{impressions}")
          )
        )
    }
      .toPublicFact("a_stats",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("is_adjustment", "Is Adjustment", Equality),
          PubCol("Test Flag", "Test Flag", Equality),
          PubCol("Month", "Month", Equality),
          PubCol("Week", "Week", Equality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("spend", "Spend", Set.empty),
          PublicFactCol("Der Fact Col A", "Der Fact Col A", InBetweenEquality)
        ), Set(EqualityFilter("Test Flag", "0", isForceFilter = true)), getMaxDaysWindow, getMaxDaysLookBack
      )
  }

  private[this] def pubfact4(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "fact4", HourlyGrain, DruidEngine, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(ForeignKey("keyword")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", 8 -> "CPV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DruidFuncDimCol("Derived Pricing Type", IntType(3), DECODE_DIM("{price_type}", "7", "6", "2", "1", "{price_type}"))
          , DruidFuncDimCol("My Date", DateType(), DRUID_TIME_FORMAT("YYYY-MM-dd HH"))

        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
        ),
        annotations = Set(DruidGroupByStrategyV2),
        underlyingTableName = Some("fact1")
      )
    }.toPublicFact("k_stats_select",
      Set(
        PubCol("My Date", "Day", InBetweenEquality),
        PubCol("id", "Keyword ID", InEquality),
        PubCol("campaign_id", "Campaign ID", InEquality),
        PubCol("advertiser_id", "Advertiser ID", InEquality),
        PubCol("price_type", "Pricing Type", In),
        PubCol("Derived Pricing Type", "Derived Pricing Type", InEquality),
      ),
      Set(
        PublicFactCol("impressions", "Impressions", InBetweenEquality)
        , PublicFactCol("clicks", "Clicks", InBetweenEquality)
      ),
      Set(),
      getMaxDaysWindow, getMaxDaysLookBack, renderLocalTimeFilter = false, dimRevision = 2
    )
  }


  private[this] def withDruidQueryExecutor(url: String,
                                           enableFallbackOnUncoveredIntervals: Boolean = false,
                                           allowPartialIfResultExceedsMaxRowLimit:Boolean = false)(fn: DruidQueryExecutor => Unit): Unit ={
    val executor = new DruidQueryExecutor(new DruidQueryExecutorConfig(50, 500, 5000, 5000, 5000, "config", url, None, 3000, 3000, 3000, 3000,
      true, 500, 3, enableFallbackOnUncoveredIntervals, allowPartialIfResultExceedsMaxRowLimit = allowPartialIfResultExceedsMaxRowLimit), new NoopExecutionLifecycleListener, ResultSetTransformer.DEFAULT_TRANSFORMS)
    try {
      fn(executor)
    } finally {
      executor.close()
    }
  }

  private[this] def getDruidQueryGenerator(maximumMaxRowsAsync: Int = 100): DruidQueryGenerator = {
    new DruidQueryGenerator(new SyncDruidQueryOptimizer(), 40000, maximumMaxRowsAsync = maximumMaxRowsAsync)
  }

  lazy val defaultRegistry = getDefaultRegistry()

  test("success case for TimeSeries query") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "213"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    withDruidQueryExecutor("http://localhost:6667/mock/timeseries"){
      executor =>
        val rowList = new CompleteRowList(query)
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(!result.rowList.isEmpty)
        var str: String = ""
        var count: Int = 0
        result.rowList.foreach {
          row => {
            count += 1

            str = str + s"$row"
          }
        }

        assert(count == 3)
        val expected = "Row(Map(Day -> 0, Impressions -> 1),ArrayBuffer(2012-01-01, 15))Row(Map(Day -> 0, Impressions -> 1),ArrayBuffer(2012-01-02, 16))Row(Map(Day -> 0, Impressions -> 1),ArrayBuffer(2012-01-03, 17))"

        str should equal(expected)(after being whiteSpaceNormalised)
        result.rowList.foreach { row =>
          val map = row.aliasMap
          for ((key, value) <- map) {
            assert(row.getValue(key) != null)
          }
        }
    }
  }

  test("success case for TimeSeries query with debugging") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "213"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "debug": true
                        }"""
    val request: ReportingRequest = getReportingRequestSync(jsonString).copy(additionalParameters = Map(Parameter.Debug -> DebugValue(value = true)))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    withDruidQueryExecutor("http://localhost:6667/mock/timeseries_debug"){
      executor =>
        val rowList = new CompleteRowList(query)
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(!result.rowList.isEmpty)
        var str: String = ""
        var count: Int = 0
        result.rowList.foreach {
          row => {
            count += 1

            str = str + s"$row"
          }
        }

        assert(count == 3)
        val expected = "Row(Map(Day -> 0, Impressions -> 1),ArrayBuffer(2012-01-01, 15))Row(Map(Day -> 0, Impressions -> 1),ArrayBuffer(2012-01-02, 16))Row(Map(Day -> 0, Impressions -> 1),ArrayBuffer(2012-01-03, 17))"

        str should equal(expected)(after being whiteSpaceNormalised)
        result.rowList.foreach { row =>
          val map = row.aliasMap
          for ((key, value) <- map) {
            assert(row.getValue(key) != null)
          }
        }
    }
  }

  test("Test TimeSeries query with response with missing result field") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "213"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]

    List("1", "2", "3", "4").foreach {
      idx =>
        withDruidQueryExecutor(s"http://localhost:6667/mock/timeseriesunsupported$idx") {
          executor =>
            val rowList = new CompleteRowList(query)
            val result = executor.execute(query, rowList, QueryAttributes.empty)
            assert(result.isFailure, s"idx=$idx result=${result.toString}")
            val thrown = result.exception.get
            assert(thrown.getMessage.contains("Unexpected field in timeseries json response"))
        }
    }
  }


  test("success case for TopN query") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "213"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":2
                        }"""
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    withDruidQueryExecutor("http://localhost:6667/mock/topn"){
      executor =>
        val rowList = new CompleteRowList(query)
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(!result.rowList.isEmpty)
        result.rowList.foreach {
          row =>
        }
        result.rowList.foreach { row =>
          val map = row.aliasMap
          for ((key, value) <- map) {
            assert(row.getValue(key) != null)
          }
        }
    }
  }

  test("TopN query with unhandled field") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "213"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":2
                        }"""
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    List("1", "2", "3", "4", "5").foreach {
      idx =>
        withDruidQueryExecutor(s"http://localhost:6667/mock/topnunsupported$idx") {
          executor =>
            val rowList = new CompleteRowList(query)
            val result = executor.execute(query, rowList, QueryAttributes.empty)
            assert(result.isFailure, s"idx=$idx result=${result.toString}")
            val thrown = result.exception.get
            assert(thrown.getMessage.contains("""Unexpected field in TopNDruidQuery json response"""))
        }
    }
  }


  test("TopN query with invalid response") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "213"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":2
                        }"""
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    withDruidQueryExecutor("http://localhost:6667/mock/timeseries"){
      executor =>
        val rowList = new CompleteRowList(query)
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(result.isFailure, result.toString)
        val thrown = result.exception.get
        assert(thrown.getMessage.contains("""Unexpected field in TopNDruidQuery json response"""))
    }
  }

  test("Test Response code other than 200") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "213"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    withDruidQueryExecutor("http://localhost:6667/mock/faultyn"){
      executor =>
        val rowList = new CompleteRowList(query)
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(result.isFailure, result.toString)
        val thrown = result.exception.get
        assert(thrown.getMessage().contains("received status code from druid is"))
    }
  }

  test("success case for groupby query execution") {

    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Keyword ID"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Pricing Type"},
                            {"field": "Average Bid"},
                            {"field": "Average Position"},
                            {"field": "Impressions"},
                            {"field": "Conversions"},
                            {"field": "Advertiser Status"},
                            {"field": "Impression Share"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "5485"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":2
                        }""".stripMargin
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]

    withDruidQueryExecutor("http://localhost:6667/mock/groupby"){
      executor =>
        val rowList = new CompleteRowList(query)
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(!result.rowList.isEmpty)

        var str: String = ""
        var count: Int = 0
        result.rowList.foreach {
          row => {
            count += 1

            str = str + s"$row"
          }
        }

        assert(count == 2)
        val expected = "Row(Map(Pricing Type -> 4, Impression Share -> 10, Keyword ID -> 1, Average Bid -> 5, Max Bid -> 2, Day -> 0, Advertiser Status -> 9, Impressions -> 7, Min Bid -> 3, Average Position -> 6, Conversions -> 8),ArrayBuffer(2012-01-01, 10, 163, 184, 11, 9, 205, 175, 15, ON, N/A))Row(Map(Pricing Type -> 4, Impression Share -> 10, Keyword ID -> 1, Average Bid -> 5, Max Bid -> 2, Day -> 0, Advertiser Status -> 9, Impressions -> 7, Min Bid -> 3, Average Position -> 6, Conversions -> 8),ArrayBuffer(2012-01-01, 14, 16, 18, 13, 15, 20, 17, 2, ON, 0.0123))"

        str should equal(expected)(after being whiteSpaceNormalised)

        result.rowList.foreach { row =>
          val map = row.aliasMap
          for ((key, value) <- map) {
            assert(row.getValue(key) != null)
          }
        }
    }
  }

  test("fail to execute groupby query type with unhandled json") {

    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Keyword ID"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Pricing Type"},
                            {"field": "Average Bid"},
                            {"field": "Average Position"},
                            {"field": "Impressions"},
                            {"field": "Conversions"},
                            {"field": "Advertiser Status"},
                            {"field": "Impression Share"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "5485"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":2
                        }""".stripMargin
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]

    List("1", "2", "3", "4").foreach {
      idx =>
        withDruidQueryExecutor(s"http://localhost:6667/mock/groupbyunsupported$idx") {
          executor =>
            val rowList = new CompleteRowList(query)
            val result = executor.execute(query, rowList, QueryAttributes.empty)
            assert(result.isFailure, s"idx=$idx result=${result.toString}")
            val thrown = result.exception.get
            assert(thrown.getMessage.contains("""Unexpected field in GroupByDruidQuery json response"""))
        }
    }
  }

  test("failure case for query execution when non paginated query returns max rows") {

    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Keyword ID"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Pricing Type"},
                            {"field": "Average Bid"},
                            {"field": "Average Position"},
                            {"field": "Impressions"},
                            {"field": "Conversions"},
                            {"field": "Advertiser Status"},
                            {"field": "Impression Share"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "5485"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":2
                        }""".stripMargin
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestAsync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator(2)) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]

    withDruidQueryExecutor("http://localhost:6667/mock/groupby") {
      executor =>
        val rowList = new CompleteRowList(query)
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(result.isFailure, result.toString)
        val thrown = result.exception.get
        assert(thrown.getMessage === "requirement failed: Non paginated query fails rowsCount < maxRows, partial result possible : rowsCount=2 maxRows=2")
    }
  }

  test("test fallback to oracle when druid fails with empty lookup value") {

    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Keyword ID"},
                            {"field": "Impressions"},
                            {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "5485"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":2
                        }""".stripMargin
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestAsync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    altQueryGeneratorRegistry.register(OracleEngine, new OracleQueryGenerator(DefaultPartitionColumnRenderer))
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]

    withDruidQueryExecutor("http://localhost:6667/mock/groupby_empty_lookup") {
      executor =>

        val oracleExecutor = new MockOracleQueryExecutor(
          { rl =>

            val expected =
              s"""
                 |SELECT *
                 |FROM (SELECT to_char(ffst0.stats_date, 'YYYYMMdd') "Day", ffst0.id "Keyword ID", coalesce(ffst0."impresssions", 1) "Impressions", ao1."Advertiser Status" "Advertiser Status"
                 |      FROM (SELECT
                 |                   advertiser_id, id, stats_date, SUM(impresssions) AS "impresssions"
                 |            FROM fd_fact_s_term FactAlias
                 |            WHERE (advertiser_id = 5485) AND (stats_date IN (to_date('${fromDate}', 'YYYYMMdd'),to_date('${toDate}', 'YYYYMMdd')))
                 |            GROUP BY advertiser_id, id, stats_date
                 |
            |           ) ffst0
                 |           LEFT OUTER JOIN
                 |           (SELECT  DECODE(status, 'ON', 'ON', 'OFF') AS "Advertiser Status", id
                 |            FROM advertiser_oracle
                 |            WHERE (id = 5485)
                 |             )
                 |           ao1 ON (ffst0.advertiser_id = ao1.id)
                 |
            |)
                 |   ORDER BY "Impressions" ASC NULLS LAST""".stripMargin

            rl.query.asString should equal(expected)(after being whiteSpaceNormalised)

            val row = rl.newRow
            row.addValue("Keyword ID", 14)
            row.addValue("Advertiser Status", "ON")
            row.addValue("Impressions", 10)
            rl.addRow(row)
            val row2 = rl.newRow
            row2.addValue("Keyword ID", 13)
            row2.addValue("Advertiser Status", "ON")
            row2.addValue("Impressions", 20)
            rl.addRow(row2)
          })

        val queryExecContext: QueryExecutorContext = new QueryExecutorContext
        queryExecContext.register(executor)
        queryExecContext.register(oracleExecutor)

        val result = queryPipelineTry.toOption.get.execute(queryExecContext)
        assert(result.isSuccess)

        var str: String = ""
        var count: Int = 0
        result.get.rowList.foreach {
          row => {
            count += 1
            str = str + s"$row"
          }
        }

        assert(count == 2)
        val expected = "Row(Map(Day -> 0, Keyword ID -> 1, Impressions -> 2, Advertiser Status -> 3),ArrayBuffer(null, 14, 10, ON))Row(Map(Day -> 0, Keyword ID -> 1, Impressions -> 2, Advertiser Status -> 3),ArrayBuffer(null, 13, 20, ON))"

        str should equal(expected)(after being whiteSpaceNormalised)
    }
  }

  test("success case for Bid Modifier and Modified Bid post results") {

    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Advertiser ID"},
                            {"field": "Spend"},
                            {"field": "Impressions"},
                            {"field": "Average Bid"},
                            {"field": "Bid Modifier"},
                            {"field": "Modified Bid"},
                            {"field": "Device Type"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "5430"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":2
                        }""".stripMargin
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]

    withDruidQueryExecutor("http://localhost:6667/mock/groupbybidmod") {
      executor =>
        val rowList = new CompleteRowList(query)
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(!result.rowList.isEmpty)

        var str: String = ""
        var count: Int = 0
        result.rowList.foreach {
          row => {
            count += 1

            str = str + s"$row"
          }
        }

        assert(count == 2)
        val expected = "Row(Map(Bid Modifier -> 5, Average Bid -> 4, Day -> 0, Modified Bid -> 6, Device Type -> 7, Impressions -> 3, Advertiser ID -> 1, Spend -> 2),ArrayBuffer(2017-08-07, 5430, 5793.1630783081, 2884485, 0.30467536, 9, 2.74208, 'Desktop'))Row(Map(Bid Modifier -> 5, Average Bid -> 4, Day -> 0, Modified Bid -> 6, Device Type -> 7, Impressions -> 3, Advertiser ID -> 1, Spend -> 2),ArrayBuffer(2017-08-08, 5430, 5237.0206726193, 2701909, 0.30023782, 8.99726, 2.70132, 'Desktop'))"

        str should equal(expected)(after being whiteSpaceNormalised)

        result.rowList.foreach { row =>
          val map = row.aliasMap
          for ((key, value) <- map) {
            assert(row.getValue(key) != null)
          }
        }
    }
  }

  test("success case for groupby query execution with Start of the week field") {

    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Week"},
                            {"field": "Keyword ID"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Pricing Type"},
                            {"field": "Average Bid"},
                            {"field": "Average Position"},
                            {"field": "Impressions"},
                            {"field": "Conversions"},
                            {"field": "Impression Share"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "5485"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":2
                        }""".stripMargin
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]

    withDruidQueryExecutor("http://localhost:6667/mock/groupby_start_of_week") {
      executor =>
        val rowList = new CompleteRowList(query)
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(!result.rowList.isEmpty)

        var str: String = ""
        var count: Int = 0
        result.rowList.foreach {
          row => {
            count += 1

            str = str + s"$row"
          }
        }

        assert(count == 2)
        val expected = "Row(Map(Pricing Type -> 4, Impression Share -> 9, Keyword ID -> 1, Average Bid -> 5, Max Bid -> 2, Impressions -> 7, Week -> 0, Min Bid -> 3, Average Position -> 6, Conversions -> 8),ArrayBuffer(2017-06-26, 10, 163, 184, 11, 9, 205, 175, 15, N/A))Row(Map(Pricing Type -> 4, Impression Share -> 9, Keyword ID -> 1, Average Bid -> 5, Max Bid -> 2, Impressions -> 7, Week -> 0, Min Bid -> 3, Average Position -> 6, Conversions -> 8),ArrayBuffer(2017-07-10, 14, 16, 18, 13, 15, 20, 17, 2, 0.0123))"

        str should equal(expected)(after being whiteSpaceNormalised)

        result.rowList.foreach { row =>
          val map = row.aliasMap
          for ((key, value) <- map) {
            assert(row.getValue(key) != null)
          }
        }
    }
  }

  test("success case for groupby query execution with Start of the month field") {

    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Month"},
                            {"field": "Keyword ID"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Pricing Type"},
                            {"field": "Average Bid"},
                            {"field": "Average Position"},
                            {"field": "Impressions"},
                            {"field": "Conversions"},
                            {"field": "Impression Share"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "5485"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":2
                        }""".stripMargin
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]

    withDruidQueryExecutor("http://localhost:6667/mock/groupby_start_of_month") {
      executor =>
        val rowList = new CompleteRowList(query)
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(!result.rowList.isEmpty)

        var str: String = ""
        var count: Int = 0
        result.rowList.foreach {
          row => {
            count += 1

            str = str + s"$row"
          }
        }

        assert(count == 2)
        val expected = "Row(Map(Month -> 0, Pricing Type -> 4, Impression Share -> 9, Keyword ID -> 1, Average Bid -> 5, Max Bid -> 2, Impressions -> 7, Min Bid -> 3, Average Position -> 6, Conversions -> 8),ArrayBuffer(2017-06-01, 10, 163, 184, 11, 9, 205, 175, 15, N/A))Row(Map(Month -> 0, Pricing Type -> 4, Impression Share -> 9, Keyword ID -> 1, Average Bid -> 5, Max Bid -> 2, Impressions -> 7, Min Bid -> 3, Average Position -> 6, Conversions -> 8),ArrayBuffer(2017-07-01, 14, 16, 18, 13, 15, 20, 17, 2, 0.0123))"

        str should equal(expected)(after being whiteSpaceNormalised)

        result.rowList.foreach { row =>
          val map = row.aliasMap
          for ((key, value) <- map) {
            assert(row.getValue(key) != null)
          }
        }
    }
  }

  test("success case for groupby query execution with startIndex < 0") {

    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Keyword ID"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Pricing Type"},
                            {"field": "Average Bid"},
                            {"field": "Average Position"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "5485"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":-1,
                          "rowsPerPage":2
                        }""".stripMargin
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    withDruidQueryExecutor("http://localhost:6667/mock/groupby") {
      executor =>
        val rowList = new CompleteRowList(query)
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(!result.rowList.isEmpty)

        var str: String = ""
        var count: Int = 0
        result.rowList.foreach {
          row => {
            count += 1

            str = str + s"$row"
          }
        }

        assert(count == 2)
        val expected = "Row(Map(Pricing Type -> 4, Keyword ID -> 1, Average Bid -> 5, Max Bid -> 2, Day -> 0, Impressions -> 7, Min Bid -> 3, Average Position -> 6),ArrayBuffer(2012-01-01, 10, 163, 184, 11, 9, 205, 175))Row(Map(Pricing Type -> 4, Keyword ID -> 1, Average Bid -> 5, Max Bid -> 2, Day -> 0, Impressions -> 7, Min Bid -> 3, Average Position -> 6),ArrayBuffer(2012-01-01, 14, 16, 18, 13, 15, 20, 17))"
        str should equal(expected)(after being whiteSpaceNormalised)

        result.rowList.foreach { row =>
          val map = row.aliasMap
          for ((key, value) <- map) {
            assert(row.getValue(key) != null)
          }
        }
    }
  }

  test("success case for groupby query execution with startIndex = 1") {

    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Keyword ID"},
                            {"field": "Country Name"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Pricing Type"},
                            {"field": "Average Bid"},
                            {"field": "Average Position"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "5485"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":1,
                          "rowsPerPage":2
                        }""".stripMargin
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    withDruidQueryExecutor("http://localhost:6667/mock/3rowsgroupby") {
      executor =>
        val rowList = new CompleteRowList(query)
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(!result.rowList.isEmpty)


        var str: String = ""
        var count: Int = 0
        result.rowList.foreach {
          row => {
            count += 1

            str = str + s"$row"
          }
        }


        assert(count == 2)
        val expected = "Row(Map(Pricing Type -> 5, Keyword ID -> 1, Average Bid -> 6, Max Bid -> 3, Day -> 0, Impressions -> 8, Min Bid -> 4, Average Position -> 7, Country Name -> 2),ArrayBuffer(2016-01-01, 2, US, 16, 18, 13, 15, 20, 17))Row(Map(Pricing Type -> 5, Keyword ID -> 1, Average Bid -> 6, Max Bid -> 3, Day -> 0, Impressions -> 8, Min Bid -> 4, Average Position -> 7, Country Name -> 2),ArrayBuffer(2016-01-01, 3, US, 160123456, 18, 13, 10, 20.12, 127))"
        str should equal(expected)(after being whiteSpaceNormalised)

        result.rowList.foreach { row =>
          val map = row.aliasMap
          for ((key, value) <- map) {
            assert(row.getValue(key) != null)
          }
        }
    }
  }

  test("groupby query execution with invalid url") {

    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Keyword ID"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Pricing Type"},
                            {"field": "Average Bid"},
                            {"field": "Average Position"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "5485"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":2
                        }""".stripMargin
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    withDruidQueryExecutor("http://localhost:6667/mock/topn") {
      executor =>
        val rowList = new CompleteRowList(query)
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(result.isFailure, result.toString)
        val thrown = result.exception.get
        assert(thrown.getMessage.contains("""Unexpected field in GroupByDruidQuery json response : """))
    }
  }


  test("success case for Partial TimeSeries query") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "213"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    withDruidQueryExecutor("http://localhost:6667/mock/timeseries") {
      executor =>
        val rowList: DimDrivenFactOrderedPartialRowList = new DimDrivenFactOrderedPartialRowList(RowGrouping("Impressions", List.empty), query)
        val row = rowList.newRow
        row.addValue("Impressions", java.lang.Integer.valueOf(15))
        row.addValue("Day", null)
        rowList.addRow(row)
        rowList.foreach {
          row =>
        }
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(!result.rowList.isEmpty)
        result.rowList.foreach {
          row =>
        }
        result.rowList.foreach { row =>
          val map = row.aliasMap
          for ((key, value) <- map) {
            assert(row.getValue(key) != null)
          }
        }
    }
  }


  test("success case for partial groupby query execution with no non fk dim filters") {

    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Pricing Type"},
                            {"field": "Average Bid"},
                            {"field": "Average Position"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "5485"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":2
                        }""".stripMargin
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    altQueryGeneratorRegistry.register(OracleEngine, new OracleQueryGenerator(DefaultPartitionColumnRenderer))
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    //val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    val executorContext = new QueryExecutorContext

    withDruidQueryExecutor("http://localhost:6667/mock/groupby") {
      executor =>
        executorContext.register(executor)
        executorContext.register(new MockOracleQueryExecutor({
          rowList =>
            rowList match {
              case irl: IndexedRowList =>

                val rowSet = irl.getRowByIndex(RowGrouping("10", List("10")))
                rowSet.map(_.addValue("Keyword Value", "ten"))
                val rowSet2 = irl.getRowByIndex(RowGrouping("14", List("14")))
                rowSet2.map(_.addValue("Keyword Value", "fourteen"))
              case any => throw new IllegalArgumentException("unexptected row list type")
            }

        }))
        val result = queryPipelineTry.toOption.get.execute(executorContext, QueryAttributes.empty).toOption.get

        assert(!result.rowList.isEmpty)
        result.rowList.foreach {
          row =>
        }
        result.rowList.foreach {
          row =>
            val map = row.aliasMap
            for ((key, value) <- map) {
              assert(row.getValue(key) != null)
            }
        }
    }
  }

  test("success case for partial groupby query execution with non fk dim filters") {

    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Advertiser Name"},
                            {"field": "Campaign Name"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Pricing Type"},
                            {"field": "Average Bid"},
                            {"field": "Average Position"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Keyword Status", "operator": "=", "value": "ON"},
                            {"field": "Advertiser ID", "operator": "=", "value": "5485"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":2,
                          "forceDimensionDriven": true,
                          "includeRowCount":true
                        }""".stripMargin
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    altQueryGeneratorRegistry.register(OracleEngine, new OracleQueryGenerator(DefaultPartitionColumnRenderer))
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val executorContext = new QueryExecutorContext
    withDruidQueryExecutor("http://localhost:6667/mock/groupby") {
      executor =>
        executorContext.register(executor)
        executorContext.register(new MockOracleQueryExecutor({
          rowList =>
            rowList match {
              case irl: IndexedRowList =>

                for {
                  row <- irl.getRowByIndex(RowGrouping("10", List("10")))
                } {
                  row.addValue("Keyword Value", "ten")
                  row.addValue("Advertiser Name", "advertiser-ten")
                  row.addValue("Campaign Name", "campaign-ten")
                }
                for {
                  row <- irl.getRowByIndex(RowGrouping("14", List("14")))
                } {
                  row.addValue("Keyword Value", "fourteen")
                  row.addValue("Advertiser Name", "advertiser-fourteen")
                  row.addValue("Campaign Name", "campaign-fourteen")
                }
              case any => throw new IllegalArgumentException("unexptected row list type")

            }

        }))
        val resultTry = queryPipelineTry.toOption.get.execute(executorContext, QueryAttributes.empty)
        assert(resultTry.isSuccess, resultTry.errorMessage("Fail to execute pipeline"))

        val result = resultTry.toOption.get
        assert(!result.rowList.isEmpty)
        result.rowList.foreach {
          row =>
        }
        result.rowList.foreach { row =>
          val map = row.aliasMap
          for ((key, value) <- map) {
            assert(row.getValue(key) != null)
          }
        }
        val sql = queryPipelineTry.toOption.get.queryChain.subsequentQueryList.head.asString

        assert(sql contains "ROWNUM")
        assert(!sql.contains("TotalRows"))
    }
  }

  test("success case for groupby fact driven query execution") {

    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Pricing Type"},
                            {"field": "Average Bid"},
                            {"field": "Average Position"},
                            {"field": "Impressions"},
                            {"field": "Impression Share"}
                          ],
                          "forceDimensionDriven": false,
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "5485"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"},
                            {"field": "Keyword Value", "order": "Asc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":2
                        }""".stripMargin
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    altQueryGeneratorRegistry.register(OracleEngine, new OracleQueryGenerator(DefaultPartitionColumnRenderer))
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val executorContext = new QueryExecutorContext
    withDruidQueryExecutor("http://localhost:6667/mock/groupby") {
      executor =>
        executorContext.register(executor)
        executorContext.register(new MockOracleQueryExecutor({
          rowList =>
            rowList match {
              case irl: IndexedRowList =>

                for {
                  row <- irl.getRowByIndex(RowGrouping("10", List.empty))
                } {
                  row.addValue("Keyword Value", "ten")
                }
                for {

                  row <- irl.getRowByIndex(RowGrouping("14", List.empty))
                } {
                  row.addValue("Keyword Value", "fourteen")
                }
                val newRow = irl.newRow
                newRow.addValue(irl.rowGrouping.indexAlias, "11")
                newRow.addValue("Keyword Value", "eleven")
                irl.updateRow(newRow)
              case any => throw new IllegalArgumentException("unexptected row list type")

            }

        }))
        val resultTry = queryPipelineTry.toOption.get.execute(executorContext, QueryAttributes.empty)
        assert(resultTry.isSuccess, resultTry.errorMessage("Fail to execute pipeline"))

        val result = resultTry.toOption.get
        assert(!result.rowList.isEmpty)
        var rowCount = 0
        result.rowList.foreach(row => rowCount = rowCount + 1)
        assert(rowCount == 2)
        result.rowList.foreach {
          row =>
        }
        result.rowList.foreach {
          row =>
            val map = row.aliasMap
            for ((key, value) <- map) {
              assert(row.getValue(key) != null)
            }
        }
    }
  }

  test("success case for TopN query with partial result set") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "213"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":2
                        }"""
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    withDruidQueryExecutor("http://localhost:6667/mock/topn") {
      executor =>
        val rowList: DimDrivenFactOrderedPartialRowList = new DimDrivenFactOrderedPartialRowList(RowGrouping("Keyword ID", List.empty), query)
        val row1 = rowList.newRow
        row1.addValue("Keyword ID", 14)
        row1.addValue("Impressions", null)
        rowList.addRow(row1)
        val row2 = rowList.newRow
        row2.addValue("Keyword ID", 13)
        row2.addValue("Impressions", null)
        rowList.addRow(row2)
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(!result.rowList.isEmpty)
        result.rowList.foreach {
          row =>
        }
        result.rowList.foreach { row =>
          val map = row.aliasMap
          for ((key, value) <- map) {
            assert(row.getValue(key) != null)
          }
        }
    }
  }

  test("Handle null values from druid gracefully") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "213"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":2
                        }"""
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    withDruidQueryExecutor("http://localhost:6667/mock/topnWithNull") {
      executor =>
        val rowList: DimDrivenFactOrderedPartialRowList = new DimDrivenFactOrderedPartialRowList(RowGrouping("Keyword ID", List.empty), query)
        val row1 = rowList.newRow
        row1.addValue("Keyword ID", 14)
        row1.addValue("Impressions", null)
        rowList.addRow(row1)
        val row2 = rowList.newRow
        row2.addValue("Keyword ID", 13)
        row2.addValue("Impressions", null)
        rowList.addRow(row2)
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(result.rowList.isEmpty)
    }
  }


  test("success case for TopN query with partial result set: MultiEngine") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Impressions"},
                            {"field": "Keyword Value"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "213"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    altQueryGeneratorRegistry.register(OracleEngine, new OracleQueryGenerator(DefaultPartitionColumnRenderer))
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    withDruidQueryExecutor("http://localhost:6667/mock/topn") {
      druidExecutor =>
        val oracleExecutor = new MockOracleQueryExecutor(
          { rl =>
            val row = rl.newRow
            row.addValue("Keyword ID", 14)
            row.addValue("Keyword Value", "one")
            rl.addRow(row)
            val row2 = rl.newRow
            row2.addValue("Keyword ID", 13)
            row2.addValue("Keyword Value", "two")
            rl.addRow(row2)
          })
        val queryExecContext: QueryExecutorContext = new QueryExecutorContext
        queryExecContext.register(druidExecutor)
        queryExecContext.register(oracleExecutor)

        val result = queryPipelineTry.toOption.get.execute(queryExecContext)
        assert(result.isSuccess)

        //var result = oracleExecutor.execute(oraclQuery.head, rowList, QueryAttributes.empty)
        val oracleQuery = queryPipelineTry.toOption.get.queryChain.subsequentQueryList.head
        //require(result.rowList, "Failed to execute MultiEngine Query")

        val expected =
          """
            | (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT  *
            |      FROM (SELECT t0.id "Keyword ID", t0.value "Keyword Value"
            |            FROM
            |                (SELECT  value, id, advertiser_id
            |            FROM targetingattribute
            |            WHERE (advertiser_id = 213) AND (id IN (13,14))
            |             ) t0
            |
            |
            |           )
            |            ) D )) UNION ALL (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  *
            |      FROM (SELECT t0.id "Keyword ID", t0.value "Keyword Value"
            |            FROM
            |                (SELECT  value, id, advertiser_id
            |            FROM targetingattribute
            |            WHERE (advertiser_id = 213) AND (id NOT IN (13,14))
            |             ) t0
            |
            |
            |           )
            |            ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100)
            |  """.stripMargin

        oracleQuery.asString should equal(expected)(after being whiteSpaceNormalised)
    }
  }

  test("success case for TopN query with partial result set: MultiEngine with 2 sorts") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [

                            {"field": "Keyword ID"},
                            {"field": "Impressions"},
                            {"field": "Keyword Value"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "213"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"},
                            {"field": "Keyword ID", "order": "Asc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    altQueryGeneratorRegistry.register(OracleEngine, new OracleQueryGenerator(DefaultPartitionColumnRenderer))
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]

    withDruidQueryExecutor("http://localhost:6667/mock/druidPlusOraclegroupby") {
      druidExecutor =>
        val oracleExecutor = new MockOracleQueryExecutor(
          { rl =>
            val row = rl.newRow
            row.addValue("Keyword ID", 14)
            row.addValue("Keyword Value", "one")
            rl.addRow(row)
            val row2 = rl.newRow
            row2.addValue("Keyword ID", 13)
            row2.addValue("Keyword Value", "two")
            rl.addRow(row2)
          })
        val queryExecContext: QueryExecutorContext = new QueryExecutorContext
        queryExecContext.register(druidExecutor)
        queryExecContext.register(oracleExecutor)

        val result = queryPipelineTry.toOption.get.execute(queryExecContext)
        assert(result.isSuccess)

        val oracleQuery = queryPipelineTry.toOption.get.queryChain.subsequentQueryList.head

        val expected =
          s"""
             | (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT  *
             |      FROM (SELECT t0.id "Keyword ID", t0.value "Keyword Value"
             |            FROM
             |                (SELECT  id, value, advertiser_id
             |            FROM targetingattribute
             |            WHERE (advertiser_id = 213) AND (id IN (13,14))
             |            ORDER BY 1 ASC  ) t0
             |
                       |
                       |           )
             |            ) D )) UNION ALL (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  *
             |      FROM (SELECT t0.id "Keyword ID", t0.value "Keyword Value"
             |            FROM
             |                (SELECT  id, value, advertiser_id
             |            FROM targetingattribute
             |            WHERE (advertiser_id = 213) AND (id NOT IN (13,14))
             |            ORDER BY 1 ASC  ) t0
             |
                       |
                       |           )
             |            ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100)  """
            .stripMargin

        oracleQuery.asString should equal(expected)(after being whiteSpaceNormalised)
    }
  }

  test("Invalid field extraction failure") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "213"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":2
                        }""".stripMargin
    val request: ReportingRequest = getReportingRequestSync(jsonString).copy(additionalParameters = Map(Parameter.Debug -> DebugValue(value = true)))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    withDruidQueryExecutor("http://localhost:6667/mock/faultytopn") {
      executor =>
        val rowList = new CompleteRowList(query)
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(result.isFailure, result.toString)
        val thrown = result.exception.get
        assert(thrown.getMessage.contains("unsupported field type"))
    }
  }

  test("TopN query with partial result set and request code not 200") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "213"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    withDruidQueryExecutor("http://localhost:6667/mock/bh") {
      executor =>
        val rowList: DimDrivenFactOrderedPartialRowList = new DimDrivenFactOrderedPartialRowList(RowGrouping("Keyword ID", List.empty), query)
        val row1 = rowList.newRow
        row1.addValue("Keyword ID", 14)
        row1.addValue("Impressions", null)
        rowList.addRow(row1)
        val row2 = rowList.newRow
        row2.addValue("Keyword ID", 13)
        row2.addValue("Impressions", null)
        rowList.addRow(row2)
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(result.isFailure, result.toString)
        val thrown = result.exception.get
        assert(thrown.getMessage().contains("received status code from druid is"))
    }
  }

  test("Test TimeSeries query with partial result set with missing result field") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "213"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    withDruidQueryExecutor("http://localhost:6667/mock/timeseriesunsupported1") {
      executor =>
        val rowList: DimDrivenFactOrderedPartialRowList = new DimDrivenFactOrderedPartialRowList(RowGrouping("Impressions", List.empty), query)
        val row = rowList.newRow
        row.addValue("Impressions", java.lang.Integer.valueOf(15))
        row.addValue("Day", null)
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(result.isFailure, result.toString)
        val thrown = result.exception.get
        assert(thrown.getMessage.contains("Unexpected field in timeseries json response"))
    }
  }


  test("testing extract field") {
    val bool = JBool(true)
    val boolVal = DruidQueryExecutor.extractField(bool)
    assert(true == boolVal)
    val string = JString("s")
    val stringVal = DruidQueryExecutor.extractField(string)
    assert("s" == stringVal)
    val dec = JDecimal(1.0)
    val decVal = DruidQueryExecutor.extractField(dec)
    assert(1.0 == decVal)
    val int = JInt(1)
    val intVal = DruidQueryExecutor.extractField(int)
    assert(1 == intVal)
    val double = JDouble(1112.23)
    val doubleVal = DruidQueryExecutor.extractField(double)
    assert(1112.23 == doubleVal)
    val long = JLong(1112)
    val longVal = DruidQueryExecutor.extractField(long)
    assert(1112 == longVal)
  }

  test("Fact View Query Tests Adjustment Stats without Filter") {
    val jsonString =
      s"""{ "cube": "a_stats",
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
         |      },
         |      {
         |         "field": "Der Fact Col A"
         |      }
         |   ],
         |   "filterExpressions": [
         |      {
         |         "field": "Advertiser ID",
         |         "operator": "=",
         |         "value": "12345"
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

    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSyncWithFactBias(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)

    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    withDruidQueryExecutor("http://localhost:6667/mock/adjustmentStatsGroupBy") {
      druidExecutor =>

        val queryExecContext: QueryExecutorContext = new QueryExecutorContext
        queryExecContext.register(druidExecutor)

        val result = queryPipelineTry.toOption.get.execute(queryExecContext)
        assert(result.isSuccess)

        val expectedSet = Set(
          "Row(Map(Is Adjustment -> 2, Der Fact Col A -> 5, Day -> 1, Impressions -> 3, Advertiser ID -> 0, Spend -> 4),ArrayBuffer(184, 2012-01-01, N, 100, 15, 1))"
          , "Row(Map(Is Adjustment -> 2, Der Fact Col A -> 5, Day -> 1, Impressions -> 3, Advertiser ID -> 0, Spend -> 4),ArrayBuffer(199, 2012-01-01, N, 100, 10, 1))"
          , "Row(Map(Is Adjustment -> 2, Der Fact Col A -> 5, Day -> 1, Impressions -> 3, Advertiser ID -> 0, Spend -> 4),ArrayBuffer(184, 2012-01-01, Y, 100, 15, 0))"
          , "Row(Map(Is Adjustment -> 2, Der Fact Col A -> 5, Day -> 1, Impressions -> 3, Advertiser ID -> 0, Spend -> 4),ArrayBuffer(199, 2012-01-01, Y, 100, 10, 0))"
        )
        var count = 0
        result.get.rowList.foreach {
          row =>

            assert(expectedSet.contains(row.toString))
            count += 1
        }
        assert(expectedSet.size == count)
    }
  }

  test("Async Fact View Query Tests A Stats Api Side join") {
    val jsonString =
      s"""{ "cube": "a_stats",
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
         |      },
         |      {
         |         "field": "Der Fact Col A"
         |      }
         |   ],
         |   "filterExpressions": [
         |      {
         |         "field": "Advertiser ID",
         |         "operator": "=",
         |         "value": "12345"
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

    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestAsync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)

    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    assert(queryPipelineTry.get.queryChain.isInstanceOf[MultiQuery])

    withDruidQueryExecutor("http://localhost:6667/mock/adjustmentStatsGroupBy") {
      druidExecutor =>

        val queryExecContext: QueryExecutorContext = new QueryExecutorContext
        queryExecContext.register(druidExecutor)

        val result = queryPipelineTry.toOption.get.execute(queryExecContext)
        assert(result.isSuccess)

        val expectedSet = Set(
          "Row(Map(Is Adjustment -> 2, Der Fact Col A -> 5, Day -> 1, Impressions -> 3, Advertiser ID -> 0, Spend -> 4),ArrayBuffer(184, 2012-01-01, N, 100, 15, 1))"
          , "Row(Map(Is Adjustment -> 2, Der Fact Col A -> 5, Day -> 1, Impressions -> 3, Advertiser ID -> 0, Spend -> 4),ArrayBuffer(199, 2012-01-01, N, 100, 10, 1))"
          , "Row(Map(Is Adjustment -> 2, Der Fact Col A -> 5, Day -> 1, Impressions -> 3, Advertiser ID -> 0, Spend -> 4),ArrayBuffer(184, 2012-01-01, Y, 100, 15, 0))"
          , "Row(Map(Is Adjustment -> 2, Der Fact Col A -> 5, Day -> 1, Impressions -> 3, Advertiser ID -> 0, Spend -> 4),ArrayBuffer(199, 2012-01-01, Y, 100, 10, 0))"
        )
        var count = 0
        result.get.rowList.foreach {
          row =>

            assert(expectedSet.contains(row.toString))
            count += 1
        }
        assert(expectedSet.size == count)
    }
  }

  test("Successfully get the result if result rowCount exceeds maxRowLimit, if allowPartialIfResultExceedsMaxRowLimit is set") {
    val jsonString =
      s"""{ "cube": "a_stats",
         |  "rowsPerPage":3,
         |   "selectFields": [
         |      {
         |         "field": "Advertiser ID"
         |      },
         |      {
         |         "field": "Day"
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
         |         "value": "12345"
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

    val request: ReportingRequest = ReportingRequest.forceDruid(getReportingRequestAsync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)

    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.get

    val query = queryPipeline.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]

    assert(query.maxRows == 3)

    assert(queryPipeline.queryChain.isInstanceOf[MultiQuery])
    val multiQuery = queryPipeline.queryChain.asInstanceOf[MultiQuery]

    assert(multiQuery.unionQueryList.size == 2)

    assert(queryPipeline.queryChain.asInstanceOf[MultiQuery].unionQueryList.nonEmpty)

    withDruidQueryExecutor("http://localhost:6667/mock/maxRowTest", allowPartialIfResultExceedsMaxRowLimit = true) {
      druidExecutor =>

        val queryExecContext: QueryExecutorContext = new QueryExecutorContext
        queryExecContext.register(druidExecutor)

        val result = queryPipelineTry.toOption.get.execute(queryExecContext)
        assert(result.isSuccess)

        val expectedSet = Set(
          "Row(Map(Advertiser ID -> 0, Day -> 1, Impressions -> 2, Spend -> 3),ArrayBuffer(184, 2012-01-01, 200, 30.0))" ,
            "Row(Map(Advertiser ID -> 0, Day -> 1, Impressions -> 2, Spend -> 3),ArrayBuffer(199, 2012-01-01, 400, 40.0))"
        )

        assert(result.get.rowList.isInstanceOf[UnionViewRowList])

        var count = 0
        result.get.rowList.foreach {
          row =>
            assert(expectedSet.contains(row.toString))
            count += 1
        }
        assert(count == 2)
    }
  }

  test("Fact View Query Tests Adjustment Stats with constant column filter") {
    val jsonString =
      s"""{ "cube": "a_stats",
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

    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSyncWithFactBias(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)

    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    withDruidQueryExecutor("http://localhost:6667/mock/adjustmentStatsGroupBy") {
      druidExecutor =>

        val queryExecContext: QueryExecutorContext = new QueryExecutorContext
        queryExecContext.register(druidExecutor)

        val queryList = queryPipelineTry.toOption.get.queryChain.asInstanceOf[MultiQuery].unionQueryList

        queryList.foreach {
          query =>

        }

        val result = queryPipelineTry.toOption.get.execute(queryExecContext)
        assert(result.isSuccess)

        val expectedSet = Set(
          "Row(Map(Is Adjustment -> 2, Day -> 1, Impressions -> 3, Advertiser ID -> 0, Spend -> 4),ArrayBuffer(184, 2012-01-01, Y, 100, 15))"
          , "Row(Map(Is Adjustment -> 2, Day -> 1, Impressions -> 3, Advertiser ID -> 0, Spend -> 4),ArrayBuffer(199, 2012-01-01, Y, 100, 10))"
        )
        var count = 0
        result.get.rowList.foreach {
          row =>

            assert(expectedSet.contains(row.toString))
            count += 1
        }
        assert(expectedSet.size == count)
    }
  }

  test("Exectue Hourly grain query with day field") {

    val jsonString =
      s"""
         |{
         |   "cube":"wg_stats",
         |   "sortBy":[
         |      {
         |         "field":"Spend Usd",
         |         "order":"DESC"
         |      }
         |   ],
         |   "selectFields":[
         |      {
         |         "field":"Day"
         |      },
         |      {
         |        "field":"Hour"
         |      },
         |      {
         |         "field":"Spend Usd"
         |      }
         |   ],
         |   "paginationStartIndex":0,
         |   "filterExpressions":[
         |      {
         |         "operator":"between",
         |         "field":"Day",
         |         "from":"$fromDate",
         |         "to":"$toDate"
         |      }
         |   ],
         |   "rowsPerPage":1000
         |}
       """.stripMargin
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString, InternalSchema))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)

    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    withDruidQueryExecutor("http://localhost:6667/mock/whiteGloveGroupBy") {
      druidExecutor =>

        val queryExecContext: QueryExecutorContext = new QueryExecutorContext
        queryExecContext.register(druidExecutor)

        val result = queryPipelineTry.toOption.get.execute(queryExecContext)
        assert(result.isSuccess)
        val expectedSet = Set(
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 12, 100702.5269201994))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 10, 100105.4773756862))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 11, 99452.796035409))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 12, 99278.7537128925))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 11, 97812.4866646528))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 10, 97590.620177567))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 13, 97567.9216952436))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 13, 96825.4939264059))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 14, 94937.976395607))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 15, 93734.2041929066))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 14, 93478.0812258236))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 15, 93372.6298325285))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 16, 92731.1196420193))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 09, 92407.9271872044))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 16, 92231.0408502519))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 09, 91216.1552691944))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 17, 86134.2663560882))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 17, 83792.8106330931))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 08, 81854.4767573178))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 08, 80682.3556755185))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 18, 78679.2603152394))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 18, 78148.13511765))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 19, 74984.7429508232))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 19, 74374.6146068573))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 20, 70020.2893772125))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 20, 69916.2466526032))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 07, 68890.5210767388))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 07, 67616.8263101578))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 21, 64026.7750177383))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 21, 61437.9518136978))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 22, 54657.0613212585))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 00, 54380.0269228816))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 06, 52897.4322437644))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 00, 52755.8068522811))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 06, 52233.0503473878))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 22, 50291.442117691))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 23, 41530.911318697))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 01, 41114.4911497831))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 01, 40357.949185878))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 05, 39912.345539093))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 23, 39016.8020768166))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 05, 39015.8409667015))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 02, 35015.9358971715))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 02, 34740.9374386966))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 04, 33221.9400693774))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 04, 32514.7562770247))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-02, 03, 31368.6910836697))",
          "Row(Map(Day -> 0, Hour -> 1, Spend Usd -> 2),ArrayBuffer(2017-05-01, 03, 31223.5407607555))"
        )
        var count = 0
        result.get.rowList.foreach {
          row =>
            assert(expectedSet.contains(row.toString))
            count += 1
        }
        assert(expectedSet.size == count)

    }
  }

  test("Uncovered Interval response: should not fall back") {

    val jsonString =
      s"""
         |{
         |   "cube": "k_stats",
         |                          "selectFields": [
         |                            {"field": "Day"},
         |                            {"field": "Keyword ID"},
         |                            {"field": "Impressions"},
         |                            {"field": "Advertiser Status"}
         |                          ],
         |                          "filterExpressions": [
         |                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
         |                            {"field": "Advertiser ID", "operator": "=", "value": "5485"}
         |                          ],
         |                          "sortBy": [
         |                            {"field": "Impressions", "order": "Asc"}
         |                          ],
         |                          "paginationStartIndex":0,
         |                          "rowsPerPage":3
         |}
       """.stripMargin
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestAsync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    altQueryGeneratorRegistry.register(OracleEngine, new OracleQueryGenerator(DefaultPartitionColumnRenderer))
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]

    withDruidQueryExecutor("http://localhost:6667/mock/uncoveredNonempty", true) {
      executor =>

        val oracleExecutor = new MockOracleQueryExecutor(
          { rl =>

            val expected =
              s"""
                 |SELECT *
                 |FROM (SELECT to_char(ffst0.stats_date, 'YYYYMMdd') "Day", ffst0.id "Keyword ID", coalesce(ffst0."impresssions", 1) "Impressions", ao1."Advertiser Status" "Advertiser Status"
                 |      FROM (SELECT
                 |                   advertiser_id, id, stats_date, SUM(impresssions) AS "impresssions"
                 |            FROM fd_fact_s_term FactAlias
                 |            WHERE (advertiser_id = 5485) AND (stats_date IN (to_date('${fromDate}', 'YYYYMMdd'),to_date('${toDate}', 'YYYYMMdd')))
                 |            GROUP BY advertiser_id, id, stats_date
                 |
            |           ) ffst0
                 |           LEFT OUTER JOIN
                 |           (SELECT  DECODE(status, 'ON', 'ON', 'OFF') AS "Advertiser Status", id
                 |            FROM advertiser_oracle
                 |            WHERE (id = 5485)
                 |             )
                 |           ao1 ON (ffst0.advertiser_id = ao1.id)
                 |
            |)
                 |   ORDER BY "Impressions" ASC NULLS LAST""".stripMargin

            rl.query.asString should equal(expected)(after being whiteSpaceNormalised)

            val row = rl.newRow
            row.addValue("Keyword ID", 14)
            row.addValue("Advertiser Status", "ON")
            row.addValue("Impressions", 10)
            rl.addRow(row)
            val row2 = rl.newRow
            row2.addValue("Keyword ID", 13)
            row2.addValue("Advertiser Status", "ON")
            row2.addValue("Impressions", 20)
            rl.addRow(row2)
          })

        val queryExecContext: QueryExecutorContext = new QueryExecutorContext
        queryExecContext.register(executor)
        queryExecContext.register(oracleExecutor)

        val result = queryPipelineTry.toOption.get.execute(queryExecContext)
        assert(result.isSuccess)

        val expected = List("Row(Map(Day -> 0, Keyword ID -> 1, Impressions -> 2, Advertiser Status -> 3),ArrayBuffer(null, 10, 175, null))"
          , "Row(Map(Day -> 0, Keyword ID -> 1, Impressions -> 2, Advertiser Status -> 3),ArrayBuffer(null, 14, 17, null))")

        result.get.rowList.foreach {
          row =>

            assert(expected.contains(row.toString))
        }
    }
  }

  test("successfully generate the dim only INNER JOIN query if request has hasNonDrivingDimNonFKNonPKFilter in MultiEngine Case") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Ad Group ID"},
                            {"field": "Campaign ID"},
                            {"field": "Impressions"},
                            {"field": "Campaign Name"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "213"},
                            {"field": "Ad Group Status", "operator": "not in", "values": ["DELETED"]},
                            {"field": "Campaign Status", "operator": "not in", "values": ["DELETED"]}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":100,
                          "isDimDriven" : true
                        }"""
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString, AdvertiserLowLatencySchema))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    altQueryGeneratorRegistry.register(OracleEngine, new OracleQueryGenerator(DefaultPartitionColumnRenderer))
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]

    withDruidQueryExecutor("http://localhost:6667/mock/topnUiQuery") {
      druidExecutor =>
        val oracleExecutor = new MockOracleQueryExecutor(
          { rl =>
            val row = rl.newRow
            row.addValue("Ad Group ID", 114)
            row.addValue("Campaign ID", 14)
            row.addValue("Campaign Name", "Fourteen")
            rl.addRow(row)
            val row2 = rl.newRow
            row2.addValue("Ad Group ID", 113)
            row2.addValue("Campaign ID", 13)
            row2.addValue("Campaign Name", "Thirteen")
            rl.addRow(row2)
          })
        val queryExecContext: QueryExecutorContext = new QueryExecutorContext
        queryExecContext.register(druidExecutor)
        queryExecContext.register(oracleExecutor)

        val result = queryPipelineTry.toOption.get.execute(queryExecContext)
        assert(result.isSuccess)

        //var result = oracleExecutor.execute(oraclQuery.head, rowList, QueryAttributes.empty)
        val oracleQuery = queryPipelineTry.toOption.get.queryChain.subsequentQueryList.head
        //require(result.rowList, "Failed to execute MultiEngine Query")

        val expected =
          """
            |
            | (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT  *
            |      FROM (SELECT ago1.id "Ad Group ID", co0.id "Campaign ID", co0.campaign_name "Campaign Name"
            |            FROM
            |               ( (SELECT  campaign_id, id, advertiser_id
            |            FROM ad_group_oracle
            |            WHERE (advertiser_id = 213) AND (id IN (113,114)) AND (DECODE(status, 'ON', 'ON', 'OFF') NOT IN ('DELETED'))
            |             ) ago1
            |          INNER JOIN
            |            (SELECT /*+ CampaignHint */ campaign_name, id, advertiser_id
            |            FROM campaign_oracle
            |            WHERE (advertiser_id = 213) AND (DECODE(status, 'ON', 'ON', 'OFF') NOT IN ('DELETED'))
            |             ) co0
            |              ON( ago1.advertiser_id = co0.advertiser_id AND ago1.campaign_id = co0.id )
            |               )
            |
            |           )
            |            ) D )) UNION ALL (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  *
            |      FROM (SELECT ago1.id "Ad Group ID", co0.id "Campaign ID", co0.campaign_name "Campaign Name"
            |            FROM
            |               ( (SELECT  campaign_id, id, advertiser_id
            |            FROM ad_group_oracle
            |            WHERE (advertiser_id = 213) AND (id NOT IN (113,114)) AND (DECODE(status, 'ON', 'ON', 'OFF') NOT IN ('DELETED'))
            |             ) ago1
            |          INNER JOIN
            |            (SELECT /*+ CampaignHint */ campaign_name, id, advertiser_id
            |            FROM campaign_oracle
            |            WHERE (advertiser_id = 213) AND (DECODE(status, 'ON', 'ON', 'OFF') NOT IN ('DELETED'))
            |             ) co0
            |              ON( ago1.advertiser_id = co0.advertiser_id AND ago1.campaign_id = co0.id )
            |               )
            |
            |           )
            |            ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100)
            |  """.stripMargin

        oracleQuery.asString should equal(expected)(after being whiteSpaceNormalised)
    }

  }

  test("successfully execute scan query type") {

    val jsonString =
      s"""{
                          "queryType": "scan",
                          "cube": "k_stats_select",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Keyword ID"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "5485"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":5
                        }""".stripMargin
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    println(query.asString)

    withDruidQueryExecutor("http://localhost:6667/mock/select") {
      executor =>
        val rowList = new CompleteRowList(query)
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(!result.rowList.isEmpty)

        var str: String = ""
        var count: Int = 0
        result.rowList.foreach {
          row => {
            count += 1

            str = str + s"$row"
          }
        }

        assert(count == 5)
        val expected = "Row(Map(Day -> 0, Keyword ID -> 1, Impressions -> 2),ArrayBuffer(2013-01-01 00, id1, 1))Row(Map(Day -> 0, Keyword ID -> 1, Impressions -> 2),ArrayBuffer(2013-01-01 00, id2, 2))Row(Map(Day -> 0, Keyword ID -> 1, Impressions -> 2),ArrayBuffer(2013-01-01 00, id3, 3))Row(Map(Day -> 0, Keyword ID -> 1, Impressions -> 2),ArrayBuffer(2013-01-01 00, id4, 4))Row(Map(Day -> 0, Keyword ID -> 1, Impressions -> 2),ArrayBuffer(2013-01-01 00, id5, 5))"

        str should equal(expected)(after being whiteSpaceNormalised)

        result.rowList.foreach { row =>
          val map = row.aliasMap
          for ((key, value) <- map) {
            assert(row.getValue(key) != null)
          }
        }

        assert(!result.pagination.isDefined)
    }
  }

  test("fail to execute scan query type with unhandled json") {

    val jsonString =
      s"""{
                          "queryType": "scan",
                          "cube": "k_stats_select",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Keyword ID"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "5485"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":5
                        }""".stripMargin
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]

    List("1", "2", "3", "4", "5", "6", "7").foreach {
      idx =>
        withDruidQueryExecutor(s"http://localhost:6667/mock/selectunsupported$idx") {
          executor =>
            val rowList = new CompleteRowList(query)
            val result = executor.execute(query, rowList, QueryAttributes.empty)
            assert(result.isFailure, s"idx=$idx result=${result.toString}")
            val thrown = result.exception.get
            assert(thrown.getMessage.contains("""Unexpected field in SelectDruidQuery json response"""))
        }
    }
  }

  test("fail to execute query when rowList throws exception") {

    val jsonString =
      s"""{
                          "queryType": "scan",
                          "cube": "k_stats_select",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Keyword ID"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "5485"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":5
                        }""".stripMargin
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    println(query.asString)

    withDruidQueryExecutor("http://localhost:6667/mock/select") {
      executor =>
        val rowList = spy(new CompleteRowList(query))
        when(rowList.addRow(anyObject(), anyObject())).thenThrow(new UnsupportedOperationException("fail"))
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(result.isFailure, result.toString)
        val thrown = result.exception.get
        assert(thrown.getMessage.contains("""fail"""))
    }
  }

  test("fail to execute query which is unknown type") {

    val jsonString =
      s"""{
                          "queryType": "scan",
                          "cube": "k_stats_select",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Keyword ID"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "5485"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":5
                        }""".stripMargin
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val actualQuery = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]

    val query = new DruidQuery[Result[ScanResultValue]] {
      override def query: org.apache.druid.query.Query[Result[ScanResultValue]] = actualQuery.query.asInstanceOf[org.apache.druid.query.Query[Result[ScanResultValue]]]

      override def maxRows: Int = actualQuery.maxRows

      override def isPaginated: Boolean = actualQuery.isPaginated

      override def queryContext: QueryContext = actualQuery.queryContext

      override def aliasColumnMap: Map[String, Column] = actualQuery.aliasColumnMap

      override def additionalColumns: IndexedSeq[String] = actualQuery.additionalColumns
    }

    withDruidQueryExecutor("http://localhost:6667/mock/select") {
      executor =>
        val rowList = new CompleteRowList(query)
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(result.isFailure, result.toString)
        val thrown = result.exception.get
        assert(thrown.getMessage.contains("Druid Query type not supported"))
    }
  }

  test("fail to execute oracle query") {

    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Keyword ID"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "5485"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":5
                        }""".stripMargin
    val request: ReportingRequest = ReportingRequest.forceOracle(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(OracleEngine, new OracleQueryGenerator(DefaultPartitionColumnRenderer))
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery]

    withDruidQueryExecutor("http://localhost:6667/mock/groupby") {
      executor =>
        val rowList = new CompleteRowList(query)
        val thrown = intercept[UnsupportedOperationException] {
          executor.execute(query, rowList, QueryAttributes.empty)
        }
        assert(thrown.getMessage.contains("DruidQueryExecutor does not support query with engine=Oracle"))
    }
  }

  test("test row count groupby query execution") {

    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Advertiser ID"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "5485"}
                          ],
                          "curators" : {
                            "rowcount" : {
                              "config" : {
                                "isFactDriven": true
                              }
                            }
                          }
                        }""".stripMargin
    // request from RowCountCurator has queryType = RowCountQuery
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString)).copy(queryType = RowCountQuery)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    println(query.asString)

    withDruidQueryExecutor("http://localhost:6667/mock/groupby_rowcount"){
      executor =>
        val rowList = new CompleteRowList(query)
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(!result.rowList.isEmpty)

        var str: String = ""
        var count: Int = 0
        result.rowList.foreach {
          row => {
            count += 1
            str = str + s"$row"
          }
        }

        assert(count == 1)
        val expected = "Row(Map(TOTALROWS -> 0),ArrayBuffer(100))"

        str should equal(expected)(after being whiteSpaceNormalised)

        result.rowList.foreach { row =>
          val map = row.aliasMap
          for ((key, value) <- map) {
            assert(row.getValue(key) != null)
          }
        }
    }
  }

  test("successfully drop rows that more than specific maxrows") {

    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Advertiser ID"},
                            {"field": "Campaign Name"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Campaign Name", "operator": "in", "values": ["test1", "test2", "test3"]}
                          ],
                          "paginationStartIndex":1,
                          "rowsPerPage":1
                        }""".stripMargin

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    // hasNonFKDimFilters = true, limit = rowsPerPage * 2 + paginationStartIndex
    assert(query.asString.contains(""""limit":3"""))

    // should drop first 1 and last 1 row from DruidExecutor
    withDruidQueryExecutor("http://localhost:6667/mock/droprowtest"){
      executor =>
        val rowList = new CompleteRowList(query)
        val result = executor.execute(query, rowList, QueryAttributes.empty)
        assert(!result.rowList.isEmpty)
        var str: String = ""
        var count: Int = 0
        result.rowList.foreach {
          row => {
            count += 1

            str = str + s"$row"
          }
        }

        assert(count == 1)

        val expected = "Row(Map(Advertiser ID -> 0, Campaign Name -> 1, Impressions -> 2),ArrayBuffer(12345, test2, 17))"
        str should equal(expected)(after being whiteSpaceNormalised)

        result.rowList.foreach { row =>
          val map = row.aliasMap
          for ((key, value) <- map) {
            assert(row.getValue(key) != null)
          }
        }
    }
  }
}
