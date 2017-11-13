// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.executor.druid

import java.net.InetSocketAddress

import com.metamx.http.client.response.ClientResponse
import com.yahoo.maha.core.CoreSchema.{AdvertiserSchema, InternalSchema, ResellerSchema}
import com.yahoo.maha.core.DruidDerivedFunction.{DECODE_DIM, GET_INTERVAL_DATE}
import com.yahoo.maha.core.DruidPostResultFunction.{POST_RESULT_DECODE, START_OF_THE_MONTH, START_OF_THE_WEEK}
import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.lookup.LongRangeLookup
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.query.druid.{DruidQuery, DruidQueryGenerator, SyncDruidQueryOptimizer}
import com.yahoo.maha.core.query.oracle.OracleQueryGenerator
import com.yahoo.maha.core.registry.RegistryBuilder
import com.yahoo.maha.core.request.{ReportingRequest, SyncRequest}
import com.yahoo.maha.executor.MockOracleQueryExecutor
import org.apache.http.HttpResponse
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.http4s.server.blaze.BlazeBuilder
import org.json4s.JsonAST._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}


/**
 * Created by hiral on 2/2/16.
 */
class DruidQueryExecutorTest extends FunSuite with Matchers with BeforeAndAfterAll with BaseQueryGeneratorTest with SharedDimSchema with TestWebService {
  var server: org.http4s.server.Server = null
  override def beforeAll() : Unit = {
    super.beforeAll()
    val builder = BlazeBuilder.mountService(service,"/mock").bindSocketAddress(new InetSocketAddress("localhost",6667) )
    server = builder.run
    info("Started blaze server")
  }

  override def afterAll{
    info("Stopping blaze server")
    server.shutdown
  }

  override protected[this] def registerFacts(forcedFilters: Set[ForcedFilter], registryBuilder: RegistryBuilder): Unit = {
    registryBuilder.register(pubfact(forcedFilters))
    registryBuilder.register(pubfact2())
    registryBuilder.register(pubfact3())
  }

  val siteNameMap = Map(
    1  ->  "SITE_1"
    ,     2  ->  "SITE_2"
    ,    3  ->  "SITE_3"
    ,    4  ->  "SITE_4"
    ,    5  ->  "SITE_5"
    ,    6  ->  "SITE_6"
    ,    7  ->  "SITE_7"
    ,    8  ->  "SITE_8"
    ,    9  ->  "SITE_9"
    ,    10  ->  "SITE_10"
    ,    11  ->  "SITE_11"
    ,    12  ->  "SITE_12"
    ,    13  ->  "SITE_13"
    ,    14  ->  "SITE_14"
    ,    15  ->  "SITE_15"
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
          , DimCol("bidding_strategy", StrType(32),annotations = Set(EscapingRequired))
          , DimCol("install_type", IntType(10))
          , DimCol("campaign_objective",StrType(40))
          , DimCol("publisher_id", IntType(10), annotations = Set(ForeignKey("publishers")))
          , DimCol("internal_bucket_id", StrType())
          , DimCol("winss", StrType())
          , DimCol("os", StrType())
          , DimCol("gender",StrType())
          , DimCol("traffic_type",StrType())
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
          , FactCol("conversions_probability", DecType(0,"0.0"))
          , FactCol("ecpa_goal_usd", DecType(0,"0.0"))
          , FactCol("rank_score", DecType(0,"0.0"))
          , FactCol("alpha", DecType(0,"0.0"))
          , FactCol("mappi_boost", DecType(0,"0.0"))
          , FactCol("marketplace_floor", DecType(0,"0.0"))
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
          PubCol("winss","Winss", InNotInEquality),
          PubCol("os","Os", InNotInEquality),
          PubCol("gender","Gender", InNotInEquality),
          PubCol("traffic_type","Traffic Type", InNotInEquality)
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
          PublicFactCol("Effective Bid","Effective Bid", Set.empty),
          PublicFactCol("Efficiency","Efficiency", Set.empty),
          PublicFactCol("Total in App Conv","Total in App Conv", Set.empty),
          PublicFactCol("Pi Efficiency","Pi Efficiency", Set.empty)

        )
        , Set()
        , Map((SyncRequest, HourlyGrain) -> 14,(SyncRequest,DailyGrain)->31)
        , Map((SyncRequest, HourlyGrain) -> 14,(SyncRequest,DailyGrain)->31)
        , dimRevision = 1
      )
  }

  private[this] def pubfact(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import DruidExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "fact_s_term", DailyGrain, DruidEngine, Set(AdvertiserSchema),
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
          , DruidFuncDimCol("device_type", StrType(), DECODE_DIM("{device_id}",  "1", "'Desktop'", "'SmartPhone'"))
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
          , FactCol("weighted_bid_usd", DecType(8, 2, "0.0"))
          , FactCol("bid_mod_impressions", IntType(10, 0))
          , DruidDerFactCol("Average Bid", DecType(8, 2, "0.0"), "{weighted_bid_usd}" /- "{impresssions}")
          , DruidDerFactCol("Average CPC", DecType(), "{spend}" / "{clicks}")
          , DruidDerFactCol("CTR", DecType(), "{clicks}" /- "{impresssions}")
          , DruidDerFactCol("Bid Mod", DecType(8, 5, "0.0"), "{weighted_bid_modifier}" /-"{bid_mod_impressions}")
          , DruidDerFactCol("derived_avg_pos", DecType(8, 2, "0.0", "0.1", "500"), "{avg_pos_times_impressions}" /- "{impresssions}")
          , DruidDerFactCol("Bid Modifier", DecType(8, 5, "0.0"), "{weighted_bid_modifier}" /-"{bid_mod_impressions}")
          , DruidPostResultDerivedFactCol("Modified Bid", DecType(8, 5, "0.0"), "{Bid Modifier}" * "{Average Bid}", postResultFunction =POST_RESULT_DECODE("{Bid Mod}", "0", "{Average Bid}"))
          , DruidPostResultDerivedFactCol("Impression Share", StrType(), "{impresssions}" /- "{sov_impresssions}", postResultFunction = POST_RESULT_DECODE("{show_sov_flag}", "0", "N/A"))
        ),
        annotations = Set()
      )
    }
      .toPublicFact("k_stats",
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

    val tableOne  = {
      ColumnContext.withColumnContext {
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
            )
          )
      }
    }

    val tableTwo  = {
      ColumnContext.withColumnContext {
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
          PublicFactCol("spend", "Spend", Set.empty)
        ), Set(EqualityFilter("Test Flag", "0", isForceFilter = true)),  getMaxDaysWindow, getMaxDaysLookBack
      )
  }


  private[this] def getDruidQueryExecutor(url: String) : DruidQueryExecutor = {
    new DruidQueryExecutor(new DruidQueryExecutorConfig(50,500,5000,5000, 5000,"config",url,None,3000,3000,3000,3000,
      true, 500, 3), new NoopExecutionLifecycleListener, ResultSetTransformers.DEFAULT_TRANSFORMS)
  }

  private[this] def getDruidQueryGenerator() : DruidQueryGenerator = {
    new DruidQueryGenerator(new SyncDruidQueryOptimizer(), 40000)
  }

  test("success case for TimeSeries query"){
    val jsonString = s"""{
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    val executor = getDruidQueryExecutor("http://localhost:6667/mock/timeseries")
    val rowList= new CompleteRowList(query)
    val result=  executor.execute(query,rowList, QueryAttributes.empty)
    assert(!result._1.isEmpty)
    result._1.foreach{
      row=> println(s"TimeSeries: row : $row")
    }
    result._1.foreach{row =>
      val map = row.aliasMap
      for((key,value)<-map) {
        assert(row.getValue(key) != null)
      }
    }
  }

  test("Test TimeSeries query with response with missing result field"){
    val jsonString = s"""{
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    val executor = getDruidQueryExecutor("http://localhost:6667/mock/faultytimeseries")
    val rowList= new CompleteRowList(query)
    val thrown = intercept[UnsupportedOperationException]{
      executor.execute(query,rowList, QueryAttributes.empty)
    }
    assert(thrown.getMessage.contains("Unexpected field in timeseries json response"))
  }



  test("success case for TopN query"){
    val jsonString = s"""{
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    val executor = getDruidQueryExecutor("http://localhost:6667/mock/topn")
    val rowList= new CompleteRowList(query)
    val result=  executor.execute(query, rowList, QueryAttributes.empty)
    assert(!result._1.isEmpty)
    result._1.foreach{
      row=> println(s"TopN :=> row : $row")
    }
    result._1.foreach{row =>
      val map = row.aliasMap
      for((key,value)<-map) {
        assert(row.getValue(key) != null)
      }
    }
  }


  test("TopN query with invalid response"){
    val jsonString = s"""{
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    val executor = getDruidQueryExecutor("http://localhost:6667/mock/timeseries")
    val rowList= new CompleteRowList(query)
    intercept[Exception]{  executor.execute(query, rowList, QueryAttributes.empty)}

  }

  test("Test Response code other than 200"){
    val jsonString = s"""{
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    val executor = getDruidQueryExecutor("http://localhost:6667/mock/faultyn")
    val rowList= new CompleteRowList(query)
    val thrown = intercept[IllegalArgumentException]{executor.execute(query, rowList, QueryAttributes.empty)}
    assert(thrown.getMessage().contains("received status code from druid is"))
  }

  test("success case for groupby query execution") {

    val jsonString =  s"""{
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    println(query.asString)
    val executor = getDruidQueryExecutor("http://localhost:6667/mock/groupby")
    val rowList= new CompleteRowList(query)
    val result=  executor.execute(query, rowList, QueryAttributes.empty)
    assert(!result._1.isEmpty)

    var str: String = ""
    var count: Int = 0
    result._1.foreach{
      row => {
        count += 1
        println(s"Groupby => row : $row")
        str= str+s"$row"
      }
    }

    assert(count==2)
    val expected = "Row(Map(Pricing Type -> 4, Impression Share -> 10, Keyword ID -> 1, Average Bid -> 5, Max Bid -> 2, Day -> 0, Advertiser Status -> 9, Impressions -> 7, Min Bid -> 3, Average Position -> 6, Conversions -> 8),ArrayBuffer(2012-01-01, 10, 163, 184, 11, 9, 205, 175, 15, ON, N/A))Row(Map(Pricing Type -> 4, Impression Share -> 10, Keyword ID -> 1, Average Bid -> 5, Max Bid -> 2, Day -> 0, Advertiser Status -> 9, Impressions -> 7, Min Bid -> 3, Average Position -> 6, Conversions -> 8),ArrayBuffer(2012-01-01, 14, 16, 18, 13, 15, 20, 17, 2, ON, 0.0123))"
    println(s"Actual: $str expected: ${expected}")
    str should equal (expected) (after being whiteSpaceNormalised)

    result._1.foreach{row =>
      val map = row.aliasMap
      for((key,value)<-map) {
        assert(row.getValue(key)!=null)
      }
    }
  }

  test("success case for Bid Modifier and Modified Bid post results") {

    val jsonString =  s"""{
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    println(query.asString)
    val executor = getDruidQueryExecutor("http://localhost:6667/mock/groupbybidmod")
    val rowList= new CompleteRowList(query)
    val result=  executor.execute(query, rowList, QueryAttributes.empty)
    assert(!result._1.isEmpty)

    var str: String = ""
    var count: Int = 0
    result._1.foreach{
      row => {
        count += 1
        println(s"Groupby => row : $row")
        str= str+s"$row"
      }
    }

    assert(count==4)
    val expected = "Row(Map(Bid Modifier -> 5, Average Bid -> 4, Day -> 0, Modified Bid -> 6, Device Type -> 7, Impressions -> 3, Advertiser ID -> 1, Spend -> 2),ArrayBuffer(2017-08-07, 5430, 5793.1630783081, 2884485, 0.3, 9, 2.74208, 'Desktop'))Row(Map(Bid Modifier -> 5, Average Bid -> 4, Day -> 0, Modified Bid -> 6, Device Type -> 7, Impressions -> 3, Advertiser ID -> 1, Spend -> 2),ArrayBuffer(2017-08-08, 5430, 5237.0206726193, 2701909, 0.3, 8.99726, 2.70132, 'Desktop'))Row(Map(Bid Modifier -> 5, Average Bid -> 4, Day -> 0, Modified Bid -> 6, Device Type -> 7, Impressions -> 3, Advertiser ID -> 1, Spend -> 2),ArrayBuffer(2017-08-07, 5430, 1.4580000341, 2250, 0.3, 0, 0.3, 'SmartPhone'))Row(Map(Bid Modifier -> 5, Average Bid -> 4, Day -> 0, Modified Bid -> 6, Device Type -> 7, Impressions -> 3, Advertiser ID -> 1, Spend -> 2),ArrayBuffer(2017-08-08, 5430, 1.0800000392, 2138, 0.3, 0.2, 0.06022, 'SmartPhone'))"
    println(s"Actual:\n $str\n expected:\n ${expected}")
    str should equal (expected) (after being whiteSpaceNormalised)

    result._1.foreach{row =>
      val map = row.aliasMap
      for((key,value)<-map) {
        assert(row.getValue(key)!=null)
      }
    }
  }

  test("success case for groupby query execution with Start of the week field") {

    val jsonString =  s"""{
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    println(query.asString)
    val executor = getDruidQueryExecutor("http://localhost:6667/mock/groupby_start_of_week")
    val rowList= new CompleteRowList(query)
    val result=  executor.execute(query, rowList, QueryAttributes.empty)
    assert(!result._1.isEmpty)

    var str: String = ""
    var count: Int = 0
    result._1.foreach{
      row => {
        count += 1
        println(s"Groupby => row : $row")
        str= str+s"$row"
      }
    }

    assert(count==2)
    val expected = "Row(Map(Pricing Type -> 4, Impression Share -> 9, Keyword ID -> 1, Average Bid -> 5, Max Bid -> 2, Impressions -> 7, Week -> 0, Min Bid -> 3, Average Position -> 6, Conversions -> 8),ArrayBuffer(2017-06-26, 10, 163, 184, 11, 9, 205, 175, 15, N/A))Row(Map(Pricing Type -> 4, Impression Share -> 9, Keyword ID -> 1, Average Bid -> 5, Max Bid -> 2, Impressions -> 7, Week -> 0, Min Bid -> 3, Average Position -> 6, Conversions -> 8),ArrayBuffer(2017-07-10, 14, 16, 18, 13, 15, 20, 17, 2, 0.0123))"
    println(s"Actual: $str expected: ${expected}")
    str should equal (expected) (after being whiteSpaceNormalised)

    result._1.foreach{row =>
      val map = row.aliasMap
      for((key,value)<-map) {
        assert(row.getValue(key)!=null)
      }
    }
  }

  test("success case for groupby query execution with Start of the month field") {

    val jsonString =  s"""{
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    println(query.asString)
    val executor = getDruidQueryExecutor("http://localhost:6667/mock/groupby_start_of_month")
    val rowList= new CompleteRowList(query)
    val result=  executor.execute(query, rowList, QueryAttributes.empty)
    assert(!result._1.isEmpty)

    var str: String = ""
    var count: Int = 0
    result._1.foreach{
      row => {
        count += 1
        println(s"Groupby => row : $row")
        str= str+s"$row"
      }
    }

    assert(count==2)
    val expected = "Row(Map(Month -> 0, Pricing Type -> 4, Impression Share -> 9, Keyword ID -> 1, Average Bid -> 5, Max Bid -> 2, Impressions -> 7, Min Bid -> 3, Average Position -> 6, Conversions -> 8),ArrayBuffer(2017-06-01, 10, 163, 184, 11, 9, 205, 175, 15, N/A))Row(Map(Month -> 0, Pricing Type -> 4, Impression Share -> 9, Keyword ID -> 1, Average Bid -> 5, Max Bid -> 2, Impressions -> 7, Min Bid -> 3, Average Position -> 6, Conversions -> 8),ArrayBuffer(2017-07-01, 14, 16, 18, 13, 15, 20, 17, 2, 0.0123))"
    println(s"Actual: $str expected: ${expected}")
    str should equal (expected) (after being whiteSpaceNormalised)

    result._1.foreach{row =>
      val map = row.aliasMap
      for((key,value)<-map) {
        assert(row.getValue(key)!=null)
      }
    }
  }

  test("success case for groupby query execution with startIndex < 0") {

    val jsonString =  s"""{
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    val executor = getDruidQueryExecutor("http://localhost:6667/mock/groupby")
    val rowList= new CompleteRowList(query)
    val result=  executor.execute(query, rowList, QueryAttributes.empty)
    assert(!result._1.isEmpty)

    var str: String = ""
    var count: Int = 0
    result._1.foreach{
      row => {
        count += 1
        println(s"Groupby => row : $row")
        str= str+s"$row"
      }
    }

    assert(count==2)
    val expected = "Row(Map(Pricing Type -> 4, Keyword ID -> 1, Average Bid -> 5, Max Bid -> 2, Day -> 0, Impressions -> 7, Min Bid -> 3, Average Position -> 6),ArrayBuffer(2012-01-01, 10, 163, 184, 11, 9, 205, 175))Row(Map(Pricing Type -> 4, Keyword ID -> 1, Average Bid -> 5, Max Bid -> 2, Day -> 0, Impressions -> 7, Min Bid -> 3, Average Position -> 6),ArrayBuffer(2012-01-01, 14, 16, 18, 13, 15, 20, 17))"
    str should equal(expected) (after being whiteSpaceNormalised)

    result._1.foreach{row =>
      val map = row.aliasMap
      for((key,value)<-map) {
        assert(row.getValue(key)!=null)
      }
    }
  }

  test("success case for groupby query execution with startIndex = 1") {

    val jsonString =  s"""{
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    val executor = getDruidQueryExecutor("http://localhost:6667/mock/3rowsgroupby")
    val rowList= new CompleteRowList(query)
    val result=  executor.execute(query, rowList, QueryAttributes.empty)
    assert(!result._1.isEmpty)
    println(query.asString)

    var str: String = ""
    var count: Int = 0
    result._1.foreach{
      row => {
        count += 1
        println(s"Groupby => row : $row")
        str= str+s"$row"
      }
    }
    println(str)

    assert(count==2)
    val expected = "Row(Map(Pricing Type -> 5, Keyword ID -> 1, Average Bid -> 6, Max Bid -> 3, Day -> 0, Impressions -> 8, Min Bid -> 4, Average Position -> 7, Country Name -> 2),ArrayBuffer(2016-01-01, 2, US, 16, 18, 13, 15, 20, 17))Row(Map(Pricing Type -> 5, Keyword ID -> 1, Average Bid -> 6, Max Bid -> 3, Day -> 0, Impressions -> 8, Min Bid -> 4, Average Position -> 7, Country Name -> 2),ArrayBuffer(2016-01-01, 3, US, 160123456, 18, 13, 10, 20.12, 127))"
    str should equal(expected) (after being whiteSpaceNormalised)

    result._1.foreach{row =>
      val map = row.aliasMap
      for((key,value)<-map) {
        assert(row.getValue(key)!=null)
      }
    }
  }

  test("groupby query execution with invalid url") {

    val jsonString =  s"""{
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    val executor = getDruidQueryExecutor("http://localhost:6667/mock/topn")
    val rowList= new CompleteRowList(query)
    intercept[Exception]{  executor.execute(query, rowList, QueryAttributes.empty)}
  }


  test("success case for Partial TimeSeries query"){
    val jsonString = s"""{
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    val executor = getDruidQueryExecutor("http://localhost:6667/mock/timeseries")
    val rowList : DimDrivenFactOrderedPartialRowList = new DimDrivenFactOrderedPartialRowList("Impressions", query)
    val row = rowList.newRow
    row.addValue("Impressions",java.lang.Integer.valueOf(15))
    row.addValue("Day",null)
    rowList.addRow(row)
    rowList.foreach{
      row=> println(s"Partial TimeSeries => row before : $row")
    }
    val result=  executor.execute(query, rowList, QueryAttributes.empty)
    assert(!result._1.isEmpty)
    result._1.foreach{
      row=> println(s"Partial TimeSeries => row after: $row")
    }
    result._1.foreach{row =>
      val map = row.aliasMap
      for((key,value)<-map) {
        assert(row.getValue(key) != null)
      }
    }
  }


  test("success case for partial groupby query execution with no non fk dim filters") {

    val jsonString =  s"""{
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    altQueryGeneratorRegistry.register(OracleEngine, new OracleQueryGenerator(DefaultPartitionColumnRenderer))
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    //val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    val executorContext = new QueryExecutorContext
    executorContext.register(getDruidQueryExecutor("http://localhost:6667/mock/groupby"))
    executorContext.register(new MockOracleQueryExecutor({
      rowList =>
        rowList match {
          case irl: IndexedRowList =>
            println(s"alias=${irl.indexAlias}")
            val rowSet = irl.getRowByIndex("10")
            rowSet.map(_.addValue("Keyword Value", "ten"))
            val rowSet2 = irl.getRowByIndex("14")
              rowSet2.map(_.addValue("Keyword Value", "fourteen"))
          case any => throw new IllegalArgumentException("unexptected row list type")
        }

    }))
    val result=  queryPipelineTry.toOption.get.execute(executorContext, QueryAttributes.empty).toOption.get
    assert(!result._1.isEmpty)
    result._1.foreach{
      row=> println(s"partial groupby query => Result row after: $row")
    }
    result._1.foreach{row =>
      val map = row.aliasMap
      for((key,value)<-map) {
        assert(row.getValue(key)!=null)
      }
    }
  }

  test("success case for partial groupby query execution with non fk dim filters") {

    val jsonString =  s"""{
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    altQueryGeneratorRegistry.register(OracleEngine, new OracleQueryGenerator(DefaultPartitionColumnRenderer))
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val executorContext = new QueryExecutorContext
    executorContext.register(getDruidQueryExecutor("http://localhost:6667/mock/groupby"))
    executorContext.register(new MockOracleQueryExecutor({
      rowList =>
        rowList match {
          case irl: IndexedRowList =>
            println(s"alias=${irl.indexAlias}")
            for {
              row <- irl.getRowByIndex("10")
            } {
              row.addValue("Keyword Value", "ten")
              row.addValue("Advertiser Name", "advertiser-ten")
              row.addValue("Campaign Name", "campaign-ten")
            }
            for {
              row <- irl.getRowByIndex("14")
            } {
              row.addValue("Keyword Value", "fourteen")
              row.addValue("Advertiser Name", "advertiser-fourteen")
              row.addValue("Campaign Name", "campaign-fourteen")
            }
          case any => throw new IllegalArgumentException("unexptected row list type")

        }

    }))
    val resultTry =  queryPipelineTry.toOption.get.execute(executorContext, QueryAttributes.empty)
    assert(resultTry.isSuccess, resultTry.errorMessage("Fail to execute pipeline"))

    val result =  resultTry.toOption.get
    assert(!result._1.isEmpty)
    result._1.foreach{
      row=> println(s"partial groupby query => Result row after: $row")
    }
    result._1.foreach{row =>
      val map = row.aliasMap
      for((key,value)<-map) {
        assert(row.getValue(key)!=null)
      }
    }
    val sql = queryPipelineTry.toOption.get.queryChain.subsequentQueryList.head.asString
    println(sql)
    assert(sql contains "ROWNUM")
    assert(!sql.contains("TotalRows"))
  }

  test("success case for groupby fact driven query execution") {

    val jsonString =  s"""{
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    altQueryGeneratorRegistry.register(OracleEngine, new OracleQueryGenerator(DefaultPartitionColumnRenderer))
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val executorContext = new QueryExecutorContext
    executorContext.register(getDruidQueryExecutor("http://localhost:6667/mock/groupby"))
    executorContext.register(new MockOracleQueryExecutor({
      rowList =>
        rowList match {
          case irl: IndexedRowList =>
            println(s"alias=${irl.indexAlias}")
            for {
              row <- irl.getRowByIndex("10")
            } {
              row.addValue("Keyword Value", "ten")
            }
            for {

              row <- irl.getRowByIndex("14")
            }
            {
              row.addValue("Keyword Value", "fourteen")
            }
            val newRow = irl.newRow
            newRow.addValue(irl.indexAlias, "11")
            newRow.addValue("Keyword Value", "eleven")
            irl.updateRow(newRow)
          case any => throw new IllegalArgumentException("unexptected row list type")

        }

    }))
    val resultTry =  queryPipelineTry.toOption.get.execute(executorContext, QueryAttributes.empty)
    assert(resultTry.isSuccess, resultTry.errorMessage("Fail to execute pipeline"))

    val result =  resultTry.toOption.get
    assert(!result._1.isEmpty)
    var rowCount = 0
    result._1.foreach( row => rowCount = rowCount + 1)
    assert(rowCount == 2)
    result._1.foreach{
      row=> println(s"partial groupby query => Result row after: $row")
    }
    result._1.foreach{row =>
      val map = row.aliasMap
      for((key,value)<-map) {
        assert(row.getValue(key)!=null)
      }
    }
  }

  test("success case for TopN query with partial result set"){
    val jsonString = s"""{
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    val executor = getDruidQueryExecutor("http://localhost:6667/mock/topn")
    val rowList : DimDrivenFactOrderedPartialRowList = new DimDrivenFactOrderedPartialRowList("Keyword ID", query)
    val row1 = rowList.newRow
    row1.addValue("Keyword ID",14)
    row1.addValue("Impressions",null)
    rowList.addRow(row1)
    val row2 = rowList.newRow
    row2.addValue("Keyword ID",13)
    row2.addValue("Impressions",null)
    rowList.addRow(row2)
    val result=  executor.execute(query, rowList, QueryAttributes.empty)
    assert(!result._1.isEmpty)
    result._1.foreach{
      row=> println(s"TopN query with partial result=> row : $row")
    }
    result._1.foreach{row =>
      val map = row.aliasMap
      for((key,value)<-map) {
        assert(row.getValue(key) != null)
      }
    }
  }

  test("Handle null values from druid gracefully"){
    val jsonString = s"""{
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    val executor = getDruidQueryExecutor("http://localhost:6667/mock/topnWithNull")
    val rowList : DimDrivenFactOrderedPartialRowList = new DimDrivenFactOrderedPartialRowList("Keyword ID", query)
    val row1 = rowList.newRow
    row1.addValue("Keyword ID",14)
    row1.addValue("Impressions",null)
    rowList.addRow(row1)
    val row2 = rowList.newRow
    row2.addValue("Keyword ID",13)
    row2.addValue("Impressions",null)
    rowList.addRow(row2)
    val result=  executor.execute(query, rowList, QueryAttributes.empty)
    assert(result._1.isEmpty)
  }


  test("success case for TopN query with partial result set: MultiEngine")
  {
    val jsonString = s"""{
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    altQueryGeneratorRegistry.register(OracleEngine, new OracleQueryGenerator(DefaultPartitionColumnRenderer))
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    val druidExecutor =  getDruidQueryExecutor("http://localhost:6667/mock/topn")
    val oracleExecutor = new MockOracleQueryExecutor(
    { rl =>
      rl.foreach(r => println(s"Inside mock: $r"))
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

    result.toOption.get._1.foreach(println)
    //var result = oracleExecutor.execute(oraclQuery.head, rowList, QueryAttributes.empty)
    val oracleQuery = queryPipelineTry.toOption.get.queryChain.subsequentQueryList.head
    //require(result._1, "Failed to execute MultiEngine Query")
    println(oracleQuery.asString)
    val expected =
    """
       | (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT  *
       |      FROM (SELECT to_char(t0.id) "Keyword ID", t0.value "Keyword Value"
       |            FROM
       |                (SELECT  value, id, advertiser_id
       |            FROM targetingattribute
       |            WHERE (advertiser_id = 213) AND (id IN (14,13))
       |             ) t0
       |
       |
       |           )
       |            ) D )) UNION ALL (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  *
       |      FROM (SELECT to_char(t0.id) "Keyword ID", t0.value "Keyword Value"
       |            FROM
       |                (SELECT  value, id, advertiser_id
       |            FROM targetingattribute
       |            WHERE (advertiser_id = 213) AND (id NOT IN (14,13))
       |             ) t0
       |
       |
       |           )
       |            ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100)
       |  """.stripMargin

    oracleQuery.asString should equal (expected) (after being whiteSpaceNormalised)

  }

  test("success case for TopN query with partial result set: MultiEngine with 2 sorts")
  {
    val jsonString = s"""{
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    altQueryGeneratorRegistry.register(OracleEngine, new OracleQueryGenerator(DefaultPartitionColumnRenderer))
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    println(query.asString)
    val druidExecutor = getDruidQueryExecutor("http://localhost:6667/mock/druidPlusOraclegroupby")
    val oracleExecutor = new MockOracleQueryExecutor(
    { rl =>
      rl.foreach(r => println(s"Inside mock: $r"))
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

    result.toOption.get._1.foreach(println)
    val oracleQuery = queryPipelineTry.toOption.get.queryChain.subsequentQueryList.head
    println(oracleQuery.asString)
    val expected = s"""
                       | (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT  *
                       |      FROM (SELECT to_char(t0.id) "Keyword ID", t0.value "Keyword Value"
                       |            FROM
                       |                (SELECT  id, value, advertiser_id
                       |            FROM targetingattribute
                       |            WHERE (advertiser_id = 213) AND (id IN (14,13))
                       |            ORDER BY 1 ASC  ) t0
                       |
                       |
                       |           )
                       |            ) D )) UNION ALL (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  *
                       |      FROM (SELECT to_char(t0.id) "Keyword ID", t0.value "Keyword Value"
                       |            FROM
                       |                (SELECT  id, value, advertiser_id
                       |            FROM targetingattribute
                       |            WHERE (advertiser_id = 213) AND (id NOT IN (14,13))
                       |            ORDER BY 1 ASC  ) t0
                       |
                       |
                       |           )
                       |            ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100)  """
      .stripMargin

    oracleQuery.asString should equal (expected) (after being whiteSpaceNormalised)

  }

  test("Invalid field extraction failure"){
    val jsonString = s"""{
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    val executor = getDruidQueryExecutor("http://localhost:6667/mock/faultytopn")
    val rowList= new CompleteRowList(query)
    val thrown = intercept[UnsupportedOperationException]{  executor.execute(query, rowList, QueryAttributes.empty)}
    assert(thrown.getMessage.contains("unsupported field type"))
  }

  test("TopN query with partial result set and request code not 200"){
    val jsonString = s"""{
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    val executor = getDruidQueryExecutor("http://localhost:6667/mock/bh")
    val rowList : DimDrivenFactOrderedPartialRowList = new DimDrivenFactOrderedPartialRowList("Keyword ID", query)
    val row1 = rowList.newRow
    row1.addValue("Keyword ID",14)
    row1.addValue("Impressions",null)
    rowList.addRow(row1)
    val row2 = rowList.newRow
    row2.addValue("Keyword ID",13)
    row2.addValue("Impressions",null)
    rowList.addRow(row2)
    val thrown=  intercept[IllegalArgumentException] {
    executor.execute(query, rowList, QueryAttributes.empty)
    }
    assert(thrown.getMessage().contains("received status code from druid is"))
  }

  test("Test TimeSeries query with partial result set with missing result field"){
    val jsonString = s"""{
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    val executor = getDruidQueryExecutor("http://localhost:6667/mock/faultytimeseries")
    val rowList : DimDrivenFactOrderedPartialRowList = new DimDrivenFactOrderedPartialRowList("Impressions", query)
    val row = rowList.newRow
    row.addValue("Impressions",java.lang.Integer.valueOf(15))
    row.addValue("Day",null)
    val thrown = intercept[UnsupportedOperationException]{
      executor.execute(query, rowList, QueryAttributes.empty)
    }
    assert(thrown.getMessage.contains("Unexpected field in timeseries json response"))
  }


  test("testing extract field"){
   val bool =JBool(true)
   val boolVal = DruidQueryExecutor.extractField(bool)
    assert(true==boolVal)
    val string = JString("s")
    val stringVal = DruidQueryExecutor.extractField(string)
    assert("s"==stringVal)
    val dec =JDecimal(1.0)
    val decVal = DruidQueryExecutor.extractField(dec)
    assert(1.0==decVal)
    val int =JInt(1)
    val intVal = DruidQueryExecutor.extractField(int)
    assert(1==intVal)
    val double =JDouble(1112.23)
    val doubleVal = DruidQueryExecutor.extractField(double)
    assert(1112.23==doubleVal)
    val long =JLong(1112)
    val longVal = DruidQueryExecutor.extractField(long)
    assert(1112==longVal)
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
         |      }
         |   ],
         |   "filterExpressions": [
         |      {
         |         "field": "Advertiser ID",
         |         "operator": "=",
         |         "value": "1035663"
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

    val request: ReportingRequest = getReportingRequestSyncWithFactBias(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)

    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val druidExecutor = getDruidQueryExecutor("http://localhost:6667/mock/adjustmentStatsGroupBy")

    val queryExecContext: QueryExecutorContext = new QueryExecutorContext
    queryExecContext.register(druidExecutor)

    val result = queryPipelineTry.toOption.get.execute(queryExecContext)
    assert(result.isSuccess)

    val expectedSet = Set(
    "Row(Map(Is Adjustment -> 2, Day -> 1, Impressions -> 3, Advertiser ID -> 0, Spend -> 4),ArrayBuffer(184, 2012-01-01, N, 100, 15))"
    ,"Row(Map(Is Adjustment -> 2, Day -> 1, Impressions -> 3, Advertiser ID -> 0, Spend -> 4),ArrayBuffer(199, 2012-01-01, N, 100, 10))"
    ,"Row(Map(Is Adjustment -> 2, Day -> 1, Impressions -> 3, Advertiser ID -> 0, Spend -> 4),ArrayBuffer(184, 2012-01-01, Y, 100, 15))"
    ,"Row(Map(Is Adjustment -> 2, Day -> 1, Impressions -> 3, Advertiser ID -> 0, Spend -> 4),ArrayBuffer(199, 2012-01-01, Y, 100, 10))"
    )
    var count = 0
    result.get._1.foreach {
      row =>
        println(row)
        assert(expectedSet.contains(row.toString))
        count+=1
    }
    assert(expectedSet.size == count)
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

    val request: ReportingRequest = getReportingRequestSyncWithFactBias(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)

    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val druidExecutor = getDruidQueryExecutor("http://localhost:6667/mock/adjustmentStatsGroupBy")

    val queryExecContext: QueryExecutorContext = new QueryExecutorContext
    queryExecContext.register(druidExecutor)

    val queryList = queryPipelineTry.toOption.get.queryChain.asInstanceOf[MultiQuery].unionQueryList

    queryList.foreach {
      query=>
        println(query.asString)
    }

    val result = queryPipelineTry.toOption.get.execute(queryExecContext)
    assert(result.isSuccess)

    val expectedSet = Set(
      "Row(Map(Is Adjustment -> 2, Day -> 1, Impressions -> 3, Advertiser ID -> 0, Spend -> 4),ArrayBuffer(184, 2012-01-01, Y, 100, 15))"
      ,"Row(Map(Is Adjustment -> 2, Day -> 1, Impressions -> 3, Advertiser ID -> 0, Spend -> 4),ArrayBuffer(199, 2012-01-01, Y, 100, 10))"
    )
    var count = 0
    result.get._1.foreach {
      row =>
        println(row)
        assert(expectedSet.contains(row.toString))
        count+=1
    }
    assert(expectedSet.size == count)
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
    val request: ReportingRequest = getReportingRequestSync(jsonString, InternalSchema)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)

    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val druidExecutor = getDruidQueryExecutor("http://localhost:6667/mock/whiteGloveGroupBy")

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
    result.get._1.foreach {
      row =>
        assert(expectedSet.contains(row.toString))
        count+=1
    }
    assert(expectedSet.size == count)

  }

  //Test if there are nonempty uncovered intervals, meaning an IllegalStateException
  // should be thrown so that response can fallback to Oracle.
  test("Uncovered Interval response") {

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
    val request: ReportingRequest = getReportingRequestSync(jsonString, InternalSchema)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)

    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val druidExecutor = getDruidQueryExecutor("http://localhost:6667/mock/uncoveredNonempty")

    val queryExecContext: QueryExecutorContext = new QueryExecutorContext
    queryExecContext.register(druidExecutor)

    val result = queryPipelineTry.toOption.get.execute(queryExecContext)
    println(result.isSuccess)
    assert(result.isSuccess)

  }

}
