// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core.CoreSchema.AdvertiserSchema
import com.yahoo.maha.core.DruidDerivedFunction._
import com.yahoo.maha.core.DruidPostResultFunction.POST_RESULT_DECODE
import com.yahoo.maha.core.FilterOperation.{Equality, In, InBetweenEquality, InEquality}
import com.yahoo.maha.core.dimension.{DimCol, DruidFuncDimCol, PubCol}
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.query.druid.{DruidQuery, DruidQueryGenerator, SyncDruidQueryOptimizer}
import com.yahoo.maha.core.query.oracle.BaseOracleQueryGeneratorTest
import com.yahoo.maha.core.registry.RegistryBuilder
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.core.{ColumnContext, ColumnInfo, DailyGrain, DateType, DecType, DruidEngine, DruidExpression, EqualityFilter, EscapingRequired, FactColumnInfo, Filter, ForeignKey, IntType, RequestModel, StrType}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hiral on 1/25/16.
 */
class DerivedRowListTest extends BaseOracleQueryGeneratorTest with BaseRowListTest {

  private[this] def factBuilder(annotations: Set[FactAnnotation]): FactBuilder = {
    import DruidExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "fact1", DailyGrain, DruidEngine, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(ForeignKey("keyword")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DruidFuncDimCol("Derived Pricing Type", IntType(3), DECODE_DIM("{price_type}", "7", "6"))
          , DimCol("start_time", IntType())
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("stats_date", DateType("yyyyMMdd"), Some("statsDate"))
          , DimCol("engagement_type", StrType(3))
          , DruidFuncDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "w"))
          , DruidFuncDimCol("name", StrType(), LOOKUP("advertiser_lookup", "name"))
          , DruidFuncDimCol("My Date", DateType(), DRUID_TIME_FORMAT("YYYY-MM-dd"))
          , DimCol("show_sov_flag", IntType())

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
          , FactCol("Reblogs", IntType(), DruidFilteredRollup(EqualityFilter("engagement_type", "1", isForceFilter = true), "engagement_count", SumRollup))
          , DruidDerFactCol("Reblog Rate", DecType(), "{Reblogs}" /- "{impressions}" * "100")
          , DruidPostResultDerivedFactCol("impression_share", StrType(), "{impressions}" /- "{sov_impressions}", postResultFunction = POST_RESULT_DECODE("{show_sov_flag}", "0", "N/A"))
        ),
        annotations = annotations
      )
    }
  }
  private[this] def druid_pubfact(forcedFilters: Set[Filter] = Set.empty): PublicFact = {
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
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("Derived Pricing Type", "Derived Pricing Type", InEquality),
          PubCol("landing_page_url", "Destination URL", Set.empty),
          PubCol("Week", "Week", InBetweenEquality),
          PubCol("name", "Advertiser Name", Equality),
          PubCol("My Date", "My Date", Equality)
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
          PublicFactCol("CTR", "CTR", InBetweenEquality)
        ),
        Set(),
        getMaxDaysWindow, getMaxDaysLookBack, renderLocalTimeFilter = true
      )
  }

  def query : Query = {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Campaign ID"},
                            {"field": "Impressions"},
                            {"field": "Campaign Name"},
                            {"field": "Campaign Status"},
                            {"field": "CTR"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "213"},
                            {"field": "Campaign Name", "operator": "=", "value": "MegaCampaign"}
                          ],
                          "sortBy": [
                            {"field": "Campaign Name", "order": "Asc"}
                          ],
                          "paginationStartIndex":-1,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery]
  }

  def druidQuery : Query = {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Impressions"},
                            {"field": "Impression Share"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "213"}
                          ],
                          "sortBy": [],
                          "paginationStartIndex":-1,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registryBuilder = new RegistryBuilder
    registryBuilder.register(druid_pubfact(Set.empty))
    registerDims(registryBuilder)
    val registry = registryBuilder.build()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
  }

  test("successfully construct Derived row list") {

    val columns : IndexedSeq[ColumnInfo] = query.queryContext.requestModel.requestCols :+ FactColumnInfo("Impressions Prev") :+ FactColumnInfo("Impressions Pct Change")
    val aliasMap : Map[String, Int] = columns.map(_.alias).zipWithIndex.toMap

    val row = new Row(aliasMap, ArrayBuffer.fill[Any](aliasMap.size)(null))
    row.addValue("Campaign ID",java.lang.Integer.valueOf(1))
    row.addValue("Impressions",java.lang.Integer.valueOf(2))
    row.addValue("Impressions Prev",java.lang.Integer.valueOf(1))
    row.addValue("Impressions Pct Change",java.lang.Double.valueOf(100.0d))
    row.addValue("Campaign Name","name")
    row.addValue("Campaign Status","on")
    row.addValue("CTR", java.lang.Double.valueOf(2.11D))

    val derivedRowList = new DerivedRowList(columns)
    derivedRowList.addRow(row)

    assert(!derivedRowList.isEmpty)

    derivedRowList.foreach(r => assert(r.toString === "Row(Map(Campaign ID -> 0, Campaign Status -> 3, Impressions -> 1, Campaign Name -> 2, CTR -> 4, Impressions Prev -> 5, Impressions Pct Change -> 6),ArrayBuffer(1, 2, name, on, 2.11, 1, 100.0))"))
    derivedRowList.map(r => assert(r.toString === "Row(Map(Campaign ID -> 0, Campaign Status -> 3, Impressions -> 1, Campaign Name -> 2, CTR -> 4, Impressions Prev -> 5, Impressions Pct Change -> 6),ArrayBuffer(1, 2, name, on, 2.11, 1, 100.0))"))

  }

}
