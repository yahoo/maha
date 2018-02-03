// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import java.nio.charset.StandardCharsets

import com.yahoo.maha.core.CoreSchema.{AdvertiserSchema, ResellerSchema}
import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core.HiveExpression._
import com.yahoo.maha.core.bucketing._
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.error.{NoRelationWithPrimaryKeyError, UnknownFieldNameError}
import com.yahoo.maha.core.fact.{FactCol, _}
import com.yahoo.maha.core.query.{RightOuterJoin, LeftOuterJoin, InnerJoin}
import com.yahoo.maha.core.registry.{Registry, RegistryBuilder}
import com.yahoo.maha.core.request._
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.{FunSuite, Matchers}

/**
 * Created by jians on 10/23/15.
 */
class RequestModelTest extends FunSuite with Matchers {
  
  CoreSchema.register()

  private[this] val fromDate = DailyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC).minusDays(7))
  private[this] val toDate = DailyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC))
  private[this] val futureFromDate = DailyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC).plusDays(1))
  private[this] val futureToDate = DailyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC).plusDays(7))

  protected[this] def getMaxDaysWindow: Map[(RequestType, Grain), Int] = {
    val interval = DailyGrain.getDaysBetween(fromDate, toDate)
    val result = interval + 1
    Map(
      (SyncRequest, DailyGrain) -> result, (AsyncRequest, DailyGrain) -> result,
      (SyncRequest, HourlyGrain) -> result, (AsyncRequest, HourlyGrain) -> result,
      (SyncRequest, MinuteGrain) -> result, (AsyncRequest, MinuteGrain) -> result
    )
  }

  protected[this] def getMaxDaysLookBack: Map[(RequestType, Grain), Int] = {
    val daysBack = DailyGrain.getDaysFromNow(fromDate)
    val result = daysBack + 10
    Map(
      (SyncRequest, DailyGrain) -> result, (AsyncRequest, DailyGrain) -> result,
      (SyncRequest, HourlyGrain) -> result, (AsyncRequest, HourlyGrain) -> result,
      (SyncRequest, MinuteGrain) -> result, (AsyncRequest, MinuteGrain) -> result
    )
  }

  def getReportingRequestAsync(jsonString: String) = {
    val result = ReportingRequest.deserializeAsync(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    require(result.isSuccess, result)
    result.toOption.get
  }
  
  def getReportingRequestSync(jsonString: String) = {
    val result = ReportingRequest.deserializeSync(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    require(result.isSuccess, result)
    result.toOption.get
  }
  
  def getFactBuilder : FactBuilder = {
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      import com.yahoo.maha.core.BaseExpressionTest._
      Fact.newFact(
        "fact1", DailyGrain, HiveEngine, Set(AdvertiserSchema),
        Set(
          DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("product_ad_id", IntType(), annotations = Set(ForeignKey("productAd")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("device_id", IntType(8, (Map(1 -> "SmartPhone", 2 -> "Tablet", 3 -> "Desktop", -1 -> "UNKNOWN"), "UNKNOWN")))
          , DimCol("device_type", IntType(8, (Map(1 -> "SmartPhone", 2 -> "Tablet", 3 -> "Desktop", -1 -> "UNKNOWN"), "UNKNOWN")), alias = Option("device_id"))
          , DimCol("start_time", IntType())
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("stats_date", DateType())
          , DimCol("network_type", StrType(100, (Map("TEST_PUBLISHER" -> "Test Publisher"), "NONE")))
          , DimCol("ad_format_id", IntType(3, (Map(2 -> "Single image"), "Other")))
          , HiveDerDimCol("Ad Group Start Date Full", StrType(),TIMESTAMP_TO_FORMATTED_DATE("{start_time}", "YYYY-MM-dd HH:mm:ss"))

        ),
        Set(
          FactCol("impressions", IntType())
          , FactCol("clicks", IntType())
          , FactCol("spend", DecType(0, "0.0"))
        )
      )
    }
  }

  def getFactBuilder2 : FactBuilder = {
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      import com.yahoo.maha.core.BaseExpressionTest._
      Fact.newFact(
        "fact2", DailyGrain, HiveEngine, Set(AdvertiserSchema),
        Set(
          DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("keyword_id", IntType(), annotations = Set(ForeignKey("keyword")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("start_time", IntType())
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("stats_date", DateType())
          , HiveDerDimCol("Ad Group Start Date Full", StrType(),TIMESTAMP_TO_FORMATTED_DATE("{start_time}", "YYYY-MM-dd HH:mm:ss"))

        ),
        Set(
          FactCol("impressions", IntType())
          , FactCol("clicks", IntType())
        ), Set.empty, None, Map.empty, Set.empty, 0, 0, None, None,
       maxDaysWindow = Option(Map(SyncRequest -> 31, AsyncRequest -> 400)),
        Option(Map(SyncRequest -> 10, AsyncRequest -> 10))
      )

    }
  }

  def pubfact(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    getFactBuilder
      .toPublicFact("publicFact",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality),
          PubCol("ad_id", "Ad ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("product_ad_id", "Product Ad ID", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty),
          PubCol("network_type", "Network Type", InEqualityIsNotNullNotIn, restrictedSchemas = Set(AdvertiserSchema)),
          PubCol("ad_format_id", "Ad Format Name", Set.empty, restrictedSchemas = Set(ResellerSchema)),
          PubCol("Ad Group Start Date Full", "Ad Group Start Date Full", InEquality),
          PubCol("device_type", "Device Type", In, incompatibleColumns = Set("Device ID")),
          PubCol("device_id", "Device ID", In, incompatibleColumns = Set("Device Type"))
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", In),
          PublicFactCol("spend", "Spend", InBetweenEquality, restrictedSchemas = Set(ResellerSchema))
        ),
        forcedFilters,
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }

  def pubfactRev2(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    getFactBuilder
      .toPublicFact("publicFactRev2",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality),
          PubCol("ad_id", "Ad ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("product_ad_id", "Product Ad ID", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty),
          PubCol("Ad Group Start Date Full", "Ad Group Start Date Full", InEquality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", In)
        ),
        forcedFilters,
        getMaxDaysWindow, getMaxDaysLookBack, revision = 2
      )
  }

  def pubfact2(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    getFactBuilder2
      .toPublicFact("publicFact2",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality),
          PubCol("ad_id", "Ad ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("keyword_id", "Keyword ID", InEquality, required=true),
          PubCol("landing_page_url", "Destination URL", Set.empty, dependsOnColumns = Set("Ad ID", "Keyword ID")),
          PubCol("Ad Group Start Date Full", "Ad Group Start Date Full", InEquality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", In)
        ),
        forcedFilters,
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }

  def pubfact3(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    getFactBuilder2
      .toPublicFact("publicFact3",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality, filteringRequired = true),
          PubCol("ad_id", "Ad ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("keyword_id", "Keyword ID", InEquality, required=true),
          PubCol("landing_page_url", "Destination URL", Set.empty, dependsOnColumns = Set("Ad ID", "Keyword ID")),
          PubCol("Ad Group Start Date Full", "Ad Group Start Date Full", InEquality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", In)
        ),
        forcedFilters,
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }

  def keyword_dim : PublicDimension = {
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      Dimension.newDimension("keyword_dim", HiveEngine, LevelFive, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
          , DimCol("status", StrType())
        )
        , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
      ).toPublicDimension("keyword",
        "keyword",
          Set(
            PubCol("id", "Keyword ID", InEquality)
            , PubCol("advertiser_id", "Advertiser ID", InEquality)
            , PubCol("ad_group_id", "Ad Group ID", InEquality)
            , PubCol("ad_id", "Ad ID", InEquality)
            , PubCol("status", "Keyword Status", InEquality)
          ), highCardinalityFilters = Set(NotInFilter("Keyword Status", List("DELETED")), InFilter("Keyword Status", List("ON")))
        )
    }
  }

  def site_dim: PublicDimension = {
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      Dimension.newDimension("site_dim", HiveEngine, LevelOne, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("status", StrType())
        )
        , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
      ).toPublicDimension("site",
        "site",
          Set(
            PubCol("id", "Site ID", Equality)
            , PubCol("status", "Site Status", Equality)
          ), highCardinalityFilters = Set(NotInFilter("Site Status", List("DELETED")))
        )
    }
  }

  def ad_dim: PublicDimension = {
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      Dimension.newDimension("ad_dim", HiveEngine, LevelFour, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("status", StrType())
        )
        , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
      ).toPublicDimension("ad","ad",
        Set(
          PubCol("id", "Ad ID", InEquality)
          , PubCol("advertiser_id", "Advertiser ID", InEquality)
          , PubCol("campaign_id", "Campaign ID", InEquality)
          , PubCol("ad_group_id", "Ad Group ID", InEquality)
          , PubCol("status", "Ad Status", InEquality)
        ), highCardinalityFilters = Set(NotInFilter("Ad Status", List("DELETED")))
      )
    }
  }

  def ad_group_dim: PublicDimension = {
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      Dimension.newDimension("ad_group_dim", HiveEngine, LevelThree, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("status", StrType())
          , DimCol("name", StrType())
        )
        , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
      ).toPublicDimension("ad_group",
        "ad_group",
          Set(
            PubCol("id", "Ad Group ID", InEquality)
            , PubCol("advertiser_id", "Advertiser ID", InEquality)
            , PubCol("campaign_id", "Campaign ID", InEquality)
            , PubCol("status", "Ad Group Status", InEquality)
            , PubCol("name", "Ad Group Name", InEquality)
          ), highCardinalityFilters = Set(NotInFilter("Ad Group Status", List("DELETED")))
        )
    }
  }

  def campaign_dim: PublicDimension = {
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      Dimension.newDimension("campaign_dim", HiveEngine, LevelTwo, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("status", StrType())
          , DimCol("name", StrType())
        )
        , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
      ).toPublicDimension("campaign",
        "campaign",
        Set(
          PubCol("id", "Campaign ID", InEquality)
          , PubCol("advertiser_id", "Advertiser ID", InEquality)
          , PubCol("status", "Campaign Status", InEquality)
          , PubCol("name", "Campaign Name", InEqualityLike)
        ), highCardinalityFilters = Set(NotInFilter("Campaign Status", List("DELETED")))
      )
    }
  }

  def advertiser_dim: PublicDimension = {
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      Dimension.newDimension("advertiser_dim", HiveEngine, LevelOne, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("status", StrType())
          , DimCol("name", StrType())
        )
        , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        , schemaColMap = Map(AdvertiserSchema -> "id")
      ).toPublicDimension("advertiser",
        "advertiser",
        Set(
          PubCol("id", "Advertiser ID", InEquality)
          , PubCol("status", "Advertiser Status", InEquality)
          , PubCol("name", "Advertiser Name", InEquality)
        ), highCardinalityFilters = Set(NotInFilter("Advertiser Status", List("DELETED")))
      )
    }
  }

  def product_ad_dim: PublicDimension = {
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      Dimension.newDimension("product_ad_dim", HiveEngine, LevelSix, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("status", StrType())
          , DimCol("description", StrType())
        )
        , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
      ).toPublicDimension("productAd","productAd",
        Set(
          PubCol("id", "Product Ad ID", InEquality)
          , PubCol("advertiser_id", "Advertiser ID", InEquality)
          , PubCol("description", "Product Ad Description", InEquality)
          , PubCol("status", "Product Ad Status", InEquality)
        ), highCardinalityFilters = Set(NotInFilter("Product Ad Status", List("DELETED")))
      )
    }
  }
  
  def getDefaultRegistry(forcedFilters: Set[ForcedFilter] = Set.empty): Registry = {
    val registryBuilder = new RegistryBuilder
    registryBuilder.register(pubfact3(forcedFilters))
    registryBuilder.register(pubfact2(forcedFilters))
    registryBuilder.register(pubfact(forcedFilters))
    registryBuilder.register(pubfactRev2(forcedFilters))
    registryBuilder.register(site_dim)
    registryBuilder.register(ad_dim)
    registryBuilder.register(advertiser_dim)
    registryBuilder.register(campaign_dim)
    registryBuilder.register(ad_group_dim)
    registryBuilder.register(keyword_dim)
    registryBuilder.register(product_ad_dim)
    registryBuilder.build()
  }

  test("create model should fail when non existing cube requested") {
    val jsonString = s"""{
                          "cube": "cube1",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Ad ID"},
                              {"field": "Impressions"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser ID", "order": "Asc"},
                              {"field": "Ad ID", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registryBuilder = new RegistryBuilder
    val registry = registryBuilder.build()
    val res = RequestModel.from(request, registry)
    res.failed.get.getMessage should startWith ("cube does not exist")
  }


  test("create model should succeed when cube columns requested without derived expression column") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed "))
  }

  test("create model should succeed when cube columns requested with derived expression column") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Advertiser Status"},
                              {"field": "Impressions"},
                              {"field": "Ad Group Start Date Full"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed "))
  }

  test("create model should succeed when dimension field with dimension key requested with facts") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed "))
  }

  test("create model should succeed when dimension field without dimension key requested with facts") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed "))
  }

  test("create model should fail when non existing field requested") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Field"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    res.isFailure shouldBe true
    res.failed.get.getMessage should startWith (s"requirement failed: ${UnknownFieldNameError("Field")}")
  }

  test("create model should fail when filter on non existing field requested") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Source"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Filter", "operator": "=", "value": "active"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    res.isFailure shouldBe true
    res.failed.get.getMessage should startWith (s"requirement failed: ${UnknownFieldNameError("Filter")}")
  }

  test("create model should fail when ordering on non existing field requested") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Source"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Blah", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    res.isFailure shouldBe true
    res.failed.get.getMessage should startWith ("requirement failed: Failed to determine dim or fact source for ordering by Blah")
  }

  test("create model should fail if ordering field is not in requested fields") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Source"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Campaign ID", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    res.isFailure shouldBe true
    res.failed.get.getMessage should startWith ("requirement failed: Ordering fields must be in requested fields")
  }

  test("create model should fail when dimension field with dimension key requested with facts for dimension not in fact") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Pricing Type"},
                              {"field": "Ad Status"},
                              {"field": "Impressions"},
                              {"field": "Ad ID"},
                              {"field": "Site ID"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    res.isFailure shouldBe true
    res.failed.get.getMessage should startWith (s"requirement failed: ${UnknownFieldNameError("Site ID")}")
  }

  test("create model should fail when dimension field without dimension key requested with facts for dimension not in fact") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Pricing Type"},
                              {"field": "Impressions"},
                              {"field": "Site Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    res.isFailure shouldBe true
    res.failed.get.getMessage should startWith (s"requirement failed: ${NoRelationWithPrimaryKeyError(request.cube, "Site ID", Option("Site Status"))}")
  }

  test("create model should fail when dimension filter with dimension key requested with facts for dimension not in fact") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Site ID", "operator": "=", "value": "101"},
                              {"field": "Site Status", "operator": "=", "value": "active"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    res.isFailure shouldBe true
    res.failed.get.getMessage should startWith (s"requirement failed: ${NoRelationWithPrimaryKeyError(request.cube, "Site ID")}")
  }

  test("create model should fail when dimension filter without dimension key requested with facts for dimension not in fact") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Site Status", "operator": "=", "value": "active"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    res.isFailure shouldBe true
    res.failed.get.getMessage should startWith (s"requirement failed: ${NoRelationWithPrimaryKeyError(request.cube, "Site ID", Option("Site Status"))}")
  }

  //Not sure how to simulate this at this time since all the error handling takes care of it
  ignore("Create model should fail if no candidates is found") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    res.isFailure shouldBe true
    res.failed.get.getMessage should startWith ("requirement failed: No candidates found for request!")
  }

  test("create model should succeed when cube columns requested with dim filter") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Campaign Status", "operator": "=", "value": "active"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed "))
    assert(res.toOption.get.isDimDriven, "Request should be dim driven but isn't!")
    assert(res.toOption.get.hasDimFilters)
    assert(!res.toOption.get.hasDimSortBy)
    assert(res.toOption.get.hasFactFilters)
    assert(!res.toOption.get.hasFactSortBy)
    assert(res.get.dimensionNameToJoinTypeMap("campaign_dim") == InnerJoin, "Should inner join as request is filtering on dim for nonDimDriven Case")
  }

  test("create model should succeed when cube columns requested for sync query with dim filter and dim sort") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "in", "values": ["$toDate", "$fromDate"]},
                              {"field": "Campaign Status", "operator": "=", "value": "active"}
                          ],
                          "sortBy": [
                              {"field": "Campaign ID", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed "))
    assert(res.toOption.get.isDimDriven, "Request should be dim driven but isn't!")
    assert(res.toOption.get.hasDimFilters)
    assert(res.toOption.get.hasDimSortBy)
    assert(res.toOption.get.hasFactFilters)
    assert(!res.toOption.get.hasFactSortBy)
    assert(res.toOption.get.factFilters.size === 1)
    assert(res.toOption.get.factFilters.map(_.field).contains("Advertiser ID"))
    assert(res.toOption.get.getMostRecentRequestedDate().equals(toDate))
    assert(res.get.dimensionNameToJoinTypeMap("campaign_dim") == InnerJoin, "Should inner join as request is filtering on dim for nonDimDriven Case")
  }

  test("create model should succeed when cube columns requested for sync query with dim filter and dim sort with forceFactDriven") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Campaign Status", "operator": "=", "value": "active"}
                          ],
                          "sortBy": [
                              {"field": "Campaign ID", "order": "Asc"}
                          ],
                          "forceFactDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed "))
    assert(res.toOption.get.isFactDriven, "Request should be fact driven but isn't!")
    assert(res.toOption.get.hasDimFilters)
    assert(res.toOption.get.hasDimSortBy)
    assert(res.toOption.get.hasFactFilters)
    assert(!res.toOption.get.hasFactSortBy)
    assert(res.toOption.get.factFilters.size === 1)
    assert(res.toOption.get.factFilters.map(_.field).contains("Advertiser ID"))
    assert(res.toOption.get.getMostRecentRequestedDate().equals(toDate))
    assert(res.get.dimensionNameToJoinTypeMap("campaign_dim") == InnerJoin, "Should inner join as request is filtering on dim for nonDimDriven Case")
  }

  test("create model should succeed when cube columns requested for async query with dim filter and dim sort") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Campaign Status", "operator": "=", "value": "active"}
                          ],
                          "sortBy": [
                              {"field": "Campaign ID", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed "))
    assert(!res.toOption.get.isDimDriven, "Request should not be dim driven but is!")
    assert(res.toOption.get.hasDimFilters)
    assert(res.toOption.get.hasDimSortBy)
    assert(res.toOption.get.hasFactFilters)
    assert(!res.toOption.get.hasFactSortBy)
    assert(res.toOption.get.factFilters.size === 1)
    assert(res.toOption.get.factFilters.map(_.field).contains("Advertiser ID"))
    assert(res.get.dimensionNameToJoinTypeMap("campaign_dim") == InnerJoin, "Should inner join as request is filtering on dim for nonDimDriven Case")
  }

  test("create model should succeed when cube columns requested for sync query with dim sort") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Campaign Status"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Campaign Status", "order": "Asc"},
                              {"field": "Campaign ID", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed "))
    assert(res.toOption.get.isDimDriven, "Request should be dim driven but isn't!")
    assert(!res.toOption.get.hasDimFilters)
    assert(res.toOption.get.hasDimSortBy)
    assert(res.toOption.get.hasFactFilters)
    assert(!res.toOption.get.hasFactSortBy)
    assert(res.toOption.get.factFilters.size === 1)
    assert(res.toOption.get.factFilters.map(_.field).contains("Advertiser ID"))
    assert(res.get.dimensionNameToJoinTypeMap("campaign_dim") == LeftOuterJoin, "Should left outer join as request is not filtering on dim")
  }

  test("create model should succeed when cube columns requested for async query with dim sort") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Campaign Status"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Campaign Status", "order": "Asc"},
                              {"field": "Campaign ID", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed "))
    assert(!res.toOption.get.isDimDriven, "Request should not be dim driven but is!")
    assert(!res.toOption.get.hasDimFilters)
    assert(res.toOption.get.hasDimSortBy)
    assert(res.toOption.get.hasFactFilters)
    assert(!res.toOption.get.hasFactSortBy)
    assert(res.toOption.get.factFilters.size === 1)
    assert(res.toOption.get.factFilters.map(_.field).contains("Advertiser ID"))
    assert(res.get.dimensionNameToJoinTypeMap("campaign_dim") == LeftOuterJoin, "Should left outer join as request is not filtering on dim")

  }

  test("create model should succeed when cube columns requested with fact filter") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Impressions", "operator": "between", "from": "10000", "to": "100000"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed "))
    assert(!res.toOption.get.isDimDriven, "Request should not be dim driven but is!")
    assert(!res.toOption.get.hasDimFilters)
    assert(!res.toOption.get.hasDimSortBy)
    assert(res.toOption.get.hasFactFilters)
    assert(!res.toOption.get.hasFactSortBy)
    assert(res.get.dimensionNameToJoinTypeMap.isEmpty)
  }

  test("create model should succeed when cube columns requested with fact filter and fact sort") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Impressions", "operator": "between", "from": "10000", "to": "100000"}
                          ],
                          "sortBy": [
                              {"field": "Campaign ID", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed "))
    assert(!res.toOption.get.isDimDriven, "Request should not be dim driven but is!")
    assert(!res.toOption.get.hasDimFilters)
    assert(!res.toOption.get.hasDimSortBy)
    assert(res.toOption.get.hasFactFilters)
    assert(res.toOption.get.hasFactSortBy)
    assert(res.get.dimensionNameToJoinTypeMap.isEmpty)

  }

  test("create model should succeed when cube columns requested with fact sort") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Pricing Type"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "Asc"},
                              {"field": "Campaign ID", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed "))
    assert(!res.toOption.get.isDimDriven, "Request should not be dim driven but is!")
    assert(!res.toOption.get.hasDimFilters)
    assert(!res.toOption.get.hasDimSortBy)
    assert(res.toOption.get.hasFactFilters)
    assert(res.toOption.get.hasFactSortBy)
    assert(res.toOption.get.factFilters.size === 1)
    assert(res.toOption.get.factFilters.map(_.field).contains("Advertiser ID"))
    assert(res.get.dimensionNameToJoinTypeMap.isEmpty)
  }

  test("create model should succeed when cube columns requested with dim filter and fact sort") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Pricing Type"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Campaign Status", "operator": "=", "value": "active"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "Asc"},
                              {"field": "Campaign ID", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed "))
    assert(!res.toOption.get.isDimDriven, "Request should not be dim driven but is!")
    assert(res.toOption.get.hasDimFilters)
    assert(!res.toOption.get.hasDimSortBy)
    assert(res.toOption.get.hasFactFilters)
    assert(res.toOption.get.hasFactSortBy)
    assert(res.toOption.get.factFilters.size === 1)
    assert(res.toOption.get.factFilters.map(_.field).contains("Advertiser ID"))
    assert(res.get.dimensionNameToJoinTypeMap("campaign_dim") == InnerJoin, "Should inner join as request is filtering on dim")
  }

  test("create model should succeed when cube columns requested with fact filter and dim sort") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Advertiser Status"},
                              {"field": "Campaign Status"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Impressions", "operator": "between", "from": "10000", "to": "100000"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Status", "order": "Asc"},
                              {"field": "Campaign Status", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed "))
    assert(!res.toOption.get.isDimDriven, "Request should not be dim driven but is!")
    assert(!res.toOption.get.hasDimFilters)
    assert(res.toOption.get.hasDimSortBy)
    assert(res.toOption.get.hasFactFilters)
    assert(!res.toOption.get.hasFactSortBy)
    assert(res.get.dimensionNameToJoinTypeMap("campaign_dim") == LeftOuterJoin, "Should left outer join as request is not filtering on dim")
  }

  test("order of filter should not change request model for sync query with fact filter") {
    val jsonString1 = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Campaign ID", "operator": "=", "value": "10000"},
                              {"field": "Impressions", "operator": "between", "from": "10000", "to": "100000"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""
    val jsonString2 = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Impressions", "operator": "between", "from": "10000", "to": "100000"},
                              {"field": "Campaign ID", "operator": "=", "value": "10000"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""


    val request1: ReportingRequest = getReportingRequestSync(jsonString1)
    val registry = getDefaultRegistry()
    val res1 = RequestModel.from(request1, registry)
    assert(res1.isSuccess, s"Create model failed : $res1")
    assert(!res1.toOption.get.isDimDriven, "Request should not be dim driven but is!")
    assert(!res1.toOption.get.hasDimFilters)
    assert(!res1.toOption.get.hasDimSortBy)
    assert(res1.toOption.get.hasFactFilters)
    assert(!res1.toOption.get.hasFactSortBy)
    assert(res1.get.dimensionNameToJoinTypeMap.isEmpty)


    val request2: ReportingRequest = getReportingRequestSync(jsonString2)
    val res2 = RequestModel.from(request1, registry)
    assert(res2.isSuccess, s"Create model failed : $res2")
    assert(!res2.toOption.get.isDimDriven, "Request should not be dim driven but is!")
    assert(!res2.toOption.get.hasDimFilters)
    assert(!res2.toOption.get.hasDimSortBy)
    assert(res2.toOption.get.hasFactFilters)
    assert(!res2.toOption.get.hasFactSortBy)
    assert(res2.get.dimensionNameToJoinTypeMap.isEmpty)

    assert(res1 === res2)
  }

  test("order of filter should not change request model for async query with fact filter") {
    val jsonString1 = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Campaign ID", "operator": "=", "value": "10000"},
                              {"field": "Impressions", "operator": "between", "from": "10000", "to": "100000"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""
    val jsonString2 = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Impressions", "operator": "between", "from": "10000", "to": "100000"},
                              {"field": "Campaign ID", "operator": "=", "value": "10000"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""


    val request1: ReportingRequest = getReportingRequestAsync(jsonString1)
    val registry = getDefaultRegistry()
    val res1 = RequestModel.from(request1, registry)
    assert(res1.isSuccess, s"Create model failed : $res1")
    assert(!res1.toOption.get.isDimDriven, "Request should not be dim driven but is!")
    assert(!res1.toOption.get.hasDimFilters)
    assert(!res1.toOption.get.hasDimSortBy)
    assert(res1.toOption.get.hasFactFilters)
    assert(!res1.toOption.get.hasFactSortBy)

    val request2: ReportingRequest = getReportingRequestAsync(jsonString2)
    val res2 = RequestModel.from(request1, registry)
    assert(res2.isSuccess, s"Create model failed : $res2")
    assert(!res2.toOption.get.isDimDriven, "Request should not be dim driven but is!")
    assert(!res2.toOption.get.hasDimFilters)
    assert(!res2.toOption.get.hasDimSortBy)
    assert(res2.toOption.get.hasFactFilters)
    assert(!res2.toOption.get.hasFactSortBy)

    assert(res1 === res2)
  }

  test("order of filter should not change request model for sync query with dim filter") {
    val jsonString1 = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Campaign ID", "operator": "=", "value": "10000"},
                              {"field": "Campaign Status", "operator": "=", "value": "active"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val jsonString2 = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Campaign Status", "operator": "=", "value": "active"},
                              {"field": "Campaign ID", "operator": "=", "value": "10000"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""


    val request1: ReportingRequest = getReportingRequestSync(jsonString1)
    val registry = getDefaultRegistry()
    val res1 = RequestModel.from(request1, registry)
    assert(res1.isSuccess, s"Create model failed : $res1")
    assert(res1.toOption.get.isDimDriven, "Request should be dim driven but isn't!")
    assert(res1.toOption.get.hasDimFilters)
    assert(!res1.toOption.get.hasDimSortBy)
    assert(res1.toOption.get.hasFactFilters)
    assert(!res1.toOption.get.hasFactSortBy)
    assert(res1.toOption.get.factFilters.size === 2)
    assert(res1.toOption.get.factFilters.map(_.field).contains("Advertiser ID"))
    assert(res1.toOption.get.factFilters.map(_.field).contains("Campaign ID"))
    assert(res1.get.dimensionNameToJoinTypeMap("campaign_dim") == InnerJoin, "Should inner join as request is filtering on dim")

    val request2: ReportingRequest = getReportingRequestSync(jsonString2)
    val res2 = RequestModel.from(request2, registry)
    assert(res2.isSuccess, s"Create model failed : $res2")
    assert(res2.toOption.get.isDimDriven, "Request should be dim driven but isn't!")
    assert(res2.toOption.get.hasDimFilters)
    assert(!res2.toOption.get.hasDimSortBy)
    assert(res2.toOption.get.hasFactFilters)
    assert(!res2.toOption.get.hasFactSortBy)
    assert(res2.toOption.get.factFilters.size === 2)
    assert(res2.toOption.get.factFilters.map(_.field).contains("Advertiser ID"))
    assert(res2.toOption.get.factFilters.map(_.field).contains("Campaign ID"))
    assert(res2.get.dimensionNameToJoinTypeMap("campaign_dim") == InnerJoin, "Should inner join as request is filtering on dim")


    // Compare objects except for the actual reportingRequest since it will contain the original order.
    assert(res1.get.copy(reportingRequest = request1) === res2.get.copy(reportingRequest = request1))
  }

  test("order of filter should not change request model for async query with dim filter") {
    val jsonString1 = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Campaign ID", "operator": "=", "value": "10000"},
                              {"field": "Campaign Status", "operator": "=", "value": "active"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val jsonString2 = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Campaign Status", "operator": "=", "value": "active"},
                              {"field": "Campaign ID", "operator": "=", "value": "10000"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""


    val request1: ReportingRequest = getReportingRequestAsync(jsonString1)
    val registry = getDefaultRegistry()
    val res1 = RequestModel.from(request1, registry)
    assert(res1.isSuccess, s"Create model failed : $res1")
    assert(!res1.toOption.get.isDimDriven, "Request should not be dim driven but is!")
    assert(res1.toOption.get.hasDimFilters)
    assert(!res1.toOption.get.hasDimSortBy)
    assert(res1.toOption.get.hasFactFilters)
    assert(!res1.toOption.get.hasFactSortBy)
    assert(res1.get.dimensionNameToJoinTypeMap("campaign_dim") == InnerJoin, "Should inner join as request is filtering on dim")


    val request2: ReportingRequest = getReportingRequestAsync(jsonString2)
    val res2 = RequestModel.from(request2, registry)
    assert(res2.isSuccess, s"Create model failed : $res2")
    assert(!res2.toOption.get.isDimDriven, "Request should not be dim driven but is!")
    assert(res2.toOption.get.hasDimFilters)
    assert(!res2.toOption.get.hasDimSortBy)
    assert(res2.toOption.get.hasFactFilters)
    assert(!res2.toOption.get.hasFactSortBy)
    assert(res2.get.dimensionNameToJoinTypeMap("campaign_dim") == InnerJoin, "Should inner join as request is filtering on dim")


    // Compare objects except for the actual reportingRequest since it will contain the original order.
    assert(res1.get.copy(reportingRequest = request1) === res2.get.copy(reportingRequest = request1))
  }

  test("order of sort by should not change request model for sync query with fact sort") {
    val jsonString1 = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Pricing Type"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Campaign ID", "order": "Asc"},
                              {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val jsonString2 = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Pricing Type"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Campaign ID", "order": "Asc"},
                              {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""


    val request1: ReportingRequest = getReportingRequestSync(jsonString1)
    val registry = getDefaultRegistry()
    val res1 = RequestModel.from(request1, registry)
    assert(res1.isSuccess, s"Create model failed : $res1")
    assert(!res1.toOption.get.isDimDriven, "Request should not be dim driven but is!")
    assert(!res1.toOption.get.hasDimFilters)
    assert(!res1.toOption.get.hasDimSortBy)
    assert(res1.toOption.get.hasFactFilters)
    assert(res1.toOption.get.hasFactSortBy)
    assert(res1.toOption.get.factFilters.size === 1)
    assert(res1.toOption.get.factFilters.map(_.field).contains("Advertiser ID"))
    assert(res1.get.dimensionNameToJoinTypeMap.isEmpty)


    val request2: ReportingRequest = getReportingRequestSync(jsonString2)
    val res2 = RequestModel.from(request2, registry)
    assert(res2.isSuccess, s"Create model failed : $res2")
    assert(!res2.toOption.get.isDimDriven, "Request should not be dim driven but is!")
    assert(!res2.toOption.get.hasDimFilters)
    assert(!res2.toOption.get.hasDimSortBy)
    assert(res2.toOption.get.hasFactFilters)
    assert(res2.toOption.get.hasFactSortBy)
    assert(res2.toOption.get.factFilters.size === 1)
    assert(res2.toOption.get.factFilters.map(_.field).contains("Advertiser ID"))
    assert(res2.get.dimensionNameToJoinTypeMap.isEmpty)

    assert(res1 === res2)
  }

  test("order of sort by should not change request model for async query with fact sort") {
    val jsonString1 = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Pricing Type"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Campaign ID", "order": "Asc"},
                              {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val jsonString2 = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Pricing Type"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Campaign ID", "order": "Asc"},
                              {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""


    val request1: ReportingRequest = getReportingRequestAsync(jsonString1)
    val registry = getDefaultRegistry()
    val res1 = RequestModel.from(request1, registry)
    assert(res1.isSuccess, s"Create model failed : $res1")
    assert(!res1.toOption.get.isDimDriven, "Request should not be dim driven but is!")
    assert(!res1.toOption.get.hasDimFilters)
    assert(!res1.toOption.get.hasDimSortBy)
    assert(res1.toOption.get.hasFactFilters)
    assert(res1.toOption.get.hasFactSortBy)
    assert(res1.toOption.get.factFilters.size === 1)
    assert(res1.toOption.get.factFilters.map(_.field).contains("Advertiser ID"))

    val request2: ReportingRequest = getReportingRequestAsync(jsonString2)
    val res2 = RequestModel.from(request2, registry)
    assert(res2.isSuccess, s"Create model failed : $res2")
    assert(!res2.toOption.get.isDimDriven, "Request should not be dim driven but is!")
    assert(!res2.toOption.get.hasDimFilters)
    assert(!res2.toOption.get.hasDimSortBy)
    assert(res2.toOption.get.hasFactFilters)
    assert(res2.toOption.get.hasFactSortBy)
    assert(res2.toOption.get.factFilters.size === 1)
    assert(res2.toOption.get.factFilters.map(_.field).contains("Advertiser ID"))

    assert(res1 === res2)
  }

  test("order of sort by should not change request model for sync query with dim sort") {
    val jsonString1 = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Pricing Type"},
                              {"field": "Impressions"},
                              {"field": "Campaign Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Campaign ID", "order": "Asc"},
                              {"field": "Campaign Status", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val jsonString2 = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Pricing Type"},
                              {"field": "Impressions"},
                              {"field": "Campaign Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Campaign ID", "order": "Asc"},
                              {"field": "Campaign Status", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""


    val request1: ReportingRequest = getReportingRequestSync(jsonString1)
    val registry = getDefaultRegistry()
    val res1 = RequestModel.from(request1, registry)
    assert(res1.isSuccess, s"Create model failed : $res1")
    assert(res1.toOption.get.isDimDriven, "Request should be dim driven but isn't!")
    assert(!res1.toOption.get.hasDimFilters)
    assert(res1.toOption.get.hasDimSortBy)
    assert(res1.toOption.get.hasFactFilters)
    assert(!res1.toOption.get.hasFactSortBy)
    assert(res1.toOption.get.factFilters.size === 1)
    assert(res1.toOption.get.factFilters.map(_.field).contains("Advertiser ID"))
    assert(res1.get.dimensionNameToJoinTypeMap("campaign_dim") == LeftOuterJoin, "Should left outer join as request is not filtering on dim")

    val request2: ReportingRequest = getReportingRequestSync(jsonString2)
    val res2 = RequestModel.from(request2, registry)
    assert(res2.isSuccess, s"Create model failed : $res2")
    assert(res2.toOption.get.isDimDriven, "Request should be dim driven but isn't!")
    assert(!res2.toOption.get.hasDimFilters)
    assert(res2.toOption.get.hasDimSortBy)
    assert(res2.toOption.get.hasFactFilters)
    assert(!res2.toOption.get.hasFactSortBy)
    assert(res2.toOption.get.factFilters.size === 1)
    assert(res2.toOption.get.factFilters.map(_.field).contains("Advertiser ID"))
    assert(res2.get.dimensionNameToJoinTypeMap("campaign_dim") == LeftOuterJoin, "Should left outer join as request is not filtering on dim")

    assert(res1 === res2)
  }

  test("order of sort by should not change request model for async query with dim sort") {
    val jsonString1 = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Pricing Type"},
                              {"field": "Impressions"},
                              {"field": "Campaign Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Campaign ID", "order": "Asc"},
                              {"field": "Campaign Status", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val jsonString2 = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Pricing Type"},
                              {"field": "Impressions"},
                              {"field": "Campaign Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Campaign ID", "order": "Asc"},
                              {"field": "Campaign Status", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""


    val request1: ReportingRequest = getReportingRequestAsync(jsonString1)
    val registry = getDefaultRegistry()
    val res1 = RequestModel.from(request1, registry)
    assert(res1.isSuccess, s"Create model failed : $res1")
    assert(!res1.toOption.get.isDimDriven, "Request should not be dim driven but is!")
    assert(!res1.toOption.get.hasDimFilters)
    assert(res1.toOption.get.hasDimSortBy)
    assert(res1.toOption.get.hasFactFilters)
    assert(!res1.toOption.get.hasFactSortBy)
    assert(res1.toOption.get.factFilters.size === 1)
    assert(res1.toOption.get.factFilters.map(_.field).contains("Advertiser ID"))

    val request2: ReportingRequest = getReportingRequestAsync(jsonString2)
    val res2 = RequestModel.from(request2, registry)
    assert(res2.isSuccess, s"Create model failed : $res2")
    assert(!res2.toOption.get.isDimDriven, "Request should not be dim driven but is!")
    assert(!res2.toOption.get.hasDimFilters)
    assert(res2.toOption.get.hasDimSortBy)
    assert(res2.toOption.get.hasFactFilters)
    assert(!res2.toOption.get.hasFactSortBy)
    assert(res2.toOption.get.factFilters.size === 1)
    assert(res2.toOption.get.factFilters.map(_.field).contains("Advertiser ID"))

    assert(res1 === res2)
  }

  test("create model should succeed when cube columns requested for sync query with constant fields") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Report Type", "value" : "MyType"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Campaign Status", "operator": "=", "value": "active"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed "))
    assert(res.toOption.get.isDimDriven, "Request should be dim driven but isn't!")
    assert(res.toOption.get.hasDimFilters)
    assert(!res.toOption.get.hasDimSortBy)
    assert(res.toOption.get.hasFactFilters)
    assert(!res.toOption.get.hasFactSortBy)
    assert(res.toOption.get.requestCols
      .filter(_.isInstanceOf[ConstantColumnInfo])
      .map(_.asInstanceOf[ConstantColumnInfo])
      .exists(ci => ci.alias === "Report Type" && ci.value === "MyType"))
    assert(res.toOption.get.factFilters.size === 1)
    assert(res.toOption.get.factFilters.map(_.field).contains("Advertiser ID"))
    assert(res.get.dimensionNameToJoinTypeMap("campaign_dim") == InnerJoin, "Should inner join as request is filtering on dim")

  }

  test("create model should fail when from date is in future") {

    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Report Type", "value" : "MyType"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$futureFromDate", "to": "$futureToDate"},
                              {"field": "Campaign Status", "operator": "=", "value": "active"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isFailure, res.errorMessage("Create model failed "))
  }

  test("create model should fail when all dates are in future") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Report Type", "value" : "MyType"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "in", "values": ["$futureFromDate", "$futureToDate"]},
                              {"field": "Campaign Status", "operator": "=", "value": "active"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

     val request: ReportingRequest = getReportingRequestSync(jsonString)
     val registry = getDefaultRegistry()
     val res = RequestModel.from(request, registry)
     assert(res.isFailure, res.errorMessage("Create model failed "))
   }

  test("create model should pass when any date is in past and other dates are in future") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Report Type", "value" : "MyType"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "in", "values": ["$futureFromDate", "$toDate", "$futureToDate"]},
                              {"field": "Campaign Status", "operator": "=", "value": "active"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed "))
  }

  test("create model should succeed when cube columns requested for async query with constant fields and forceDimensionDriven") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Report Type", "value" : "MyType"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Campaign Status", "operator": "=", "value": "active"}
                          ],
                          "sortBy": [
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed "))
    assert(res.toOption.get.isDimDriven, "Request should be dim driven but isn't!")
    assert(res.toOption.get.hasDimFilters)
    assert(!res.toOption.get.hasDimSortBy)
    assert(res.toOption.get.hasFactFilters)
    assert(!res.toOption.get.hasFactSortBy)
    assert(res.toOption.get.requestCols
      .filter(_.isInstanceOf[ConstantColumnInfo])
      .map(_.asInstanceOf[ConstantColumnInfo])
      .exists(ci => ci.alias === "Report Type" && ci.value === "MyType"))
    assert(res.toOption.get.factFilters.size === 1)
    assert(res.toOption.get.factFilters.map(_.field).contains("Advertiser ID"))
    assert(res.get.dimensionNameToJoinTypeMap("campaign_dim") == RightOuterJoin, "Should RightOuterJoin as request is dimDriven")
  }

  test("create model should succeed when cube columns requested for sync query with constant fields and forceFactDriven") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Report Type", "value" : "MyType"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Campaign Status", "operator": "=", "value": "active"}
                          ],
                          "sortBy": [
                          ],
                          "forceFactDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed "))
    assert(res.toOption.get.isFactDriven, "Request should be fact driven but isn't!")
    assert(res.toOption.get.hasDimFilters)
    assert(!res.toOption.get.hasDimSortBy)
    assert(res.toOption.get.hasFactFilters)
    assert(!res.toOption.get.hasFactSortBy)
    assert(res.toOption.get.requestCols
      .filter(_.isInstanceOf[ConstantColumnInfo])
      .map(_.asInstanceOf[ConstantColumnInfo])
      .exists(ci => ci.alias === "Report Type" && ci.value === "MyType"))
    assert(res.toOption.get.factFilters.size === 1)
    assert(res.toOption.get.factFilters.map(_.field).contains("Advertiser ID"))
    assert(res.get.dimensionNameToJoinTypeMap("campaign_dim") == InnerJoin, "Should RightOuterJoin as request is filtering on dim")

  }

  test("create model should succeed when cube columns requested for async query with constant fields") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Report Type", "value" : "MyType"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Campaign Status", "operator": "=", "value": "active"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed "))
    assert(!res.toOption.get.isDimDriven, "Request should not be dim driven but is!")
    assert(res.toOption.get.hasDimFilters)
    assert(!res.toOption.get.hasDimSortBy)
    assert(res.toOption.get.hasFactFilters)
    assert(!res.toOption.get.hasFactSortBy)
    assert(res.toOption.get.requestCols
      .filter(_.isInstanceOf[ConstantColumnInfo])
      .map(_.asInstanceOf[ConstantColumnInfo])
      .exists(ci => ci.alias === "Report Type" && ci.value === "MyType"))
    assert(res.toOption.get.factFilters.size === 1)
    assert(res.toOption.get.factFilters.map(_.field).contains("Advertiser ID"))
    assert(res.get.dimensionNameToJoinTypeMap("campaign_dim") == InnerJoin, "Should RightOuterJoin as request is filtering on dim")

  }

  test("create model should fail when missing schema required field") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Impressions"},
                              {"field": "Campaign ID"}
                          ],
                          "filterExpressions": [
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Campaign Status", "operator": "=", "value": "active"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    res.isFailure shouldBe true
    res.failed.get.getMessage should startWith ("requirement failed: required filter for cube=publicFact, schema=advertiser, fact=fact1 not found = Set(Advertiser ID)")
  }

  test(
    """generate valid model for sync query with fields having dimension attribute,
       filter on fact dim col which is a foreign key and it is in the list of fields,
       order by fact dim col which is a foreign key""") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Advertiser ID", "operator": "in", "values": ["1", "2", "3"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser ID", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 3)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser Status")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser ID")) === true)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true)
    assert(model.hasDimSortBy === true)
    assert(model.isDimDriven === true)
    assert(model.hasFactFilters)
    assert(model.dimColumnAliases.contains("Advertiser Status") === true)
    assert(model.dimensionsCandidates.size === 1, s"dimensionsCandidates = ${model.dimensionsCandidates}")
    assert(model.dimensionsCandidates.exists(_.dim.name == "advertiser") === true)
    assert(model.dimSortByMap.contains("Advertiser ID") === true)
    assert(model.dimSortByMap("Advertiser ID") === ASC)
    assert(model.factFilters.size === 1)
    assert(model.factFilters.map(_.field).contains("Advertiser ID"))

    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")

  }

  test(
    """generate valid model for async query with forceDimensionDriven and fields having dimension attribute,
      |filter on fact dim col which is a foreign key and it is in the list of fields,
      |order by fact dim col which is a foreign key""".stripMargin) {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Advertiser ID", "operator": "in", "values": ["1", "2", "3"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser ID", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 3)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser Status")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser ID")) === true)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true)
    assert(model.hasDimSortBy === true)
    assert(model.isDimDriven === true)
    assert(model.hasFactFilters)
    assert(model.dimColumnAliases.contains("Advertiser Status") === true)
    assert(model.dimensionsCandidates.size === 1, s"dimensionsCandidates = ${model.dimensionsCandidates}")
    assert(model.dimensionsCandidates.exists(_.dim.name == "advertiser") === true)
    assert(model.dimSortByMap.contains("Advertiser ID") === true)
    assert(model.dimSortByMap("Advertiser ID") === ASC)
    assert(model.factFilters.size === 1)
    assert(model.factFilters.map(_.field).contains("Advertiser ID"))

    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")

    assert(res.get.dimensionNameToJoinTypeMap("advertiser_dim") == RightOuterJoin, "Should RightOuterJoin as request is dimDriven")


  }

  test(
    """generate valid model for async query with fields having dimension attribute,
      |filter on fact dim col which is a foreign key and it is in the list of fields,
      |order by fact dim col which is a foreign key""".stripMargin) {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Advertiser ID", "operator": "in", "values": ["1", "2", "3"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser ID", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 3)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser Status")) === true)
    assert(model.requestCols.contains(FactColumnInfo("Advertiser ID")) === true)
    assert(model.hasDimSortBy === true)
    assert(model.isDimDriven === false)
    assert(model.hasFactFilters)
    assert(model.factFilters.size === 1)
    assert(model.factFilters.map(_.field).contains("Advertiser ID"))
    assert(model.dimColumnAliases.contains("Advertiser Status") === true)
    assert(model.dimensionsCandidates.size === 1, s"dimensionsCandidates = ${model.dimensionsCandidates}")
    assert(model.dimensionsCandidates.exists(_.dim.name == "advertiser") === true)
    assert(model.dimSortByMap.contains("Advertiser ID") === true)
    assert(model.dimSortByMap("Advertiser ID") === ASC)

    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")
    assert(res.get.dimensionNameToJoinTypeMap("advertiser_dim") == LeftOuterJoin, "Should RightOuterJoin as request is fact driven and async")

  }

  test("""generate valid model for sync query with fields having no dimension attribute,
      filter on fact dim col which is a foreign key and it is in the list of fields,
      order by fact dim col which is a foreign key""") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Advertiser ID", "operator": "in", "values": ["1", "2", "3"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser ID", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, s"$res")
    val model = res.toOption.get
    assert(model.requestCols.size === 2)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser ID")) === true)
    assert(model.hasDimSortBy === true)
    assert(model.isDimDriven === true)
    assert(model.hasFactFilters)
    assert(model.dimColumnAliases === Set("Advertiser ID"), model.dimColumnAliases )
    assert(model.dimensionsCandidates.size === 1, s"dimensionsCandidates = ${model.dimensionsCandidates}")
    assert(model.dimSortByMap.contains("Advertiser ID") === true)
    assert(model.dimSortByMap("Advertiser ID") === ASC)
    assert(model.factFilters.size === 1)
    assert(model.factFilters.map(_.field).contains("Advertiser ID"))

    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.filters.exists(_.field === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.filters}")
  }

  test(
    """generate valid model for async query with forceDimensionDriven and fields having no dimension attribute,
       filter on fact dim col which is a foreign key and it is in the list of fields,
       order by fact dim col which is a foreign key""") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Advertiser ID", "operator": "in", "values": ["1", "2", "3"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser ID", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 2)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser ID")) === true)
    assert(model.hasDimSortBy === true)
    assert(model.isDimDriven === true)
    assert(model.hasFactFilters)
    assert(model.dimColumnAliases === Set("Advertiser ID"))
    assert(model.dimensionsCandidates.size === 1, s"dimensionsCandidates = ${model.dimensionsCandidates}")
    assert(model.dimSortByMap.contains("Advertiser ID") === true)
    assert(model.dimSortByMap("Advertiser ID") === ASC)
    assert(model.factFilters.size === 1)
    assert(model.factFilters.map(_.field).contains("Advertiser ID"))

    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.filters.exists(_.field === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.filters}")
    assert(model.dimensionNameToJoinTypeMap("advertiser_dim") == RightOuterJoin, "ROJ for dim driven query")
  }

  test(
    """generate valid model for async query with fields having no dimension attribute,
      |filter on fact dim col which is a foreign key and it is in the list of fields,
      |order by fact dim col which is a foreign key""".stripMargin) {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Advertiser ID", "operator": "in", "values": ["1", "2", "3"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser ID", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 2)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true)
    assert(model.requestCols.contains(FactColumnInfo("Advertiser ID")) === true)
    assert(model.hasDimSortBy === false)
    assert(model.isDimDriven === false)
    assert(model.hasFactFilters)
    assert(model.factFilters.size === 1)
    assert(model.factFilters.map(_.field).contains("Advertiser ID"))
    assert(model.dimColumnAliases.isEmpty === true)
    assert(model.dimensionsCandidates.size === 0, s"dimensionsCandidates = ${model.dimensionsCandidates}")
    assert(model.factSortByMap.contains("Advertiser ID") === true)
    assert(model.factSortByMap("Advertiser ID") === ASC)
    assert(model.dimensionNameToJoinTypeMap.isEmpty)
  }

  test("""generate valid model for sync query with fields having multiple dimension keys,
      filter on fact dim col which is a foreign key and it is in the list of fields,
      order by fact dim col which is a foreign key""") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Advertiser ID", "operator": "in", "values": ["1", "2", "3"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser ID", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, s"$res")
    val model = res.toOption.get
    assert(model.requestCols.size === 3)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser ID")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Campaign ID")) === true)
    assert(model.hasDimSortBy === true)
    assert(model.isDimDriven === true)
    assert(model.hasFactFilters)
    assert(model.dimColumnAliases === Set("Advertiser ID", "Campaign ID"), model.dimColumnAliases )
    assert(model.dimensionsCandidates.size === 1, s"dimensionsCandidates = ${model.dimensionsCandidates}")
    assert(model.dimSortByMap.contains("Advertiser ID") === true)
    assert(model.dimSortByMap("Advertiser ID") === ASC)
    assert(model.factFilters.size === 1)
    assert(model.factFilters.map(_.field).contains("Advertiser ID"))

    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.filters.exists(_.field === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.filters}")
  }

  test("generate valid model for async query with forceDimensionDriven and fields having multiple dimension keys,filter on fact dim col which is a foreign key and it is in the list of fields,order by fact dim col which is a foreign key") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Advertiser ID", "operator": "in", "values": ["1", "2", "3"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser ID", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, s"$res")
    val model = res.toOption.get
    assert(model.requestCols.size === 3)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser ID")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Campaign ID")) === true)
    assert(model.hasDimSortBy === true)
    assert(model.isDimDriven === true)
    assert(model.hasFactFilters)
    assert(model.dimColumnAliases === Set("Advertiser ID", "Campaign ID"), model.dimColumnAliases )
    assert(model.dimensionsCandidates.size === 1, s"dimensionsCandidates = ${model.dimensionsCandidates}")
    assert(model.dimSortByMap.contains("Advertiser ID") === true)
    assert(model.dimSortByMap("Advertiser ID") === ASC)
    assert(model.factFilters.size === 1)
    assert(model.factFilters.map(_.field).contains("Advertiser ID"))

    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.filters.exists(_.field === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.filters}")

    assert(model.dimensionNameToJoinTypeMap("campaign_dim") == RightOuterJoin, "ROJ for dim driven query")
  }

  test(
    """generate valid model for async query with fields having multiple dimension keys,
       filter on fact dim col which is a foreign key and it is in the list of fields,
       order by fact dim col which is a foreign key""") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Advertiser ID", "operator": "in", "values": ["1", "2", "3"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser ID", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 3)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true)
    assert(model.requestCols.contains(FactColumnInfo("Advertiser ID")) === true)
    assert(model.requestCols.contains(FactColumnInfo("Campaign ID")) === true)
    assert(model.hasDimSortBy === false)
    assert(model.isDimDriven === false)
    assert(model.hasFactFilters)
    assert(model.factFilters.size === 1)
    assert(model.factFilters.map(_.field).contains("Advertiser ID"))
    assert(model.dimColumnAliases.isEmpty === true)
    assert(model.dimensionsCandidates.size === 0, s"dimensionsCandidates = ${model.dimensionsCandidates}")
    assert(model.factSortByMap.contains("Advertiser ID") === true)
    assert(model.factSortByMap("Advertiser ID") === ASC)
    assert(model.dimensionNameToJoinTypeMap.isEmpty)
  }

  test("generate valid model for sync dim driven query with non id fields having multiple dimension ,and should include the foreign keys of the other dimension to favor multiple dimensions joins") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Campaign Name"},
                              {"field": "Ad Group ID"},
                              {"field": "Ad Group Name"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "forceDimensionDriven" : true
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 5)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true, res.errorMessage("Impressions Missing"))
    assert(model.requestCols.contains(DimColumnInfo("Campaign ID")) === true, res.errorMessage("Campaign ID Missing"))
    assert(model.isDimDriven === true, res.errorMessage("Not a dim driven query"))
    assert(model.hasFactFilters, res.errorMessage("Fact Filters missing"))
    assert(model.factFilters.size === 1, res.errorMessage("Fact Filters != 1"))
    assert(model.factFilters.map(_.field).contains("Advertiser ID"))
    assert(model.dimColumnAliases.isEmpty === false, res.errorMessage("Dim Candidates empty"))

    assert(model.dimensionNameToJoinTypeMap("campaign_dim") == RightOuterJoin, "Should be RightOuterJoin as request is dimDriven")
    assert(model.dimensionNameToJoinTypeMap("ad_group_dim") == RightOuterJoin, "Should be RightOuterJoin as request is dimDriven")


    // It is case of Multiple Dimensions
    assert(model.dimensionsCandidates.size === 2, s"dimensionsCandidates = ${model.dimensionsCandidates}")

    val adGroupDimCandidate = model.dimensionsCandidates.filter(d=> d.dim.name=="ad_group").firstKey

    val campaignDimCandidate = model.dimensionsCandidates.filter(d=> d.dim.name=="campaign").firstKey

    //Test for to check the join candidate list
    assert(adGroupDimCandidate.lowerCandidates.head.name.equals("campaign"), s"Missing join candidate for ad_group ${adGroupDimCandidate.lowerCandidates} ")

    assert(campaignDimCandidate.upperCandidates.head.name.equals("ad_group"), s"Missing upper candidate for campaign ${campaignDimCandidate.upperCandidates} ")

    // Should Include foreign keys in the upper hierarchy dims to favor join
    assert(adGroupDimCandidate.fields.contains("Campaign ID") ,
      s" Ad Group Dimension should include the Campaign ID although not requested to favor multiple join = ${model.dimensionsCandidates}")
  }

  test("Test upper and lower join candidates in the multiple dimension joins") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Advertiser Name"},
                              {"field": "Campaign ID"},
                              {"field": "Campaign Name"},
                              {"field": "Ad Group ID"},
                              {"field": "Ad Group Name"},
                              {"field": "Product Ad ID"},
                              {"field": "Product Ad Description"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "forceDimensionDriven" : true
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get

    // It is case of Multiple Dimensions
    assert(model.dimensionsCandidates.size === 4, s"dimensionsCandidates = ${model.dimensionsCandidates}")

    val advertiserDimCandidate = model.dimensionsCandidates.filter(d=> d.dim.name=="advertiser").firstKey

    val adGroupDimCandidate = model.dimensionsCandidates.filter(d=> d.dim.name=="ad_group").firstKey

    val campaignDimCandidate = model.dimensionsCandidates.filter(d=> d.dim.name=="campaign").firstKey

    val productAdDimCandidate = model.dimensionsCandidates.filter(d=> d.dim.name=="productAd").firstKey

    //Test for to check the join candidate list
    assert(adGroupDimCandidate.upperCandidates.equals(List.empty), s"Lower candidate should be empty for ad_group ${adGroupDimCandidate.upperCandidates} ")
    assert(adGroupDimCandidate.lowerCandidates.head.name.equals("campaign"), s"Missing join candidate for ad_group ${adGroupDimCandidate.lowerCandidates} ")

    assert(campaignDimCandidate.upperCandidates.head.name.equals("ad_group"), s"Missing upper candidate for campaign ${campaignDimCandidate.upperCandidates} ")
    assert(campaignDimCandidate.lowerCandidates.head.name.equals("advertiser"), s"Missing upper candidate for campaign ${campaignDimCandidate.upperCandidates} ")

    assert(advertiserDimCandidate.upperCandidates.head.name.equals("campaign"), s"Missing upper candidate for advertiser ${advertiserDimCandidate.upperCandidates} ")
    assert(advertiserDimCandidate.lowerCandidates.equals(List.empty), s"Lower candidate should be empty for advertiser  ${advertiserDimCandidate.lowerCandidates}")

    assert(productAdDimCandidate.upperCandidates.equals(List.empty), s"Lower candidate should be empty for productAd ${productAdDimCandidate.upperCandidates} ")
    assert(productAdDimCandidate.lowerCandidates.head.name.equals("advertiser"), s"Missing join candidate for productAd ${productAdDimCandidate.lowerCandidates} ")


// Checking the size of upper and lower candidates, shouldnt be more than 1
    assert(adGroupDimCandidate.lowerCandidates.size==1, s" Number of lower candiadates overflow for ad_group ${adGroupDimCandidate.lowerCandidates} ")

    assert(campaignDimCandidate.upperCandidates.size==1, s" Number of lower candiadates overflow for campaign ${campaignDimCandidate.upperCandidates} ")
    assert(campaignDimCandidate.lowerCandidates.size==1, s" Number of lower candiadates overflow for campaign ${campaignDimCandidate.upperCandidates} ")

    assert(advertiserDimCandidate.upperCandidates.size==1, s" Number of lower candiadates overflow for advertiser ${campaignDimCandidate.upperCandidates} ")

    assert(productAdDimCandidate.upperCandidates.size==0, s" Number of upper candidates overflow for productAd ${productAdDimCandidate.upperCandidates} ")
    assert(productAdDimCandidate.lowerCandidates.size==1, s" Number of lower candidates overflow for productAd ${productAdDimCandidate.lowerCandidates} ")

    assert(model.dimensionNameToJoinTypeMap("advertiser_dim") == RightOuterJoin, "Should be RightOuterJoin as request is dimDriven")
    assert(model.dimensionNameToJoinTypeMap("campaign_dim") == RightOuterJoin, "Should be RightOuterJoin as request is dimDriven")
    assert(model.dimensionNameToJoinTypeMap("ad_group_dim") == RightOuterJoin, "Should be RightOuterJoin as request is dimDriven")
    assert(model.dimensionNameToJoinTypeMap("product_ad_dim") == RightOuterJoin, "Should be RightOuterJoin as request is dimDriven")
  }


  test(
    """generate valid model for sync query with fields having dimension attribute,
       filter on fact dim col which is a foreign key and it is in the list of fields,
       order by dim attribute""") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Ad Group ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Ad Group ID", "operator": "in", "values": ["1", "2", "3"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Status", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 4)
    assert(model.requestCols.contains(DimColumnInfo("Campaign ID")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser Status")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Ad Group ID")) === true)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true)
    assert(model.hasDimSortBy === true)
    assert(model.isDimDriven === true)
    assert(model.hasFactFilters)
    assert(model.factFilters.size === 2)
    assert(model.factFilters.map(_.field).contains("Advertiser ID"))
    assert(model.factFilters.map(_.field).contains("Ad Group ID"))
    assert(model.dimColumnAliases.contains("Advertiser Status") === true)
    assert(model.dimensionsCandidates.size === 2, s"dimensionsCandidates = ${model.dimensionsCandidates}")
    assert(model.dimensionsCandidates.exists(_.dim.name == "ad_group") === true)
    assert(model.dimensionsCandidates.exists(_.dim.name == "advertiser") === true)
    assert(model.dimSortByMap.contains("Advertiser Status") === true)
    assert(model.dimSortByMap("Advertiser Status") === ASC)

    assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields.exists(_ === "Ad Group ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields}")
    //assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields.exists(_ === "Campaign ID") === true,
      //s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")

    assert(model.dimensionNameToJoinTypeMap("advertiser_dim") == LeftOuterJoin, "Should LeftOuterJoin as request is fact driven")

  }

  test(
    """generate valid model for async query with forceDimensionDriven and fields having dimension attribute,
      |filter on fact dim col which is a foreign key and it is in the list of fields,
      |order by dim attribute""".stripMargin) {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Ad Group ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Ad Group ID", "operator": "in", "values": ["1", "2", "3"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Status", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 4)
    assert(model.requestCols.contains(DimColumnInfo("Campaign ID")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser Status")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Ad Group ID")) === true)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true)
    assert(model.hasDimSortBy === true)
    assert(model.isDimDriven === true)
    assert(model.hasFactFilters)
    assert(model.factFilters.size === 2)
    assert(model.factFilters.map(_.field).contains("Advertiser ID"))
    assert(model.factFilters.map(_.field).contains("Ad Group ID"))
    assert(model.dimColumnAliases.contains("Advertiser Status") === true)
    assert(model.dimensionsCandidates.size === 2, s"dimensionsCandidates = ${model.dimensionsCandidates}")
    assert(model.dimensionsCandidates.exists(_.dim.name == "ad_group") === true)
    assert(model.dimensionsCandidates.exists(_.dim.name == "advertiser") === true)
    assert(model.dimSortByMap.contains("Advertiser Status") === true)
    assert(model.dimSortByMap("Advertiser Status") === ASC)

    assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields.exists(_ === "Ad Group ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields}")
    //assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields.exists(_ === "Campaign ID") === true,
    //s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")

    assert(model.dimensionNameToJoinTypeMap("advertiser_dim") == RightOuterJoin, "Should RightOuterJoin as request is dim driven and also async")

  }

  test(
    """generate valid model for async query with fields having dimension attribute,
      |filter on fact dim col which is a foreign key and it is in the list of fields,
      |order by dim attribute""".stripMargin) {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Ad Group ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Ad Group ID", "operator": "in", "values": ["1", "2", "3"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Status", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 4)
    assert(model.requestCols.contains(FactColumnInfo("Campaign ID")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser Status")) === true)
    assert(model.requestCols.contains(FactColumnInfo("Ad Group ID")) === true)
    assert(model.hasDimSortBy === true)
    assert(model.isDimDriven === false)
    assert(model.hasFactFilters)
    assert(model.factFilters.size === 2)
    assert(model.factFilters.map(_.field).contains("Advertiser ID"))
    assert(model.factFilters.map(_.field).contains("Ad Group ID"))
    assert(model.dimColumnAliases.contains("Advertiser Status") === true)
    assert(model.dimensionsCandidates.size === 1, s"dimensionsCandidates = ${model.dimensionsCandidates}")
    assert(model.dimensionsCandidates.exists(_.dim.name == "advertiser") === true)
    assert(model.dimSortByMap.contains("Advertiser Status") === true)
    assert(model.dimSortByMap("Advertiser Status") === ASC)

    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")

    assert(model.dimensionNameToJoinTypeMap("advertiser_dim") == LeftOuterJoin, "Should LeftOuterJoin as request is async")


  }

  test(
    """generate valid model for sync query with fields having dimension attribute,
       filter on fact dim col which is a foreign key and it not in the list of fields,
       order by dim attribute""") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Ad Group ID", "operator": "in", "values": ["1", "2", "3"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Status", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 3)
    assert(model.requestCols.contains(DimColumnInfo("Campaign ID")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser Status")) === true)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true)
    assert(model.hasDimSortBy === true)
    assert(model.isDimDriven)
    assert(model.hasFactFilters)
    assert(model.factFilters.size === 2)
    assert(model.factFilters.map(_.field).contains("Advertiser ID"))
    assert(model.factFilters.map(_.field).contains("Ad Group ID"))
    assert(model.dimColumnAliases.contains("Advertiser Status") === true)
    assert(model.dimensionsCandidates.size === 2, s"dimensionsCandidates = ${model.dimensionsCandidates}")
    assert(model.dimensionsCandidates.exists(_.dim.name == "advertiser") === true)
    assert(model.dimensionsCandidates.exists(_.dim.name == "campaign") === true)
    assert(model.dimSortByMap.contains("Advertiser Status") === true)
    assert(model.dimSortByMap("Advertiser Status") === ASC)

    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")

    assert(model.dimensionNameToJoinTypeMap("advertiser_dim") == LeftOuterJoin, "Should LeftOuterJoin as request is fact driven")

  }

  test(
    """generate valid model for async query with forceDimensionDriven and fields having dimension attribute,
      |filter on fact dim col which is a foreign key and it not in the list of fields,
      |order by dim attribute""".stripMargin) {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Ad Group ID", "operator": "in", "values": ["1", "2", "3"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Status", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 3)
    assert(model.requestCols.contains(DimColumnInfo("Campaign ID")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser Status")) === true)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true)
    assert(model.hasDimSortBy === true)
    assert(model.isDimDriven)
    assert(model.hasFactFilters === true)
    assert(model.factFilters.size === 2)
    assert(model.factFilters.exists(_.field === "Advertiser ID") === true)
    assert(model.factFilters.exists(_.field === "Ad Group ID") === true)
    assert(model.dimColumnAliases.contains("Advertiser Status") === true)
    assert(model.dimensionsCandidates.size === 2, s"dimensionsCandidates = ${model.dimensionsCandidates}")
    assert(model.dimensionsCandidates.exists(_.dim.name == "advertiser") === true)
    assert(model.dimensionsCandidates.exists(_.dim.name == "campaign") === true)
    assert(model.dimSortByMap.contains("Advertiser Status") === true)
    assert(model.dimSortByMap("Advertiser Status") === ASC)

    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")

  }

  test(
    """generate valid model for async query with fields having dimension attribute,
      |filter on fact dim col which is a foreign key and it not in the list of fields,
      |order by dim attribute""".stripMargin) {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Ad Group ID", "operator": "in", "values": ["1", "2", "3"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Status", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 3)
    assert(model.requestCols.contains(FactColumnInfo("Campaign ID")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser Status")) === true)
    assert(model.hasDimSortBy === true)
    assert(model.isDimDriven === false)
    assert(model.hasFactFilters === true)
    assert(model.factFilters.exists(_.field === "Advertiser ID") === true)
    assert(model.factFilters.exists(_.field === "Ad Group ID") === true)
    assert(model.dimColumnAliases.contains("Advertiser Status") === true)
    assert(model.dimensionsCandidates.size === 1, s"dimensionsCandidates = ${model.dimensionsCandidates}")
    assert(model.dimensionsCandidates.exists(_.dim.name == "advertiser") === true)
    assert(model.dimSortByMap.contains("Advertiser Status") === true)
    assert(model.dimSortByMap("Advertiser Status") === ASC)

    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")

  }

  test(
    """generate valid model for sync query with fields having 
      |one dim attribute with ordering and one without ordering, 
      |filter on fact dim col""".stripMargin) {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Campaign Status"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Campaign ID", "operator": "in", "values": ["1", "2", "3"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Status", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 4)
    assert(model.requestCols.contains(DimColumnInfo("Campaign ID")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Campaign Status")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser Status")) === true)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true)
    assert(model.factFilters.size === 2)
    assert(model.factFilters.exists(_.field === "Advertiser ID") === true)
    assert(model.factFilters.exists(_.field === "Campaign ID") === true)
    assert(model.hasDimSortBy === true)
    assert(model.isDimDriven === true)
    assert(model.dimColumnAliases.contains("Advertiser Status") === true)
    assert(model.dimensionsCandidates.size === 2, s"dimensionsCandidates = ${model.dimensionsCandidates}")
    assert(model.dimensionsCandidates.exists(_.dim.name == "campaign") === true)
    assert(model.dimensionsCandidates.exists(_.dim.name == "advertiser") === true)
    assert(model.dimSortByMap.contains("Advertiser Status") === true)
    assert(model.dimSortByMap("Advertiser Status") === ASC)

    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields.exists(_ === "Campaign ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields.exists(_ === "Campaign Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")

  }

  test(
    """generate valid model for async query with forceDimensionDriven and fields having
      |one dim attribute with ordering and one without ordering, 
      |filter on fact dim col""".stripMargin) {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Campaign Status"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Campaign ID", "operator": "in", "values": ["1", "2", "3"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Status", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 4)
    assert(model.requestCols.contains(DimColumnInfo("Campaign ID")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Campaign Status")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser Status")) === true)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true)
    assert(model.factFilters.size === 2)
    assert(model.factFilters.exists(_.field === "Advertiser ID") === true)
    assert(model.factFilters.exists(_.field === "Campaign ID") === true)
    assert(model.hasDimSortBy === true)
    assert(model.isDimDriven === true)
    assert(model.dimColumnAliases.contains("Advertiser Status") === true)
    assert(model.dimensionsCandidates.size === 2, s"dimensionsCandidates = ${model.dimensionsCandidates}")
    assert(model.dimensionsCandidates.exists(_.dim.name == "campaign") === true)
    assert(model.dimensionsCandidates.exists(_.dim.name == "advertiser") === true)
    assert(model.dimSortByMap.contains("Advertiser Status") === true)
    assert(model.dimSortByMap("Advertiser Status") === ASC)

    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields.exists(_ === "Campaign ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields.exists(_ === "Campaign Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")

  }

  test(
    """generate valid model for async query with fields having
      |one dim attribute with ordering and one without ordering,
      |filter on fact dim col""".stripMargin) {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Campaign Status"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Campaign ID", "operator": "in", "values": ["1", "2", "3"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Status", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 4)
    assert(model.requestCols.contains(FactColumnInfo("Campaign ID")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Campaign Status")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser Status")) === true)
    assert(model.isDimDriven === false)
    assert(model.hasDimSortBy === true)
    assert(model.factFilters.size === 2)
    assert(model.factFilters.exists(_.field === "Advertiser ID") === true)
    assert(model.factFilters.exists(_.field === "Campaign ID") === true)
    assert(model.dimColumnAliases.contains("Advertiser Status") === true)
    assert(model.dimensionsCandidates.size === 2, s"dimensionsCandidates = ${model.dimensionsCandidates}")
    assert(model.dimensionsCandidates.exists(_.dim.name == "campaign") === true)
    assert(model.dimensionsCandidates.exists(_.dim.name == "advertiser") === true)
    assert(model.dimSortByMap.contains("Advertiser Status") === true)
    assert(model.dimSortByMap("Advertiser Status") === ASC)

    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields.exists(_ === "Campaign ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields.exists(_ === "Campaign Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")

  }

  test(
    """generate valid model for sync query with fields having dimension attribute,
       filter on dim attribute and it not in the list of fields,
       order by dim attribute"""){
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Ad Group Status", "operator": "in", "values": ["ON"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Status", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 3)
    assert(model.requestCols.contains(DimColumnInfo("Campaign ID")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser Status")) === true)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true)
    assert(model.factFilters.size === 1)
    assert(model.factFilters.exists(_.field === "Advertiser ID") === true)
    assert(model.hasDimSortBy === true)
    assert(model.isDimDriven === true)
    assert(model.dimColumnAliases.contains("Advertiser Status") === true)
    assert(model.dimensionsCandidates.size === 2)
    assert(model.dimensionsCandidates.exists(_.dim.name == "ad_group") === true)
    assert(model.dimensionsCandidates.exists(_.dim.name == "advertiser") === true)
    assert(model.dimSortByMap.contains("Advertiser Status") === true)
    assert(model.dimSortByMap("Advertiser Status") === ASC)

    assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields.exists(_ === "Ad Group ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields}")
    //assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields.exists(_ === "Campaign ID") === true,
      //s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.filters.exists(_.field === "Ad Group Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.filters}")

    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")

  }

  test("generate valid model for async query with forceDimensionDriven and fields having dimension attribute and filter on dim attribute and it not in the list of fields and order by dim attribute") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Ad Group Status", "operator": "in", "values": ["ON"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Status", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 3)
    assert(model.requestCols.contains(DimColumnInfo("Campaign ID")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser Status")) === true)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true)
    assert(model.factFilters.size === 1)
    assert(model.factFilters.exists(_.field === "Advertiser ID") === true)
    assert(model.hasDimSortBy === true)
    assert(model.isDimDriven === true)
    assert(model.dimColumnAliases.contains("Advertiser Status") === true)
    assert(model.dimensionsCandidates.size === 2)
    assert(model.dimensionsCandidates.exists(_.dim.name == "ad_group") === true)
    assert(model.dimensionsCandidates.exists(_.dim.name == "advertiser") === true)
    assert(model.dimSortByMap.contains("Advertiser Status") === true)
    assert(model.dimSortByMap("Advertiser Status") === ASC)

    assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields.exists(_ === "Ad Group ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields}")
    //assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields.exists(_ === "Campaign ID") === true,
    //s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.filters.exists(_.field === "Ad Group Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.filters}")

    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")

    assert(model.dimensionNameToJoinTypeMap("advertiser_dim") == RightOuterJoin, "Should be RightOuterJoin as request is dimDriven")
    assert(model.dimensionNameToJoinTypeMap("ad_group_dim") == RightOuterJoin, "Should be RightOuterJoin as request is dimDriven")

  }

  test(
    """generate valid model for async query with fields having dimension attribute,
      |filter on dim attribute and it not in the list of fields,
      |order by dim attribute""".stripMargin) {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Ad Group Status", "operator": "in", "values": ["ON"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Status", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 3)
    assert(model.requestCols.contains(FactColumnInfo("Campaign ID")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser Status")) === true)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true)
    assert(model.factFilters.size === 1)
    assert(model.factFilters.exists(_.field === "Advertiser ID") === true)
    assert(model.hasDimSortBy === true)
    assert(model.isDimDriven === false)
    assert(model.dimColumnAliases.contains("Advertiser Status") === true)
    assert(model.dimensionsCandidates.size === 2)
    assert(model.dimensionsCandidates.exists(_.dim.name == "ad_group") === true)
    assert(model.dimensionsCandidates.exists(_.dim.name == "advertiser") === true)
    assert(model.dimSortByMap.contains("Advertiser Status") === true)
    assert(model.dimSortByMap("Advertiser Status") === ASC)

    assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields.exists(_ === "Ad Group ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields}")
    //assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields.exists(_ === "Campaign ID") === true,
      //s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.filters.exists(_.field === "Ad Group Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.filters}")

    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")

    assert(model.dimensionNameToJoinTypeMap("advertiser_dim") == InnerJoin, "Should be InnerJoin as request has dim filters")
    assert(model.dimensionNameToJoinTypeMap("ad_group_dim") == InnerJoin, "Should be InnerJoin as request has dim filters")

  }

  test(
    """generate valid model for sync query with fields having two dimension attributes,
      |filter on dim attribute and it not in the list of fields,
      |order by dim attribute""".stripMargin) {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Campaign Status"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Ad Group Status", "operator": "in", "values": ["ON"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Status", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 4)
    assert(model.requestCols.contains(DimColumnInfo("Campaign ID")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Campaign Status")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser Status")) === true)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true)
    assert(model.factFilters.size === 1)
    assert(model.factFilters.exists(_.field === "Advertiser ID") === true)
    assert(model.hasDimSortBy === true)
    assert(model.isDimDriven === true)
    assert(model.dimColumnAliases.contains("Advertiser Status") === true)
    assert(model.dimColumnAliases.contains("Campaign Status") === true)
    assert(model.dimensionsCandidates.size === 3)
    assert(model.dimensionsCandidates.exists(_.dim.name == "ad_group") === true)
    assert(model.dimensionsCandidates.exists(_.dim.name == "campaign") === true)
    assert(model.dimensionsCandidates.exists(_.dim.name == "advertiser") === true)
    assert(model.dimSortByMap.contains("Advertiser Status") === true)
    assert(model.dimSortByMap("Advertiser Status") === ASC)

    assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields.exists(_ === "Ad Group ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields}")

    assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.filters.exists(_.field === "Ad Group Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.filters}")

    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields.exists(_ === "Campaign ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields.exists(_ === "Campaign Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields}")

    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")

    assert(model.dimensionNameToJoinTypeMap("advertiser_dim") == InnerJoin, "Should be InnerJoin as request has dim filters")
    assert(model.dimensionNameToJoinTypeMap("ad_group_dim") == InnerJoin, "Should be InnerJoin as request has dim filters")

  }

  test(
    """generate valid model for async query with forceDimensionDriven and fields having two dimension attributes,
      |filter on dim attribute and it not in the list of fields,
      |order by dim attribute""".stripMargin) {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Campaign Status"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Ad Group Status", "operator": "in", "values": ["ON"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Status", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 4)
    assert(model.requestCols.contains(DimColumnInfo("Campaign ID")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Campaign Status")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser Status")) === true)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true)
    assert(model.factFilters.size === 1)
    assert(model.factFilters.exists(_.field === "Advertiser ID") === true)
    assert(model.hasDimSortBy === true)
    assert(model.isDimDriven === true)
    assert(model.dimColumnAliases.contains("Advertiser Status") === true)
    assert(model.dimColumnAliases.contains("Campaign Status") === true)
    assert(model.dimensionsCandidates.size === 3)
    assert(model.dimensionsCandidates.exists(_.dim.name == "ad_group") === true)
    assert(model.dimensionsCandidates.exists(_.dim.name == "campaign") === true)
    assert(model.dimensionsCandidates.exists(_.dim.name == "advertiser") === true)
    assert(model.dimSortByMap.contains("Advertiser Status") === true)
    assert(model.dimSortByMap("Advertiser Status") === ASC)

    assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields.exists(_ === "Ad Group ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields}")

    assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.filters.exists(_.field === "Ad Group Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.filters}")

    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields.exists(_ === "Campaign ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields.exists(_ === "Campaign Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields}")

    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")

    assert(model.dimensionNameToJoinTypeMap("advertiser_dim") == RightOuterJoin, "Should be RightOuterJoin as request has dim filters")
    assert(model.dimensionNameToJoinTypeMap("ad_group_dim") == RightOuterJoin, "Should be RightOuterJoin as request has dim filters")


  }

  test(
    """generate valid model for async query with fields having two dimension attributes,
       filter on dim attribute and it not in the list of fields,
       order by dim attribute""") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Campaign Status"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Ad Group Status", "operator": "in", "values": ["ON"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Status", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 4)
    assert(model.requestCols.contains(FactColumnInfo("Campaign ID")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Campaign Status")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser Status")) === true)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true)
    assert(model.factFilters.size === 1)
    assert(model.factFilters.exists(_.field === "Advertiser ID") === true)
    assert(model.hasDimSortBy === true)
    assert(model.isDimDriven === false)
    assert(model.dimColumnAliases.contains("Advertiser Status") === true)
    assert(model.dimColumnAliases.contains("Campaign Status") === true)
    assert(model.dimensionsCandidates.size === 3)
    assert(model.dimensionsCandidates.exists(_.dim.name == "ad_group") === true)
    assert(model.dimensionsCandidates.exists(_.dim.name == "campaign") === true)
    assert(model.dimensionsCandidates.exists(_.dim.name == "advertiser") === true)
    assert(model.dimSortByMap.contains("Advertiser Status") === true)
    assert(model.dimSortByMap("Advertiser Status") === ASC)

    assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields.exists(_ === "Ad Group ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields}")

    assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.filters.exists(_.field === "Ad Group Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.filters}")
    assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.filters.exists(_.field === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.filters}")

    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields.exists(_ === "Campaign ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields.exists(_ === "Campaign Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields}")

    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.filters.exists(_.field === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.filters}")
    
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")

    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.filters.exists(_.field === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.filters}")

    assert(model.bestCandidates.get.facts.size == 1)
    assert(model.bestCandidates.get.fkCols("ad_group_id"))
    assert(model.bestCandidates.get.fkCols("campaign_id"))
    assert(model.bestCandidates.get.fkCols("advertiser_id"))
    assert(model.bestCandidates.get.dimColMapping.contains("ad_group_id"))
    assert(model.bestCandidates.get.dimColMapping.contains("campaign_id"))
    assert(model.bestCandidates.get.dimColMapping.contains("advertiser_id"))
    assert(model.bestCandidates.get.factColMapping.contains("impressions"))

    assert(model.dimensionNameToJoinTypeMap("advertiser_dim") == InnerJoin, "Should be InnerJoin as request has dim filters")
    assert(model.dimensionNameToJoinTypeMap("ad_group_dim") == InnerJoin, "Should be InnerJoin as request has dim filters")

  }

  test(
    """generate valid model for sync query with fields having two dimension attributes,
       filter on dim attribute and it not in the list of fields,
       order by fact""") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Campaign Status"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Ad Group Status", "operator": "in", "values": ["ON"]}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 4)
    assert(model.requestCols.contains(DimColumnInfo("Campaign ID")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Campaign Status")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser Status")) === true)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true)
    assert(model.factFilters.size === 1)
    assert(model.factFilters.exists(_.field === "Advertiser ID") === true)
    assert(!model.hasDimSortBy)
    assert(!model.isDimDriven)
    assert(model.hasFactSortBy)
    assert(model.dimColumnAliases.contains("Advertiser Status") === true)
    assert(model.dimColumnAliases.contains("Campaign Status") === true)
    assert(model.dimensionsCandidates.size === 3)
    assert(model.dimensionsCandidates.exists(_.dim.name == "ad_group") === true)
    assert(model.dimensionsCandidates.exists(_.dim.name == "campaign") === true)
    assert(model.dimensionsCandidates.exists(_.dim.name == "advertiser") === true)
    assert(model.factSortByMap.contains("Impressions") === true)
    assert(model.factSortByMap("Impressions") === ASC)

    assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields.exists(_ === "Ad Group ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields}")

    assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.filters.exists(_.field === "Ad Group Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.filters}")
    assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.filters.exists(_.field === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.filters}")

    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields.exists(_ === "Campaign ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields.exists(_ === "Campaign Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields}")

    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.filters.exists(_.field === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.filters}")

    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields.exists(_ === "Advertiser Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields}")

    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.filters.exists(_.field === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.filters}")

    assert(model.bestCandidates.get.facts.size == 1)
    assert(model.bestCandidates.get.fkCols("ad_group_id"))
    assert(model.bestCandidates.get.fkCols("campaign_id"))
    assert(model.bestCandidates.get.fkCols("advertiser_id"))
    assert(model.bestCandidates.get.dimColMapping.contains("ad_group_id"))
    assert(model.bestCandidates.get.dimColMapping.contains("campaign_id"))
    assert(model.bestCandidates.get.dimColMapping.contains("advertiser_id"))
    assert(model.bestCandidates.get.factColMapping.contains("impressions"))

    assert(model.dimensionNameToJoinTypeMap("advertiser_dim") == InnerJoin, "Should be InnerJoin as request has dim filters")
    assert(model.dimensionNameToJoinTypeMap("ad_group_dim") == InnerJoin, "Should be InnerJoin as request has dim filters")

  }

  test("""generate valid model with filter on field with static mapping and not in fields list""") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Campaign Status"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Pricing Type", "operator": "in", "values": ["CPE", "CPA"]}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.factFilters.size === 2)
    assert(model.factFilters.exists(_.field === "Advertiser ID") === true)
    assert(model.factFilters.exists(_.field === "Pricing Type") === true)
    assert(model.factFilters.find(_.field === "Pricing Type").get.asInstanceOf[InFilter].values === List("-10", "2"))

    assert(model.dimensionNameToJoinTypeMap("advertiser_dim") == LeftOuterJoin, "Should LeftOuterJoin as request fact driven")
    assert(model.dimensionNameToJoinTypeMap("campaign_dim") == LeftOuterJoin, "Should LeftOuterJoin as request fact driven")

  }

  test("""generate valid model with filter on field with static mapping and in fields list""") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Campaign Status"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Pricing Type", "operator": "in", "values": ["CPE", "CPA"]}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.factFilters.size === 2)
    assert(model.factFilters.exists(_.field === "Advertiser ID") === true)
    assert(model.factFilters.exists(_.field === "Pricing Type") === true)
    assert(model.factFilters.find(_.field === "Pricing Type").get.asInstanceOf[InFilter].values === List("-10", "2"))
    assert(model.dimensionNameToJoinTypeMap("campaign_dim") == LeftOuterJoin, "Should LeftOuterJoin as request fact driven")

  }

  test("""create model should fail when filtering with unsupported operation on fact col""") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Campaign Status"},
                              {"field": "Impressions"},
                              {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Clicks", "operator": "between", "from": "0", "to": "300"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isFailure, "request model should fail")
    res.failed.get.getMessage should startWith ("requirement failed: Unsupported filter operation : cube=publicFact, col=Clicks, operation=Between")
  }

  test("""create model should fail when filtering with unsupported operation on fact dim col""") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Campaign Status"},
                              {"field": "Impressions"},
                              {"field": "Source"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Source", "operator": "between", "from": "0", "to": "3"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isFailure, "request model should fail")
    res.failed.get.getMessage should startWith ("requirement failed: Unsupported filter operation : cube=publicFact, col=Source, operation=Between")
  }

  test("""create model should fail when filtering with unsupported operation on dimension dim col""") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Campaign Status"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Campaign Status", "operator": "between", "from": "ON", "to": "OFF"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isFailure, "request model should fail")
    res.failed.get.getMessage should startWith ("requirement failed: Unsupported filter operation : dimension=campaign, col=Campaign Status, operation=Between")
  }

  test("""create model should fail when missing required field""") {
    val jsonString = s"""{
                          "cube": "publicFact2",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Keyword Status"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Keyword Status", "operator": "in", "values": ["ON"]}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isFailure, "request model should fail")
    res.failed.get.getMessage should startWith ("requirement failed: Missing required field: cube=publicFact2, field=Keyword ID")
  }

  test("""create model should succeed when required field present""") {
    val jsonString = s"""{
                          "cube": "publicFact2",
                          "selectFields": [
                              {"field": "Keyword ID"},
                              {"field": "Campaign ID"},
                              {"field": "Keyword Status"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Keyword Status", "operator": "in", "values": ["ON"]}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("request model should succeed"))
  }

  test("""create model should fail when request dates out of window""") {
    val jsonString = s"""{
                          "cube": "publicFact2",
                          "selectFields": [
                              {"field": "Keyword ID"},
                              {"field": "Campaign ID"},
                              {"field": "Keyword Status"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "2016-09-01", "to": "2016-09-21"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isFailure, res.errorMessage("Should be : Max days window exceeded expected=8, actual=20 for cube/fact=publicFact2"))
  }

  test("""create model should fail when missing required filter""") {
    val jsonString = s"""{
                          "cube": "publicFact3",
                          "selectFields": [
                              {"field": "Keyword ID"},
                              {"field": "Campaign ID"},
                              {"field": "Keyword Status"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Keyword Status", "operator": "in", "values": ["ON"]}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isFailure, "request model should fail")
    res.failed.get.getMessage should startWith ("requirement failed: Missing required filter: cube=publicFact3, field=Ad Group ID")
  }

  test("""create model should succeed when required filter present""") {
    val jsonString = s"""{
                          "cube": "publicFact3",
                          "selectFields": [
                              {"field": "Keyword ID"},
                              {"field": "Campaign ID"},
                              {"field": "Keyword Status"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Ad Group ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Keyword Status", "operator": "in", "values": ["ON"]}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("request model should succeed"))
  }

  test("""create model should fail when missing dependent field""") {
    val jsonString = s"""{
                          "cube": "publicFact2",
                          "selectFields": [
                              {"field": "Keyword ID"},
                              {"field": "Campaign ID"},
                              {"field": "Keyword Status"},
                              {"field": "Destination URL"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Keyword Status", "operator": "in", "values": ["ON"]}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isFailure, "request model should fail")
    res.failed.get.getMessage should startWith ("requirement failed: Missing dependent column : cube=publicFact2, field=Destination URL, depensOnColumn=Ad ID")
  }

  test("""create model should succeed when all dependent columns present""") {
    val jsonString = s"""{
                          "cube": "publicFact2",
                          "selectFields": [
                              {"field": "Keyword ID"},
                              {"field": "Ad ID"},
                              {"field": "Campaign ID"},
                              {"field": "Keyword Status"},
                              {"field": "Destination URL"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Keyword Status", "operator": "in", "values": ["ON"]}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("request model should succeed"))
  }

  test("generate valid model with forced filter in filter list and not in fields list") {
    val jsonString = s"""{
                          "cube": "publicFact2",
                          "selectFields": [
                              {"field": "Keyword ID"},
                              {"field": "Ad ID"},
                              {"field": "Campaign ID"},
                              {"field": "Keyword Status"},
                              {"field": "Destination URL"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Keyword Status", "operator": "in", "values": ["ON"]}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry(Set(EqualityFilter("Source", "2", isForceFilter = true)))
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("request model should succeed"))
    val model = res.toOption.get
    assert(!model.bestCandidates.get.requestCols("stats_source"))
    assert(model.bestCandidates.get.facts.head._2.filterCols("stats_source"))
    assert(res.get.dimensionNameToJoinTypeMap("keyword_dim") == InnerJoin, "Should InnerJoin as request is filtering on dim")

  }

  test("generate valid model for dim driven query with dim filters") {
   val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Advertiser Status", "operator": "in", "values": ["ON"]}
                          ],
                          "sortBy": [
                              {"field": "Campaign ID", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 3)
    assert(model.requestCols.contains(DimColumnInfo("Campaign ID")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Advertiser ID")) === true)
    assert(model.requestCols.contains(FactColumnInfo("Impressions")) === true)
    assert(model.factFilters.size === 1)
    assert(model.factFilters.exists(_.field === "Advertiser ID") === true)
    assert(model.hasDimSortBy)
    assert(model.isDimDriven)
    assert(!model.hasFactSortBy)
    assert(model.dimColumnAliases.contains("Campaign ID") === true)
    assert(model.dimensionsCandidates.size === 2)
    assert(model.dimensionsCandidates.exists(_.dim.name == "advertiser") === true)
    assert(model.dimensionsCandidates.exists(_.dim.name == "campaign") === true)
    assert(model.dimSortByMap.contains("Campaign ID") === true)
    assert(model.dimSortByMap("Campaign ID") === ASC)

    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields.exists(_ === "Campaign ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "campaign").get.filters.exists(_.field === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "campaign").get.filters}")

    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields === Set("Advertiser ID"))
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.filters.exists(_.field === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.filters}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.filters.exists(_.field === "Advertiser Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.filters}")

    assert(res.get.dimensionNameToJoinTypeMap("advertiser_dim") == InnerJoin, "Should InnerJoin as request is filtering on dim")
  }

  test("Sorting on the on driving dimension should set the correct flag") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Campaign Name"},
                              {"field": "Ad Group ID"},
                              {"field": "Campaign Status"},
                              {"field": "Ad Group Name"},
                              {"field": "Advertiser Name"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Campaign Name", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.hasNonDrivingDimSortOrFilter,"Failed to recognize the case of sorting on non driving dimension")
    assert(model.dimensionsCandidates.take(2).forall(!_.isDrivingDimension))
    assert(model.dimensionsCandidates.last.isDrivingDimension)

    assert(model.dimensionNameToJoinTypeMap("advertiser_dim") == RightOuterJoin, "Should be RightOuterJoin as request is dim sort")
    assert(model.dimensionNameToJoinTypeMap("campaign_dim") == RightOuterJoin, "Should be RightOuterJoin as request is dim sort")
    assert(model.dimensionNameToJoinTypeMap("ad_group_dim") == RightOuterJoin, "Should be RightOuterJoin as request is dim sort")

  }

  test("Filtering on the on driving dimension should set the correct flag") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Campaign Name"},
                              {"field": "Ad Group ID"},
                              {"field": "Campaign Status"},
                              {"field": "Ad Group Name"},
                              {"field": "Advertiser Name"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Campaign Name", "operator": "LiKe", "value": "CapsLockMesSeDup"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.hasNonDrivingDimSortOrFilter,"Failed to recognize the case of sorting and filtering on non driving dimension")
    assert(model.dimensionsCandidates.take(2).forall(!_.isDrivingDimension))
    assert(model.dimensionsCandidates.last.isDrivingDimension)

    assert(model.dimensionNameToJoinTypeMap("advertiser_dim") == RightOuterJoin, "Should be RightOuterJoin as request is fact driven and has dim filtering")
    assert(model.dimensionNameToJoinTypeMap("campaign_dim") == RightOuterJoin, "Should be RightOuterJoin as request is fact driven and has dim filtering")
    assert(model.dimensionNameToJoinTypeMap("ad_group_dim") == RightOuterJoin, "Should be RightOuterJoin as request is fact driven and has dim filtering")

  }

  test("generate valid model for dim driven query with dim filters and no fact cols or filters") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Ad Group ID"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Advertiser Status", "operator": "in", "values": ["ON"]}
                          ],
                          "sortBy": [
                              {"field": "Campaign ID", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.requestCols.size === 2)
    assert(model.requestCols.contains(DimColumnInfo("Campaign ID")) === true)
    assert(model.requestCols.contains(DimColumnInfo("Ad Group ID")) === true)
    assert(model.factFilters.size === 0, model.factFilters)
    assert(model.hasDimSortBy)
    assert(model.isDimDriven)
    assert(!model.hasFactSortBy)
    assert(model.dimColumnAliases.contains("Campaign ID") === true)
    assert(model.dimensionsCandidates.size === 2)
    assert(model.dimensionsCandidates.exists(_.dim.name == "advertiser") === true)
    assert(model.dimensionsCandidates.exists(_.dim.name == "ad_group") === true)
    assert(model.dimSortByMap.contains("Campaign ID") === true)
    assert(model.dimSortByMap("Campaign ID") === ASC)

    assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields.exists(_ === "Ad Group ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields.exists(_ === "Campaign ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields.exists(_ === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.fields}")
    assert(model.dimensionsCandidates.find(_.dim.name == "ad_group").get.filters.exists(_.field === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "ad_group").get.filters}")

    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.fields === Set("Advertiser ID"))
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.filters.exists(_.field === "Advertiser ID") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.filters}")
    assert(model.dimensionsCandidates.find(_.dim.name == "advertiser").get.filters.exists(_.field === "Advertiser Status") === true,
      s"${model.dimensionsCandidates.find(_.dim.name == "advertiser").get.filters}")
    assert(model.bestCandidates.isEmpty)

  }
  
  test("generate valid model for fact driven query with only fact fields, and filters") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Ad Group ID"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.isFactDriven)
  }

  test("generate valid model with request join cols correctly populated") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Ad Group Name"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.bestCandidates.get.requestJoinCols("ad_group_id"))
  }

  test("""generate valid model with injected dimension when no direct relationship when dim driven""") {
    val jsonString = s"""{
                          "cube": "publicFact2",
                          "selectFields": [
                              {"field": "Keyword ID"},
                              {"field": "Ad ID"},
                              {"field": "Campaign ID"},
                              {"field": "Keyword Status"},
                              {"field": "Destination URL"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Keyword Status", "operator": "in", "values": ["ON"]}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "forceDimensionDriven": true
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry(Set(EqualityFilter("Source", "2", isForceFilter = true)))
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("request model should succeed"))
    val model = res.toOption.get
    assert(!model.bestCandidates.get.requestCols("stats_source"))
    assert(model.bestCandidates.get.facts.head._2.filterCols("stats_source"))
    assert(!model.hasLowCardinalityDimFilters)
  }

  test("""should succeed even if the number of 'in' items exceeds 999""") {
    val jsonString = s"""{
                         	"selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Ad ID"},
                            {"field": "Campaign ID"},
                            {"field": "Keyword Status"},
                            {"field": "Destination URL"},
                            {"field": "Impressions"}
                           ],
                         	"cube": "publicFact2",
                         	"filterExpressions": [{
                         		"operator": "=",
                         		"field": "Day",
                         		"value": "2016-06-16"
                         	}, {
                         		"operator": "=",
                         		"field": "Advertiser ID",
                         		"value": 949677
                         	}, {
                         		"operator": "IN",
                         		"field": "Ad ID",
                         		"values": ["29333583444", "29333583445", "29351618540", "29352996619", "29355770791", "29372881270", "29372881760", "29382857005", "29585532124", "29604923679", "29629901673", "29629901674", "30003300044", "30095597936", "30129731358", "30146096561", "30162237869", "30162237871", "30162237903", "30162237950", "30226618247", "30245115008", "30309905311", "30327736150", "30327736151", "30327736152", "30327736153", "30327736154", "30327736155", "30327736156", "30327736157", "30327736158", "30327736159", "30327736160", "30327736161", "30327736162", "30327736163", "30327736164", "30337665479", "30344636496", "30344636500", "30345858321", "30345858322", "30345858323", "30345858324", "30345858325", "30345858326", "30345858327", "30345858328", "30345858329", "30345858330", "30345858331", "30345858332", "30345858333", "30345858334", "30345858335", "30345858336", "30345858337", "30345858338", "30345858339", "30345858340", "30345858341", "30345858342", "30345858343", "30345858344", "30345858345", "30345858346", "30345858347", "30345858348", "30345858349", "30345858350", "30345858351", "30345858352", "30345858353", "30345858354", "30345858355", "30345858356", "30345858357", "30345858358", "30345858359", "30568554384", "30568554385", "30568555263", "30568555265", "30586104723", "30586104732", "30639339447", "30666163004", "30666163005", "30666163008", "30666163010", "30666163011", "30666163014", "30666163015", "30666163016", "30666163017", "30666163018", "31490470849", "31490470829", "31490470850", "31490470840", "31490470830", "31490470845", "31490470833", "31490470838", "31490470848", "31490470839", "31490470846", "31490470847", "31490470832", "31490470828", "31490470834", "31490470844", "31519275589", "31490470831", "31519275593", "31490470836", "31490470842", "31490470853", "31490470857", "31519275582", "31519275585", "31519275588", "31519275584", "31519275583", "31519275592", "31490470835", "31490470854", "31490470851", "31519092114", "31519092115", "31519092116", "31519275590", "31519275591", "31519275596", "31519275595", "31490470843", "31490470855", "31490470856", "31519275586", "31519275587", "31519275594", "31552369339", "31552369354", "31552369356", "31552352103", "31552352107", "31552352113", "31552369337", "31552369338", "31552369342", "31552369343", "31552369346", "31552369347", "31552369355", "31552352100", "31552352101", "31552352117", "31552352115", "31552352119", "31552352116", "31552369341", "31552369345", "31552369336", "31552369348", "31552369350", "31552352105", "31552352106", "31552352111", "31552352102", "31552352112", "31552352114", "31552369352", "31552369351", "31552369344", "31552369340", "31552369349", "31552369353", "31552352109", "31552352118", "31552352120", "31552352104", "31552352108", "31552352110", "31573953085", "31573953089", "31573953091", "31573953086", "31573953093", "31573953094", "31573953092", "31573953099", "31573953087", "31573953088", "31573953097", "31573953100", "31573953084", "31573953090", "31573953098", "31602255625", "31602255631", "31602255632", "31602255623", "31602255626", "31602255627", "31602255624", "31602255629", "31602255634", "31602255621", "31602255622", "31602255628", "31602255620", "31602255630", "31602255633", "31677244575", "31677244577", "31677244571", "31677244570", "31677244581", "31677244582", "31677244569", "31677244576", "31677244578", "31677244568", "31677244574", "31677244580", "31678065559", "31678065566", "31678065570", "31678065561", "31678065564", "31678065567", "31678065562", "31678065563", "31678065568", "31678065556", "31678065558", "31678065560", "31677244572", "31677244579", "31677244573", "31678065557", "31678065565", "31678065569", "31687619610", "31687619604", "31687619611", "31687619605", "31687619607", "31687619609", "31687619606", "31687619608", "31687619612", "31696877644", "31696877645", "31696877647", "31696877646", "31696877650", "31696877652", "31696877648", "31696877649", "31696877651", "31759441468", "31759441477", "31759441465", "31759441467", "31759441469", "31759441476", "31759334835", "31759334836", "31759334840", "31759334824", "31759334829", "31759334837", "31759334826", "31759334832", "31759334839", "31759334821", "31759334822", "31759334827", "31759441471", "31759441478", "31759441479", "31759441473", "31759441474", "31759441475", "31759334825", "31759334828", "31759441466", "31759441470", "31759441472", "31759334820", "31759334823", "31759334834", "31759334830", "31759334831", "31759334833", "31759334838", "31835462136", "31835462142", "31835462131", "31835462149", "31835462137", "31835462148", "31835462145", "31835462135", "31835462130", "31835462150", "31835462139", "31835462144", "31835462155", "31835462132", "31835462141", "31835462152", "31835462134", "31835462143", "31835462147", "31835462133", "31835462146", "31835462151", "31835462153", "31835462156", "31835462154", "31835462140", "31835462129", "31835462138", "31841054888", "31841054893", "31841054894", "31841054887", "31841054890", "31841054899", "31841054896", "31841054897", "31841054898", "31841054889", "31841054895", "31841054901", "31841054886", "31841054900", "31841054905", "31841054902", "31841054906", "31841054903", "31841054891", "31841054892", "31841054904", "31853237666", "31853265235", "31853268228", "31853243080", "31853266385", "31853265236", "31853255750", "31853245881", "31853244210", "31853245882", "31927280095", "31927280107", "31927280090", "31927280103", "31927280100", "31927280114", "31927280098", "31927280104", "31927280112", "31927280108", "31927280113", "31927280087", "31927280089", "31927280110", "31927280093", "31927280097", "31927280099", "31927280105", "31927280106", "31927280115", "31927280101", "31927280102", "31927280109", "31927280092", "31927280094", "31927280096", "31927280088", "31927280091", "31927280111", "32001804755", "32001997279", "32002043019", "32002028090", "32002028091", "32002017361", "32001983765", "32002010123", "32001990861", "32002010127", "32001991880", "32002010130", "32001978896", "32002010131", "32002043030", "32002028102", "32001993741", "32002010134", "32001983770", "32001993742", "32001978903", "32001978904", "32001991885", "32001990869", "32001993747", "32002017383", "32001983776", "32001991889", "32001995542", "32002017387", "32002028111", "32001976985", "32001993752", "32001990872", "32001995546", "32001978913", "32001995549", "32002010149", "32001983788", "32001991898", "32001983792", "32001995553", "32002017401", "32001990881", "32002028117", "32001993760", "32001995555", "32001990883", "32002010152", "32001997312", "32001983794", "32002010153", "32002017404", "32001995556", "32002028120", "32020755734", "32020597846", "32020502583", "32020486891", "32020502584", "32020486892", "32020502585", "32021726430", "32032400382", "32032405395", "32032576147", "32032604314", "32032624545", "32033013856", "32034643984", "32034757570", "32034755449", "32034760392", "32034760393", "32034770196", "32034758208", "32034768358", "32034758209", "32034762460", "32034770206", "32034763768", "32034773186", "32034772108", "32034759636", "32034754987", "32034770507", "32034770508", "32034763769", "32034770526", "32034763770", "32034754988", "32034769690", "32034770827", "32034772413", "32034754989", "32034758510", "32034768735", "32034770828", "32034775237", "32034769691", "32034763771", "32034770829", "32034758511", "32034771171", "32034758512", "32034762461", "32034763772", "32034773528", "32034763773", "32034769692", "32034773616", "32034763774", "32034762462", "32034763030", "32035130603", "32036749948", "32036761247", "32036756208", "32038003770", "32038172055", "32038051295", "32038810688", "32038880335", "32039006092", "32038992457", "32038986756", "32039408566", "32039408568", "32039408583", "32039622303", "32039623260", "32039747582", "32041356794", "32041369151", "32041356795", "32041555799", "32041555800", "32041544877", "32041536892", "32041546626", "32041548349", "32041551760", "32041554422", "32041555749", "32041559214", "32041559513", "32041559546", "32041563236", "32041560598", "32041563238", "32041565055", "32041566114", "32041567252", "32041587445", "32041589933", "32041609310", "32041622930", "32041631241", "32041633042", "32041634369", "32041635571", "32041637280", "32041624771", "32041628943", "32041633053", "32041635573", "32041635574", "32041635575", "32041642369", "32041642370", "32041643501", "32042923235", "32042926628", "32042933609", "32042934536", "32042937271", "32042937275", "32042967074", "32042972050", "32042972083", "32045268847", "32045268849", "32045271964", "32088230329", "32088225788", "32088226271", "32088231215", "32088227853", "32088234352", "32088259373", "32088274179", "32088228619", "32088259506", "32102970845", "32106199205", "32106186551", "32106185956", "32106199209", "32106197982", "32106198342", "32106212056", "32106184115", "32106214490", "32106218436", "32106180507", "32106222300", "32106231721", "32106228538", "32106184702", "32106199753", "32106184703", "32106199748", "32106199756", "32106199781", "32106214580", "32106214581", "32106214645", "32106225112", "32106226136", "32106226176", "32106227088", "32106230374", "32106230376", "32106231207", "32106232708", "32106232710", "32106233165", "32106369088", "32106360106", "32106332280", "32106375234", "32106375523", "32106385819", "32106390283", "32106350311", "32106406713", "32106412425", "32106424087", "32106412852", "32108333726", "32108332753", "32108416062", "32108336214", "32108416063", "32108416064", "32108414169", "32108351822", "32108352133", "32108333727", "32108334940", "32108416065", "32108353857", "32108333728", "32108351823", "32108414170", "32108320087", "32108353858", "32108414171", "32114018702", "32114028023", "32114020147", "32114023044", "32114025352", "32114020148", "32114030241", "32114028024", "32114014352", "32114020149", "32114028025", "32114017703", "32114017704", "32114023046", "32114028026", "32118736520", "32118733775", "32118733776", "32118736521", "32154156697", "32154159453", "32154159462", "32154166337", "32154201060", "32154157550", "32154156728", "32154162430", "32154200085", "32154156729", "32154156730", "32154198258", "32154200082", "32154198259", "32154162414", "32154200083", "32154162415", "32154198260", "32154201058", "32154156727", "32154160665", "32154198261", "32154201059", "32154166335", "32154159505", "32154160666", "32154162416", "32154158692", "32154199064", "32154158693", "32154166336", "32154160667", "32154162417", "32154306547", "32154306548", "32154302574", "32154304076", "32154307048", "32154302575", "32154302576", "32154268866", "32154304078", "32154307049", "32154303144", "32158151878", "32158166034", "32158169329", "32158137895", "32158175011", "32158142767", "32158138776", "32158151879", "32158137896", "32158170645", "32158146464", "32158168173", "32158170646", "32158170647", "32158137897", "32158167701", "32158146465", "32158170648", "32158170649", "32158151880", "32158151881", "32158138777", "32158169330", "32158146466", "32158142768", "32158167738", "32158166073", "32158181185", "32158168520", "32158146510", "32158146511", "32158165236", "32158138834", "32158146512", "32158138835", "32158169622", "32158146513", "32158165237", "32158183513", "32158175046", "32158175047", "32158167819", "32158169623", "32158181186", "32158169624", "32158166074", "32158165238", "32158169625", "32158181187", "32158175048", "32158165239", "32158169626", "32158183514", "32158142856", "32158183515", "32158183516", "32158165240", "32158183517", "32158175049", "32158169627", "32158181188", "32158181189", "32158183536", "32158166075", "32158137926", "32158175050", "32158146515", "32158175051", "32158181190", "32158142857", "32158183537", "32158167820", "32158166076", "32158183538", "32158175052", "32158168521", "32158137927", "32158138836", "32158138837", "32158142858", "32158137928", "32158137929", "32158142859", "32158181191", "32158146516", "32158168522", "32158181192", "32158167821", "32158146517", "32158181193", "32158169628", "32158146518", "32158166077", "32158146532", "32158142860", "32158165241", "32158181194", "32158165242", "32158169629", "32158166078", "32158175053", "32158142861", "32158166079", "32158166080", "32158138838", "32158166081", "32158175054", "32158166082", "32158175055", "32158137930", "32158138839", "32158138840", "32158169630", "32158181195", "32158146533", "32158165243", "32158168523", "32158168524", "32158169631", "32158138841", "32158168525", "32158166083", "32158166084", "32158138842", "32158169632", "32158166085", "32158175057", "32158165244", "32158137931", "32158137932", "32158175058", "32158181196", "32158146534", "32158137933", "32158168526", "32158142862", "32158137934", "32158181197", "32158138843", "32158165245", "32158137935", "32158165246", "32158181198", "32158137936", "32158165247", "32158146535", "32158175059", "32158137937", "32158137938", "32158146536", "32158137939", "32158183539", "32158175060", "32158175061", "32158168527", "32158168528", "32158169757", "32158181468", "32158168627", "32158168629", "32158175168", "32158186099", "32158168630", "32158186101", "32158146656", "32158169761", "32158146657", "32158165349", "32158187003", "32158187004", "32158165351", "32158146658", "32158169764", "32158186102", "32158169765", "32158146660", "32158169766", "32158169767", "32158181469", "32158169768", "32158187006", "32158167900", "32158186104", "32158183736", "32158186105", "32158168631", "32158186106", "32158166172", "32158186107", "32158183738", "32158187007", "32158166173", "32158165353", "32158165355", "32158175173", "32158167901", "32158167902", "32158181470", "32158146662", "32158181471", "32158169770", "32158183740", "32158186108", "32158166174", "32158183741", "32158175174", "32158146663", "32158168632", "32158187011", "32158181472", "32158187012", "32158166176", "32158181473", "32158146665", "32158183743", "32158138993", "32158187014", "32158146666", "32158187015", "32158166178", "32158167904", "32158187016", "32158165357", "32158146667", "32158166179", "32158183745", "32158165358", "32158167906", "32158146668", "32158165359", "32158146669", "32158175175", "32158187018", "32158166181", "32158186113", "32158167907", "32158168634", "32158166182", "32158186114", "32158186115", "32158165363", "32158181474", "32158187020", "32158146671", "32158187021", "32158175177", "32158138994", "32158187022", "32158165365", "32158166184", "32158146672", "32158187023", "32158181475", "32158168636", "32158146673", "32158146674", "32158186118", "32158146675", "32158815490", "32158812702", "32158816336", "32158812742", "32158809961", "32158820128", "32158819115", "32158816421", "32158816438", "32158814543", "32158822058", "32158813647", "32158817512", "32158820248", "32158813684", "32158815741", "32158811822", "32158823020", "32158822209", "32158823061", "32158895718", "32158891898", "32158898432", "32158897666", "32158893890", "32158893899", "32158900393", "32158896626", "32158898512", "32158897765", "32158893960", "32158899554", "32158894892", "32158897841", "32158901506", "32158897872", "32158896759", "32158901530", "32158905048", "32158900630", "32158900639", "32158899679", "32158903297", "32158907011", "32158904227", "32158901686", "32158901703", "32158901712", "32158908108", "32158907051", "32158907061", "32158908173", "32158906183", "32158903465", "32158904352", "32158902712", "32158905317", "32158901821", "32158907145", "32158909031", "32158908274", "32158903595", "32158909140", "32158898998", "32158909167", "32158906305", "32158911053", "32158902918", "32158900989", "32158903695", "32158955600", "32158960406", "32158954852", "32158953847", "32158960424", "32158964076", "32158954899", "32158954900", "32158954901", "32158963187", "32158957790", "32158961475", "32158961492", "32158965164", "32158958575", "32158962351", "32158957820", "32158962376", "32158960644", "32158961544", "32158964279", "32158960662", "32158967084", "32158965281", "32158962486", "32158957896", "32158955889", "32158962503", "32158958745", "32158958746", "32158957954", "32158966150", "32158965388", "32158965398", "32158961645", "32158958797", "32158960807", "32158959731", "32158965433", "32158958847", "32158958848", "32158962641", "32158969132", "32158965523", "32158966316", "32158958941", "32158963639", "32158966380", "32158967415", "32158964711", "32158966415", "32158968152", "32158966426", "32158959950", "32158965636", "32158965657", "32158964739", "32158970051", "32158968241", "32158972016", "32158970070", "32158964813", "32158963773", "32158963792", "32158970089", "32158969385", "32158964894", "32158973014", "32158964912", "32158969506", "32158971202", "32158970132", "32158967735", "32158967754", "32158965788", "32158964985", "32158970196", "32158967783", "32158966679", "32158971264", "32158975025", "32158966709", "32158963969", "32158967814", "32158970229", "32158966747", "32158969700", "32158976000", "32158976001", "32158969720", "32158965957", "32158969739", "32158973248", "32158976027", "32158973268", "32158966848", "32158967940", "32158975163", "32158966858", "32158967957", "32158967967", "32158972468", "32158972487", "32158970406", "32158976051", "32158974360", "32158973380", "32158968767", "32158977169", "32158968785", "32158978085", "32158976165", "32158977250", "32158971607", "32158973509", "32158968892", "32158977286", "32158970574", "32158976277", "32158976286", "32158971713", "32158970628", "32158970629", "32158975485", "32158972719", "32158977450", "32158977463", "32158972724", "32158974662", "32158974681", "32158972734", "32159019223", "32159013962", "32159009898", "32159017488", "32159019281", "32159017499", "32159018257", "32159016519", "32159017512", "32159021062", "32159016532", "32159012948", "32159016541", "32159020121", "32159017540", "32159021102", "32159020140", "32159017553", "32159023064", "32159015731", "32159020166", "32159019390", "32159019399", "32159018336", "32159018341", "32159026026", "32159021197", "32159022196", "32159026050", "32159018367", "32159022255", "32159026073", "32159020276", "32159016714", "32159023186", "32159022289", "32159025093", "32159021311", "32159021328", "32159023225", "32159017854", "32159025107", "32159019629", "32159016833", "32159015998", "32159026218", "32159025152", "32159020430", "32159026269", "32159027032", "32159020467", "32159019709", "32159026300", "32159027066", "32159019740", "32159021481", "32159019767", "32159019768", "32159023384", "32159021486", "32159024313", "32159029014", "32159020622", "32159021520", "32159026414", "32159023425", "32159025392", "32159019866", "32159029109", "32159020702", "32159022679", "32159028201", "32159025439", "32159024441", "32159028254", "32159021640", "32159019973", "32159018825", "32159026645", "32159026655", "32159024595", "32159026701", "32159024613", "32159030200", "32159023720", "32159020954", "32159026748", "32159024642", "32159026758", "32159029381", "32159030237", "32159025778", "32159026796", "32159027523", "32159025817", "32159024726", "32159030346", "32159024765", "32159023944", "32159026924", "32159030376", "32159021962", "32159033200", "32159025898", "32159027700", "32159035018"]
                         	}]
                         }"""

    val result = ReportingRequest.deserializeAsync(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    assert(result.isSuccess)
  }

  test("Multiple requestModels are returned from factory for DryRun") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Ad Group ID", "operator": "in", "values": ["1", "2", "3"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Status", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    object TestBucketingConfig extends BucketingConfig {
      override def getConfig(cube: String): Option[CubeBucketingConfig] = {
        Some(CubeBucketingConfig.builder()
          .internalBucketPercentage(Map(1 -> 100, 2 -> 0))
          .externalBucketPercentage(Map(1 -> 100, 2 -> 0))
          .dryRunPercentage(Map(1 -> (25, None), 2 -> (100, Some(DruidEngine))))
          .build())
      }
    }

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val bucketParams = new BucketParams(new UserInfo("test-user", false)) // isInternal = false
    val registry = getDefaultRegistry()
    val bucketSelector = new BucketSelector(registry, TestBucketingConfig)
    val requestModelResult = RequestModelFactory.fromBucketSelector(request, bucketParams, registry, bucketSelector)
    assert(requestModelResult.isSuccess)
    assert(requestModelResult.get.model.isInstanceOf[RequestModel])
    assert(requestModelResult.get.dryRunModelTry.isDefined, "Failed to get 2nd Request Model")
    assert(requestModelResult.get.dryRunModelTry.get.isSuccess)
  }

  test("Only one requestModel is returned from factory for NO DryRun") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Ad Group ID", "operator": "in", "values": ["1", "2", "3"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Status", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    object TestBucketingConfig extends BucketingConfig {
      override def getConfig(cube: String): Option[CubeBucketingConfig] = {
        Some(CubeBucketingConfig.builder()
          .internalBucketPercentage(Map(1 -> 100, 2 -> 0))
          .externalBucketPercentage(Map(1 -> 100, 2 -> 0))
          .dryRunPercentage(Map(1 -> (0, None), 2 -> (0, Some(DruidEngine))))
          .build())
      }
    }

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val bucketParams = new BucketParams(new UserInfo("test-user", false)) // isInternal = false
    val registry = getDefaultRegistry()
    val bucketSelector = new BucketSelector(registry, TestBucketingConfig)
    val requestModels = RequestModelFactory.fromBucketSelector(request, bucketParams, registry, bucketSelector)
    assert(requestModels.isSuccess)
    assert(requestModels.get.dryRunModelTry.isDefined == false)
  }

  test("Test for BucketParams") {
    val jsonString = s"""{
                          "cube": "publicFactRev2",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Ad Group ID", "operator": "in", "values": ["1", "2", "3"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Status", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    object TestBucketingConfig extends BucketingConfig {
      override def getConfig(cube: String): Option[CubeBucketingConfig] = {
        Some(CubeBucketingConfig.builder()
          .internalBucketPercentage(Map(1 -> 100, 2 -> 0))
          .externalBucketPercentage(Map(1 -> 100, 2 -> 0))
          .dryRunPercentage(Map(1 -> (0, None), 2 -> (0, Some(DruidEngine))))
          .build())
      }
    }

    val registry = getDefaultRegistry()
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val bucketSelector = new BucketSelector(registry, TestBucketingConfig)

    var bucketParams = new BucketParams(new UserInfo("test-user", false), Some(2), Some(1), Some(DruidEngine)) // isInternal = false
    var requestModels = RequestModelFactory.fromBucketSelector(request, bucketParams, registry, bucketSelector)
    assert(requestModels.isSuccess)
    assert(requestModels.get.dryRunModelTry.isDefined)
    assert(requestModels.get.dryRunModelTry.get.isSuccess) // Test force-DryRun
    assert(requestModels.get.dryRunModelTry.get.get.bestCandidates.get.publicFact.revision == 2) // Test force-DryRun-revision
    assert(requestModels.get.dryRunModelTry.get.get.forceQueryEngine.get.equals(DruidEngine)) // Test forceEngine

    bucketParams = new BucketParams(new UserInfo("test-user", false), Some(1), Some(1), None) // isInternal = false
    requestModels = RequestModelFactory.fromBucketSelector(request, bucketParams, registry, bucketSelector)
    assert(requestModels.isSuccess)
    assert(requestModels.get.dryRunModelTry.get.isSuccess) // Test force-DryRun
    assert(requestModels.get.dryRunModelTry.get.get.bestCandidates.get.publicFact.revision == 2) // There's no cube with revision 1, hence default revision is returned
    assert(requestModels.get.dryRunModelTry.get.get.forceQueryEngine.equals(None)) // ForceEngine is None
  }

  test("Base Request Col Test") {
    val baseRequestCol = new BaseRequestCol("Keyword Value")
    require(baseRequestCol.isKey == false)
    val joinKey = new JoinKeyCol("Keyword ID")
    require(joinKey.isKey == true)
  }

  test("Filtering on the dimension with the like operator should generate valid model") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Campaign Name"},
                              {"field": "Ad Group ID"},
                              {"field": "Campaign Status"},
                              {"field": "Ad Group Name"},
                              {"field": "Advertiser Name"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Campaign Name", "operator": "Like", "value": "CapsLockMesSeDup"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    val model = res.toOption.get
    assert(model.dimFilters.exists(_.operator == LikeFilterOperation))
    assert(model.hasLowCardinalityDimFilters)
  }

  test("create model should fail when incompatible fields are requested") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Device ID"},
                              {"field": "Device Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    res.isFailure shouldBe true
    res.failed.get.getMessage should startWith ("requirement failed: ERROR_CODE:10008 Incompatible columns found in request, Device ID is not compatible with Set(Device Type)")
  }

  test("create model should pass when no incompatible field is requested") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Device ID"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed "))
  }

  test("create model should fail when duplicate field is requested") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Device ID"},
                              {"field": "Device ID"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isFailure, res.errorMessage("Create model succeeded even with duplidate fields "))
  }

  test("create model should fail when duplicate alias is requested") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID",
                               "alias": null},
                              {"field": "Impressions",
                                "alias": "Impr"},
                              {"field": "Clicks",
                               "alias": "Impr"},
                              {"field": "Device ID"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isFailure, res.errorMessage("Create model succeeded even with duplidate fields "))
  }

  test("create model should fail when public col with forbidden schema is requested") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Clicks"},
                              {"field": "Ad Format Name"},
                              {"field": "Spend"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isFailure, res.errorMessage("Create model succeeded even with forbidden schema "))
    res.failed.get.getMessage should startWith ("requirement failed: ERROR_CODE:10007 (Ad Format Name, Spend) can't be used with advertiser schema in publicFact cube")
  }

  test("create model should succeed when public col with non forbidden schema is requested") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Clicks"},
                              {"field": "Network Type"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed with non forbidden schema"))
  }

  test("create model should succeed when using outer filters on requested columns") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Advertiser Status"},
                              {"field": "Impressions"},
                              {"field": "Ad Group Start Date Full"}
                          ],
                          "filterExpressions": [
                             {"operator": "outer", "outerFilters": [
                                  {"field": "Campaign ID", "operator": "isnull"},
                                  {"field": "Advertiser Status", "operator": "=", "value":"ON"}
                                  ]
                             },
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Create model failed "))
  }

  test("create model should fail when using outer filters on non-requested columns") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Advertiser Status"},
                              {"field": "Impressions"},
                              {"field": "Ad Group Start Date Full"}
                          ],
                          "filterExpressions": [
                             {"operator": "outer", "outerFilters": [
                                  {"field": "Advertiser Status", "operator": "=", "value":"ON"},
                                  {"field": "Ad Group ID", "operator": "isnull"}
                                  ]
                             },
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isFailure, res.errorMessage("OuterFilter Ad Group ID is not in selected column list"))
    res.failed.get.getMessage should startWith ("requirement failed: OuterFilter Ad Group ID is not in selected column list")
  }

  test("create model should fail when using or with empty filters") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Advertiser Status"},
                              {"field": "Impressions"},
                              {"field": "Ad Group Start Date Full"}
                          ],
                          "filterExpressions": [
                             {"operator": "or", "filterExpressions": []
                             },
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    intercept[IllegalArgumentException] {
      val request: ReportingRequest = getReportingRequestAsync(jsonString)
    }
  }

  test("create model should fail when using or filters with fact and dim filter combination") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Advertiser Status"},
                              {"field": "Impressions"},
                              {"field": "Ad Group Start Date Full"}
                          ],
                          "filterExpressions": [
                             {"operator": "or", "filterExpressions": [
                                  {"field": "Campaign ID", "operator": "=", "value":"1"},
                                  {"field": "Impressions", "operator": "=", "value":"1"}
                                  ]
                             },
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isFailure)
    res.failed.get.getMessage should startWith ("requirement failed: Or filter cannot have combination of fact and dim filters, factFilters=Some(List(EqualityFilter(Impressions,1,false,false))) dimFilters=Some(List(EqualityFilter(Campaign ID,1,false,false)))")
  }

  test("create model should succeed when using or filters with fact filters combination") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Advertiser Status"},
                              {"field": "Impressions"},
                              {"field": "Ad Group Start Date Full"}
                          ],
                          "filterExpressions": [
                             {"operator": "or", "filterExpressions": [
                                  {"field": "Clicks", "operator": "=", "value":"1"},
                                  {"field": "Impressions", "operator": "=", "value":"1"}
                                  ]
                             },
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess)
    assert(!res.get.orFilterMeta.isEmpty)
    assert(res.get.orFilterMeta.head.isFactFilters == true)
    assert(res.get.orFilterMeta.head.orFliter.filters.size == 2)
    assert(res.get.orFilterMeta.head.orFliter.operator == OrFilterOperation)
    assert(res.get.orFilterMeta.head.orFliter.field == "or")
    assert(res.get.orFilterMeta.head.orFliter.filters(0).operator == EqualityFilterOperation)
    assert(res.get.orFilterMeta.head.orFliter.filters(0).field == "Clicks")
    assert(res.get.orFilterMeta.head.orFliter.filters(0).asValues == "1")
    assert(res.get.orFilterMeta.head.orFliter.filters(1).operator == EqualityFilterOperation)
    assert(res.get.orFilterMeta.head.orFliter.filters(1).field == "Impressions")
    assert(res.get.orFilterMeta.head.orFliter.filters(1).asValues == "1")
  }

  test("create model should succeed when using or filters with dim filters combination") {
    val jsonString = s"""{
                          "cube": "publicFact",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Advertiser Status"},
                              {"field": "Impressions"},
                              {"field": "Ad Group Start Date Full"}
                          ],
                          "filterExpressions": [
                             {"operator": "or", "filterExpressions": [
                                  {"field": "Campaign ID", "operator": "=", "value":"1"},
                                  {"field": "Advertiser Status", "operator": "=", "value":"ON"}
                                  ]
                             },
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess)
    assert(!res.get.orFilterMeta.isEmpty)
    assert(res.get.orFilterMeta.head.isFactFilters == false)
    assert(res.get.orFilterMeta.head.orFliter.filters.size == 2)
    assert(res.get.orFilterMeta.head.orFliter.operator == OrFilterOperation)
    assert(res.get.orFilterMeta.head.orFliter.field == "or")
    assert(res.get.orFilterMeta.head.orFliter.filters(0).operator == EqualityFilterOperation)
    assert(res.get.orFilterMeta.head.orFliter.filters(0).field == "Campaign ID")
    assert(res.get.orFilterMeta.head.orFliter.filters(0).asValues == "1")
    assert(res.get.orFilterMeta.head.orFliter.filters(1).operator == EqualityFilterOperation)
    assert(res.get.orFilterMeta.head.orFliter.filters(1).field == "Advertiser Status")
    assert(res.get.orFilterMeta.head.orFliter.filters(1).asValues == "ON")
  }

}

