// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.executor.bigquery

import com.google.cloud.bigquery.{BigQuery, BigQueryError, Field, FieldList, FieldValue, FieldValueList, Job, JobInfo, JobStatus, Schema, StandardSQLTypeName, TableResult}
import com.google.cloud.bigquery.FieldValue.Attribute
import com.google.common.collect.ImmutableList
import com.google.cloud.PageImpl
import org.json4s.JObject
import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core._
import com.yahoo.maha.core.bucketing.BucketParams
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.query.bigquery.{BigqueryPartitionColumnRenderer, BigqueryQueryGenerator}
import com.yahoo.maha.core.registry.RegistryBuilder
import com.yahoo.maha.core.request._
import org.mockito.Matchers._
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import scala.util.Try

class BigqueryQueryExecutorTest
  extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with BaseQueryGeneratorTest {

  private var bigqueryQueryExecutor: Option[BigqueryQueryExecutor] = None
  private val queryExecutorContext: QueryExecutorContext = new QueryExecutorContext

  val bigqueryQueryExecutorConfigMock = Mockito.mock(classOf[BigqueryQueryExecutorConfig])
  val bigqueryClientMock = Mockito.mock(classOf[BigQuery])
  val jobMock = Mockito.mock(classOf[Job])
  val jobStatusMock = Mockito.mock(classOf[JobStatus])

  override protected def beforeAll(): Unit = {
    BigqueryQueryGenerator.register(queryGeneratorRegistry, BigqueryPartitionColumnRenderer, Set.empty)
    doReturn(bigqueryClientMock).when(bigqueryQueryExecutorConfigMock).buildBigqueryClient()
    val bigqueryExecutor = new BigqueryQueryExecutor(bigqueryQueryExecutorConfigMock, new NoopExecutionLifecycleListener)
    bigqueryQueryExecutor = Some(bigqueryExecutor)
    bigqueryQueryExecutor.foreach(queryExecutorContext.register(_))
  }

  override protected def beforeEach(): Unit = {
    doReturn(jobMock).when(bigqueryClientMock).create(any(classOf[JobInfo]))
    doReturn(jobMock).when(jobMock).waitFor()
    doReturn(jobStatusMock).when(jobMock).getStatus()
    doReturn(null).when(jobStatusMock).getError()
    doReturn(validMockTableResultData()).when(jobMock).getQueryResults()
  }

  override protected def afterAll(): Unit = {
    bigqueryQueryExecutor.foreach(queryExecutorContext.remove(_))
  }

  override protected[this] def registerFacts(forcedFilters: Set[ForcedFilter], registryBuilder: RegistryBuilder): Unit = {
    registryBuilder.register(pubfact)
  }

  override protected[this] def registerDims(registryBuilder : RegistryBuilder): Unit = {
    registryBuilder.register(advertiser_dim)
    registryBuilder.register(campaign_dim)
    registryBuilder.register(ad_group_dim)
    registryBuilder.register(ad_dim)
    registryBuilder.build()
  }

  private[this] def ad_dim: PublicDimension = {
    val builder : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        import BigqueryExpression._
        Dimension.newDimension("ad_bigquery", BigqueryEngine, LevelFour, Set(AdvertiserSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("title", StrType(), annotations = Set(EscapingRequired, CaseInsensitive))
            , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
            , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
            , DimCol("status", StrType())
            , DimCol("created_date", DateType())
            , DimCol("last_updated", TimestampType())
            , BigqueryDerDimCol("Ad Status", StrType(), BigqueryDerivedExpression("functionIF(status = 'ON', 'ON', 'OFF')"))
            , BigqueryDerDimCol("Ad Date Modified", StrType(),BigqueryDerivedExpression("format_datetime(last_updated, 'YYYY-MM-DD')"), annotations = Set.empty)
            , BigqueryDerDimCol("Ad Date Created", StrType(),BigqueryDerivedExpression("format_datetime(last_updated, 'YYYY-MM-DD')"), annotations = Set.empty)
            , BigqueryPartDimCol("load_time", StrType(10, default="2018"), partitionLevel = FirstPartitionLevel)
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , annotations = Set()
        )
      }
    }

    builder
      .toPublicDimension("ad","ad",
        Set(
          PubCol("id", "Ad ID", InEquality)
          , PubCol("title", "Ad Title", InEqualityLike)
          , PubCol("advertiser_id", "Advertiser ID", InEquality)
          , PubCol("campaign_id", "Campaign ID", InEquality)
          , PubCol("ad_group_id", "Ad Group ID", InEquality)
          , PubCol("Ad Status", "Ad Status", InEquality)
          , PubCol("Ad Date Created", "Ad Date Created", InBetweenEquality)
          , PubCol("created_date", "Ad Creation Date", InBetweenEquality)
          , PubCol("Ad Date Modified", "Ad Date Modified", InBetweenEquality, restrictedSchemas = Set(AdvertiserLowLatencySchema, AdvertiserSchema), dependsOnColumns = Set("Ad Date Modified Timestamp"))
          , PubCol("last_updated", "Ad Date Modified Timestamp", Set.empty)
        ), highCardinalityFilters = Set(NotInFilter("Ad Status", List("DELETED"), isForceFilter = true), InFilter("Ad Status", List("ON"), isForceFilter = true), EqualityFilter("Ad Status", "ON", isForceFilter = true))
      )
  }

  private[this] def ad_group_dim: PublicDimension = {
    val builder : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        import BigqueryExpression._
        Dimension.newDimension("ad_group_bigquery", BigqueryEngine, LevelThree, Set(AdvertiserSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("name", StrType(), annotations = Set(EscapingRequired, CaseInsensitive))
            , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
            , DimCol("status", StrType())
            , DimCol("start_time", StrType(), annotations = Set(EscapingRequired))
            , DimCol("end_time", StrType(), annotations = Set(EscapingRequired))
            , DimCol("created_date", TimestampType())
            , DimCol("last_updated", TimestampType())
            , BigqueryDerDimCol("Ad Group Status", StrType(), BigqueryDerivedExpression("functionIF(status = 'ON', 'ON', 'OFF')"))
            , BigqueryDerDimCol("Ad Group Start Date Full", StrType(), BigqueryDerivedExpression("format_datetime(start_time, 'YYYY-MM-DD HH:mm:ss')"), annotations = Set.empty)
            , BigqueryDerDimCol("Ad Group End Date Full", StrType(),BigqueryDerivedExpression("format_datetime(end_time, 'YYYY-MM-DD HH:mm:ss')"), annotations = Set.empty)
            , BigqueryDerDimCol("Ad Group Start Date", StrType(), BigqueryDerivedExpression("format_datetime(start_time, 'YYYY-MM-DD')"), annotations = Set.empty)
            , BigqueryDerDimCol("Ad Group End Date", StrType(),BigqueryDerivedExpression("format_datetime(end_time, 'YYYY-MM-DD')"), annotations = Set.empty)
            , BigqueryDerDimCol("Ad Group Date Created", StrType(), BigqueryDerivedExpression("format_datetime(created_date, 'YYYY-MM-DD')"), annotations = Set.empty)
            , BigqueryDerDimCol("Ad Group Date Modified", StrType(),BigqueryDerivedExpression("format_datetime(last_updated, 'YYYY-MM-DD')"), annotations = Set.empty)
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , annotations = Set()
        )
      }
    }

    builder
      .toPublicDimension("ad_group","ad_group",
        Set(
          PubCol("id", "Ad Group ID", InEquality)
          , PubCol("name","Ad Group Name", InEqualityLike)
          , PubCol("advertiser_id", "Advertiser ID", InEquality)
          , PubCol("campaign_id", "Campaign ID", InEquality)
          , PubCol("Ad Group Status", "Ad Group Status", InEquality)
          , PubCol("Ad Group Start Date Full", "Ad Group Start Date Full", InEquality)
          , PubCol("Ad Group End Date Full", "Ad Group End Date Full", InEquality)
          , PubCol("Ad Group Start Date", "Ad Group Start Date", InBetweenEquality)
          , PubCol("Ad Group End Date", "Ad Group End Date", InBetweenEquality)
          , PubCol("Ad Group Date Created", "Ad Group Date Created", InBetweenEquality)
          , PubCol("Ad Group Date Modified", "Ad Group Date Modified", InBetweenEquality)
        ), highCardinalityFilters = Set(NotInFilter("Ad Group Status", List("DELETED"), isForceFilter = true), InFilter("Ad Group Status", List("ON"), isForceFilter = true), EqualityFilter("Ad Group Status", "ON", isForceFilter = true))
      )
  }

  def campaign_dim: PublicDimension = {
    val builder : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        import BigqueryExpression._
        Dimension.newDimension("campaign_bigquery", BigqueryEngine, LevelTwo, Set(AdvertiserSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("name", StrType(), annotations = Set(EscapingRequired, CaseInsensitive))
            , DimCol("status", StrType())
            , BigqueryDerDimCol("Campaign Status", StrType(), BigqueryDerivedExpression("functionIF(status = 'ON', 'ON', 'OFF')"))
            , BigqueryPartDimCol("load_time", StrType(10, default="2018"), partitionLevel = FirstPartitionLevel)
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , annotations = Set()
        )
      }
    }

    builder
      .toPublicDimension("campaign","campaign",
        Set(
          PubCol("id", "Campaign ID", InEquality)
          , PubCol("advertiser_id", "Advertiser ID", InEquality)
          , PubCol("name", "Campaign Name", InEquality)
          , PubCol("Campaign Status", "Campaign Status", InNotInEquality)
        ), highCardinalityFilters = Set(NotInFilter("Campaign Status", List("DELETED"), isForceFilter = true), InFilter("Campaign Status", List("ON"), isForceFilter = true), EqualityFilter("Campaign Status", "ON", isForceFilter = true))
      )
  }

  def advertiser_dim: PublicDimension = {
    val builder : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        import BigqueryExpression._
        Dimension.newDimension("advertiser_bigquery", BigqueryEngine, LevelOne, Set(AdvertiserSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("name", StrType())
            , DimCol("status", StrType())
            , BigqueryDerDimCol("Advertiser Status", StrType(), BigqueryDerivedExpression("functionIF(status = 'ON', 'ON', 'OFF')"))
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , schemaColMap = Map(AdvertiserSchema -> "id")
          , annotations = Set()
        )
      }
    }

    builder
      .toPublicDimension("advertiser","advertiser",
        Set(
          PubCol("id", "Advertiser ID", InEquality)
          , PubCol("name", "Advertiser Name", Equality)
          , PubCol("Advertiser Status", "Advertiser Status", InEquality)
        ), highCardinalityFilters = Set(NotInFilter("Advertiser Status", List("DELETED"), isForceFilter = true), InFilter("Advertiser Status", List("ON"), isForceFilter = true), EqualityFilter("Advertiser Status", "ON", isForceFilter = true))
      )
  }

  private[this] def pubfact(): PublicFact = {
    import BigqueryExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "ad_stats_bigquery", DailyGrain, BigqueryEngine, Set(AdvertiserSchema),
        Set(
          DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("country", StrType())
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("network_type", StrType(100, (Map("TEST_PUBLISHER" -> "Test Publisher", "CONTENT_S" -> "Content Secured", "EXTERNAL" -> "External Partners" ,  "INTERNAL" -> "Internal Properties"), "NONE")))
          , DimCol("stats_date", DateType("YYYY-MM-dd"))
          , BigqueryDerDimCol("Hour", DateType("YYYY-MM-DD HH24"), "concat({stats_date},' 00')")
        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(8, 0, "0.0"))
          , FactCol("max_bid", DecType(0, 2, "0.0"), MaxRollup)
          , BigqueryDerFactCol("average_cpc", DecType(0, 2, "0"), "{spend}" / "{clicks}")
          , FactCol("CTR", DecType(5, 2, "0"), BigqueryCustomRollup(SUM("{clicks}" /- "{impressions}")))
          , BigqueryDerFactCol("CTR Percentage", DecType(), "{clicks}" /- "{impressions}" * "100")
          , FactCol("Count", IntType(), rollupExpression = CountRollup)
        ),
        annotations = Set()
      )
    }
      .toPublicFact("ad_stats",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("Hour", "Hour", InBetweenEquality),
          PubCol("ad_id", "Ad ID", InEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("country", "Country", Equality),
          PubCol("network_type", "Network ID", InEquality),
          PubCol("price_type", "Pricing Type", In)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InNotInBetweenEqualityNotEqualsGreaterLesser),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("spend", "Spend", Set.empty),
          PublicFactCol("max_bid", "Max Bid", Set.empty, incompatibleColumns = Set("Clicks")),
          PublicFactCol("average_cpc", "Average CPC", InBetweenEquality),
          PublicFactCol("CTR Percentage", "CTR Percentage", Set.empty),
          PublicFactCol("CTR", "CTR", InBetweenEquality),
          PublicFactCol("Count", "Count", Set.empty)
        ),
        Set(EqualityFilter("Source", "2", isForceFilter = true)),
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }

  def validMockTableResultData(): TableResult = {
    val fieldList: FieldList = FieldList.of(
      Field.of("mang_day", StandardSQLTypeName.STRING),
      Field.of("ad_id", StandardSQLTypeName.STRING),
      Field.of("mang_campaign_name", StandardSQLTypeName.STRING),
      Field.of("mang_count", StandardSQLTypeName.STRING),
      Field.of("mang_impressions", StandardSQLTypeName.STRING),
      Field.of("mang_spend", StandardSQLTypeName.STRING)
    )

    val schema: Schema = Schema.of(fieldList)

    val fieldValuesList1: FieldValueList =
      FieldValueList.of(
        ImmutableList.of(
          FieldValue.of(Attribute.PRIMITIVE, "2020-11-22"),
          FieldValue.of(Attribute.PRIMITIVE, "1004"),
          FieldValue.of(Attribute.PRIMITIVE, "Campaign 1"),
          FieldValue.of(Attribute.PRIMITIVE, "5000"),
          FieldValue.of(Attribute.PRIMITIVE, "20"),
          FieldValue.of(Attribute.PRIMITIVE, "0.123")
        ),
        fieldList
      )

    val fieldValuesList2: FieldValueList =
      FieldValueList.of(
        ImmutableList.of(
          FieldValue.of(Attribute.PRIMITIVE, "2020-11-22"),
          FieldValue.of(Attribute.PRIMITIVE, "1005"),
          FieldValue.of(Attribute.PRIMITIVE, "Campaign 2"),
          FieldValue.of(Attribute.PRIMITIVE, "6000"),
          FieldValue.of(Attribute.PRIMITIVE, "70"),
          FieldValue.of(Attribute.PRIMITIVE, "0.234")
        ),
        fieldList
      )

    val page = new PageImpl(
      new PageImpl.NextPageFetcher[FieldValueList]() {
        override def getNextPage = null
      },
      null,
      ImmutableList.of(fieldValuesList1, fieldValuesList2)
    )

    new TableResult(schema, 1, page)
  }

  test("Successfully execute async query for ad_stats") {
    val jsonString =
      s"""
         |{
         |  "cube": "ad_stats",
         |  "selectFields": [
         |     { "field": "Day" },
         |     { "field": "Ad ID" },
         |     { "field": "Campaign Name" },
         |     { "field": "Count" },
         |     { "field": "Impressions" },
         |     { "field": "Spend" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" },
         |    { "field": "Advertiser ID", "operator": "=", "value": "1004" },
         |    { "field": "Impressions", "operator": ">", "value": "2014" }
         |  ],
         |  "sortBy": [
         |    { "field": "Ad ID", "order": "Desc" }
         |  ],
         |  "paginationStartIndex": 0,
         |  "rowsPerPage": 100
         |}
       """.stripMargin

    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestAsync(jsonString))
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipeline = queryPipelineFactory.builder(requestModel.toOption.get, QueryAttributes.empty).get.build()
    val sqlQuery =  queryPipeline.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString

    val result = queryPipeline.execute(queryExecutorContext)

    result match {
      case scala.util.Success(queryPipelineResult) =>
        val inmem = queryPipelineResult.rowList
        assert(!inmem.isEmpty)
        inmem.foreach({ row =>
          row.getValue("Ad ID").toString match {
            case "1004" | "1005" =>
              assert(true)
            case any =>
              assert(false)
          }
          row.getValue("Spend").toString match {
            case "0.123" | "0.234" =>
              assert(true)
            case any =>
              assert(false)
          }
          row.getValue("Campaign Name").toString match {
            case "Campaign 1" | "Campaign 2" =>
              assert(true)
            case any =>
              assert(false)
          }
        })
      case any =>
        any.failed.get.printStackTrace()
        throw new UnsupportedOperationException(s"unexpected row list : $any")
    }
  }

  test("Requested column missing in result") {
    val jsonString =
      s"""
         |{
         |  "cube": "ad_stats",
         |  "selectFields": [
         |    { "field": "Day" },
         |    { "field": "Ad ID" },
         |    { "field": "Ad Creation Date" },
         |    { "field": "Count" },
         |    { "field": "Impressions" },
         |    { "field": "Spend" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" },
         |    { "field": "Advertiser ID", "operator": "=", "value": "1" }
         |  ],
         |  "paginationStartIndex": 0,
         |  "rowsPerPage": 100
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipeline = queryPipelineFactory.builder(requestModel.toOption.get, QueryAttributes.empty, None, BucketParams())._1.get.build()
    val sqlQuery =  queryPipeline.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString

    val result = queryPipeline.execute(queryExecutorContext)

    result match {
      case scala.util.Success(queryPipelineResult) =>
        fail("Expected to fail")
      case any =>
        assert(any.isFailure)
        assert(any.failed.get.getMessage == "Field with name 'mang_ad_creation_date' was not found")
    }
  }

  test("Query execution failure") {
    val jsonString =
      s"""
         |{
         |  "cube": "ad_stats",
         |  "selectFields": [
         |    { "field": "Day" },
         |    { "field": "Ad ID" },
         |    { "field": "Ad Creation Date" },
         |    { "field": "Impressions" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" },
         |    { "field": "Advertiser ID", "operator": "=", "value": "1" }
         |  ],
         |  "paginationStartIndex": 0,
         |  "rowsPerPage": 100
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipeline = queryPipelineFactory.builder(requestModel.toOption.get, QueryAttributes.empty, None, BucketParams())._1.get.build()
    val sqlQuery =  queryPipeline.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString

    val bigqueryError = new BigQueryError("query failure", "location", "invalid query")
    doReturn(bigqueryError).when(jobStatusMock).getError()

    val result = queryPipeline.execute(queryExecutorContext)

    result match {
      case scala.util.Success(queryPipelineResult) =>
        fail("Expected to fail")
      case any =>
        assert(any.isFailure)
        assert(any.failed.get.getMessage.contains("BigQuery Job Failed"))
    }
  }

  test("Invalid job ID or job not found") {
    val jsonString =
      s"""
         |{
         |  "cube": "ad_stats",
         |  "selectFields": [
         |    { "field": "Day" },
         |    { "field": "Ad ID" },
         |    { "field": "Ad Creation Date" },
         |    { "field": "Impressions" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" },
         |    { "field": "Advertiser ID", "operator": "=", "value": "1" }
         |  ],
         |  "paginationStartIndex": 0,
         |  "rowsPerPage": 100
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipeline = queryPipelineFactory.builder(requestModel.toOption.get, QueryAttributes.empty, None, BucketParams())._1.get.build()
    val sqlQuery =  queryPipeline.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString

    doReturn(null).when(jobMock).waitFor()

    val result = queryPipeline.execute(queryExecutorContext)

    result match {
      case scala.util.Success(queryPipelineResult) =>
        fail("Expected to fail")
      case any =>
        assert(any.isFailure)
        assert(any.failed.get.getMessage.contains("BigQuery Job doesn't exist"))
    }
  }

  test("Invalid query engine") {
    val requestModel = Mockito.mock(classOf[RequestModel])
    val queryContext = Mockito.mock(classOf[QueryContext])
    val query = Mockito.mock(classOf[OracleQuery])
    doReturn(IndexedSeq.empty).when(requestModel).requestCols
    doReturn(requestModel).when(queryContext).requestModel
    doReturn(queryContext).when(query).queryContext

    val rowList = new InMemRowList {
      override def query: Query = {
        val requestModel = Mockito.mock(classOf[RequestModel])
        val queryContext = Mockito.mock(classOf[QueryContext])
        val query = Mockito.mock(classOf[OracleQuery])
        doReturn(IndexedSeq.empty).when(requestModel).requestCols
        doReturn(requestModel).when(queryContext).requestModel
        doReturn(queryContext).when(query).queryContext
        doReturn(Map.empty).when(query).aliasColumnMap
        query
      }
      override def columnNames : IndexedSeq[String] = {
        IndexedSeq.empty
      }
      override def ephemeralColumnNames: IndexedSeq[String] = {
        IndexedSeq.empty
      }
    }

    val result = Try(bigqueryQueryExecutor.get.execute(query, rowList, QueryAttributes.empty))
    assert(result.isFailure)
    assert(result.failed.get.isInstanceOf[UnsupportedOperationException])
  }

  test("Render proper JSON strings for all public cols") {
    val registry = getDefaultRegistry()
    val pubFact = registry.getFact("ad_stats")
    val pubFactCols = pubFact.get.factCols
    val pubDimCols = pubFact.get.dimCols
    val fkAliases = pubFact.get.foreignKeySources
    val fkTables = fkAliases.map(source => registry.getDimension(source).get)
    val fkTableNames = fkTables.map(table => table.name)
    val fkCols = fkTables.flatMap(dim => dim.columnsByAliasMap.map(_._2))
    val allJSONs: Set[JObject] = (pubFactCols ++ pubDimCols ++ fkCols).map(col => col.asJSON)

    val expectAliases: List[String] = List(
      "Ad Creation Date",
      "Ad Date Created",
      "Ad Date Modified Timestamp",
      "Ad Date Modified",
      "Ad Group Date Created",
      "Ad Group Date Modified",
      "Ad Group End Date Full",
      "Ad Group End Date",
      "Ad Group ID",
      "Ad Group Name",
      "Ad Group Start Date Full",
      "Ad Group Start Date",
      "Ad Group Status",
      "Ad ID",
      "Ad Status",
      "Ad Title",
      "Advertiser ID",
      "Advertiser Name",
      "Advertiser Status",
      "Average CPC",
      "CTR Percentage",
      "CTR",
      "Campaign ID",
      "Campaign Name",
      "Campaign Status",
      "Clicks",
      "Count",
      "Country",
      "Day",
      "Hour",
      "Impressions",
      "Max Bid",
      "Network ID",
      "Pricing Type",
      "Source",
      "Spend"
    )

    val aliases: List[String] = allJSONs.map(json => json.values("alias").toString).toList
    assert(aliases.forall(alias => expectAliases.contains(alias)))
  }
}
