// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.executor.presto

import java.sql.{Date, ResultSet, Timestamp}
import java.util.UUID

import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core._
import com.yahoo.maha.core.bucketing.BucketParams
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.query.presto.{PrestoQueryGenerator, PrestoQueryGeneratorV1}
import com.yahoo.maha.core.registry.RegistryBuilder
import com.yahoo.maha.core.request._
import com.yahoo.maha.jdbc._
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.mockito.Matchers._
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.util.Try

class PrestoQueryExecutorTest extends FunSuite with Matchers with BeforeAndAfterAll with BaseQueryGeneratorTest {
  
  private var dataSource: Option[HikariDataSource] = None
  private var jdbcConnection: Option[JdbcConnection] = None
  private var prestoQueryExecutor : Option[PrestoQueryExecutor] = None
  private val columnValueExtractor = new ColumnValueExtractor
  private val queryExecutorContext : QueryExecutorContext = new QueryExecutorContext
  private val staticTimestamp = new Timestamp(System.currentTimeMillis())
  private val staticTimestamp2 = new Timestamp(System.currentTimeMillis() + 1)
  private val prestoQueryTemplate = new PrestoQueryTemplate {
    override def buildFinalQuery(query: String, queryContext: QueryContext, queryAttributes: QueryAttributes): String = query
  }

  val woeidTransformer = new ResultSetTransformer {
    override def transform(grain: Grain, resultAlias: String, column: Column, inputValue: Any): Any = {
      if (inputValue != null && inputValue.toString.toLong == 23424977) {
        return "United States"
      } else {
        return "NA"
      }
    }

    override def canTransform(resultAlias: String, column: Column): Boolean = {
      if (resultAlias != null && resultAlias.equalsIgnoreCase("Country")) {
        true
      } else {
        false
      }
    }
  }

  override protected def beforeAll(): Unit = {
    val config = new HikariConfig()
    config.setJdbcUrl("jdbc:h2:mem:" + UUID.randomUUID().toString.replace("-",
      "") + ";DB_CLOSE_DELAY=-1")
    config.setUsername("sa")
    config.setPassword("sa")
    config.setMaximumPoolSize(1)
    PrestoQueryGenerator.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer, Set.empty)
    PrestoQueryGeneratorV1.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer, Set.empty)
    dataSource = Option(new HikariDataSource(config))
    jdbcConnection = dataSource.map(new JdbcConnection(_))
    prestoQueryExecutor = jdbcConnection.map(new PrestoQueryExecutor(_, prestoQueryTemplate, new NoopExecutionLifecycleListener, List(woeidTransformer)))
    prestoQueryExecutor.foreach(queryExecutorContext.register(_))
    initDdlsAndData()
    createUDFs()
  }
  
  override protected def afterAll(): Unit = {
    dataSource.foreach(_.close())
    prestoQueryExecutor.foreach(queryExecutorContext.remove(_))
  }

  override protected[this] def registerFacts(forcedFilters: Set[ForcedFilter], registryBuilder: RegistryBuilder): Unit = {
    registryBuilder.register(pubfact(forcedFilters))
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
        import PrestoExpression._
        Dimension.newDimension("ad_presto", PrestoEngine, LevelFour, Set(AdvertiserSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("title", StrType(), annotations = Set(EscapingRequired, CaseInsensitive))
            , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
            , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
            , DimCol("status", StrType())
            , DimCol("created_date", DateType())
            , DimCol("last_updated", TimestampType())
            , PrestoDerDimCol("Ad Status", StrType(), PrestoDerivedExpression("functionIF(status = 'ON', 'ON', 'OFF')"))
            , PrestoDerDimCol("Ad Date Modified", StrType(),PrestoDerivedExpression("format_datetime(last_updated, 'YYYY-MM-DD')"), annotations = Set.empty)
            , PrestoDerDimCol("Ad Date Created", StrType(),PrestoDerivedExpression("format_datetime(last_updated, 'YYYY-MM-DD')"), annotations = Set.empty)
            , PrestoPartDimCol("load_time", StrType(10, default="2018"), partitionLevel = FirstPartitionLevel)
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
          , PubCol("Ad Date Modified", "Ad Date Modified", InBetweenEquality)
          , PubCol("last_updated", "Ad Date Modified Timestamp", Set.empty)
        ), highCardinalityFilters = Set(NotInFilter("Ad Status", List("DELETED"), isForceFilter = true), InFilter("Ad Status", List("ON"), isForceFilter = true), EqualityFilter("Ad Status", "ON", isForceFilter = true))
      )
  }

  private[this] def ad_group_dim: PublicDimension = {
    val builder : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        import PrestoExpression._
        Dimension.newDimension("ad_group_presto", PrestoEngine, LevelThree, Set(AdvertiserSchema),
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
            , PrestoDerDimCol("Ad Group Status", StrType(), PrestoDerivedExpression("functionIF(status = 'ON', 'ON', 'OFF')"))
            , PrestoDerDimCol("Ad Group Start Date Full", StrType(), PrestoDerivedExpression("format_datetime(start_time, 'YYYY-MM-DD HH:mm:ss')"), annotations = Set.empty)
            , PrestoDerDimCol("Ad Group End Date Full", StrType(),PrestoDerivedExpression("format_datetime(end_time, 'YYYY-MM-DD HH:mm:ss')"), annotations = Set.empty)
            , PrestoDerDimCol("Ad Group Start Date", StrType(), PrestoDerivedExpression("format_datetime(start_time, 'YYYY-MM-DD')"), annotations = Set.empty)
            , PrestoDerDimCol("Ad Group End Date", StrType(),PrestoDerivedExpression("format_datetime(end_time, 'YYYY-MM-DD')"), annotations = Set.empty)
            , PrestoDerDimCol("Ad Group Date Created", StrType(), PrestoDerivedExpression("format_datetime(created_date, 'YYYY-MM-DD')"), annotations = Set.empty)
            , PrestoDerDimCol("Ad Group Date Modified", StrType(),PrestoDerivedExpression("format_datetime(last_updated, 'YYYY-MM-DD')"), annotations = Set.empty)
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
        import PrestoExpression._
        Dimension.newDimension("campaign_presto", PrestoEngine, LevelTwo, Set(AdvertiserSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("name", StrType(), annotations = Set(EscapingRequired, CaseInsensitive))
            , DimCol("status", StrType())
            , PrestoDerDimCol("Campaign Status", StrType(), PrestoDerivedExpression("functionIF(status = 'ON', 'ON', 'OFF')"))
            , PrestoPartDimCol("load_time", StrType(10, default="2018"), partitionLevel = FirstPartitionLevel)
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
        import PrestoExpression._
        Dimension.newDimension("advertiser_presto", PrestoEngine, LevelOne, Set(AdvertiserSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("name", StrType())
            , DimCol("status", StrType())
            , PrestoDerDimCol("Advertiser Status", StrType(), PrestoDerivedExpression("functionIF(status = 'ON', 'ON', 'OFF')"))
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

  private[this] def pubfact(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import PrestoExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "ad_stats_presto", DailyGrain, PrestoEngine, Set(AdvertiserSchema),
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
          , PrestoDerDimCol("Hour", DateType("YYYY-MM-DD HH24"), "concat({stats_date},' 00')")
        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(8, 0, "0.0"))
          , FactCol("max_bid", DecType(0, 2, "0.0"), MaxRollup)
          , PrestoDerFactCol("average_cpc", DecType(0, 2, "0"), "{spend}" / "{clicks}")
          , FactCol("CTR", DecType(5, 2, "0"), PrestoCustomRollup(SUM("{clicks}" /- "{impressions}")))
          , PrestoDerFactCol("CTR Percentage", DecType(), "{clicks}" /- "{impressions}" * "100")
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
          PublicFactCol("max_bid", "Max Bid", Set.empty),
          PublicFactCol("average_cpc", "Average CPC", InBetweenEquality),
          PublicFactCol("CTR Percentage", "CTR Percentage", Set.empty),
          PublicFactCol("CTR", "CTR", InBetweenEquality),
          PublicFactCol("Count", "Count", Set.empty)
        ),
        Set(EqualityFilter("Source", "2", isForceFilter = true)),
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }

  private def insertRows(insertSql: String, rows: List[Seq[Any]], validationSql: String) : Unit = {
    rows.foreach {
      row =>
        val result = jdbcConnection.get.executeUpdate(insertSql, row)
        assert(result.isSuccess && result.toOption.get === 1)
    } 
    var count = 0
    jdbcConnection.get.queryForObject(validationSql) {
      rs =>
        while(rs.next()) {
          count+=1
        }
    }
    assert(rows.size === count)
  }

  def createUDFs(): Unit = {


    var stmt = "CREATE ALIAS getFormattedDate AS $$ import java.sql.*; @CODE String getFormattedDate(Timestamp value) { return \"01-03-2018\"; } $$"
    var resultMacro = jdbcConnection.get.execute(stmt)
    //resultMacro.failed.get.printStackTrace()
    assert(resultMacro.isSuccess && resultMacro.toOption.get === false)

    stmt = "CREATE ALIAS GETCSVESCAPEDSTRING AS $$ String getCSVEscapedString(String value) { return value; } $$"
    resultMacro = jdbcConnection.get.execute(stmt)
    assert(resultMacro.isSuccess && resultMacro.toOption.get === false)

    stmt = "CREATE ALIAS FORMAT_DATETIME AS $$ import java.sql.*; @CODE String formatDateTime(java.sql.Timestamp date, String format) { return date.toString(); } $$"
    resultMacro = jdbcConnection.get.execute(stmt)
    assert(resultMacro.isSuccess && resultMacro.toOption.get === false)

    stmt = "CREATE ALIAS DUMMY AS $$ import java.sql.*; @CODE String dummy(java.sql.Timestamp date) { return \"2017-03-01\"; } $$"
    resultMacro = jdbcConnection.get.execute(stmt)
    assert(resultMacro.isSuccess && resultMacro.toOption.get === false)

    stmt = "CREATE ALIAS functionIF AS $$ String functionIf(Boolean condition, String val1, String val2) { return \"TEST\"; } $$"
    resultMacro = jdbcConnection.get.execute(stmt)
    //resultMacro.failed.get.printStackTrace()
    assert(resultMacro.isSuccess && resultMacro.toOption.get === false)
  }

  def initDdlsAndData (): Unit = {

    val resultAds = jdbcConnection.get.execute(
      """
        //CREATE TABLE ad_presto (id VARCHAR2(244), num INT, decimalValue DECIMAL, dt DATE, ts TIMESTAMP)
        CREATE TABLE ad_presto (
          id NUMBER
          , title VARCHAR2(255 CHAR)
          , advertiser_id NUMBER
          , campaign_id NUMBER
          , ad_group_id NUMBER
          , status VARCHAR2(255 CHAR)
          , created_date TIMESTAMP
          , load_time NUMBER
          , last_updated TIMESTAMP)
      """
    )
    assert(resultAds.isSuccess && resultAds.toOption.get === false)

    val resultAdGroup = jdbcConnection.get.execute(
      """
        CREATE TABLE ad_group_presto (
          id NUMBER
          , name  VARCHAR2(255 CHAR)
          , advertiser_id NUMBER
          , campaign_id NUMBER
          , status VARCHAR2(255 CHAR)
          , created_date TIMESTAMP
          , last_updated TIMESTAMP)
      """
    )
    assert(resultAdGroup.isSuccess && resultAdGroup.toOption.get === false)

    val resultCampaign = jdbcConnection.get.execute(
      """
        CREATE TABLE campaign_presto (
          id NUMBER
          , name  VARCHAR2(255 CHAR)
          , advertiser_id NUMBER
          , status VARCHAR2(255 CHAR)
          , created_date TIMESTAMP
          , load_time NUMBER
          , last_updated TIMESTAMP)
      """
    )
    assert(resultCampaign.isSuccess && resultCampaign.toOption.get === false)

    val resultAdvertiser = jdbcConnection.get.execute(
      """
        CREATE TABLE advertiser_presto (
          id NUMBER
          , name  VARCHAR2(255 CHAR)
          , status VARCHAR2(255 CHAR)
          , created_date TIMESTAMP
          , last_updated TIMESTAMP)
      """
    )
    assert(resultAdvertiser.isSuccess && resultAdvertiser.toOption.get === false)

    val resultAdsStats = jdbcConnection.get.execute(
      """
        CREATE TABLE ad_stats_presto (
          stats_date DATE
          , ad_id NUMBER
          , ad_group_id NUMBER
          , campaign_id NUMBER
          , advertiser_id NUMBER
          , stats_source NUMBER(3)
          , country NUMBER(19)
          , price_type NUMBER(3)
          , impressions NUMBER(19)
          , clicks NUMBER(19)
          , spend NUMBER(21,6)
          , max_bid NUMBER(21,6)
          , network_type VARCHAR2(100 CHAR))
      """
    )
    assert(resultAdsStats.isSuccess && resultAdsStats.toOption.get === false)

    val insertSqlAdvertiser =
      """
        INSERT INTO advertiser_presto (id, name, status, created_date, last_updated)
        VALUES (?, ?, ?, ?, ?)
      """

    val rowsAdvertiser: List[Seq[Any]] = List(
      Seq(1, "advertiser1", "ON", staticTimestamp, staticTimestamp2)
    )

    insertRows(insertSqlAdvertiser, rowsAdvertiser, "SELECT * FROM advertiser_presto")

    val insertSqlCampaign =
      """
        INSERT INTO campaign_presto (id, name, advertiser_id, status, created_date, load_time, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      """

    val rowsCampaigns: List[Seq[Any]] = List(
      Seq(10, "campaign10", 1, "ON", staticTimestamp, "2018", staticTimestamp2)
      , Seq(11, "campaign11", 1, "ON", staticTimestamp, "2018", staticTimestamp2)
    )

    insertRows(insertSqlCampaign, rowsCampaigns, "SELECT * FROM campaign_presto")

    val insertSqlAdGroup =
      """
        INSERT INTO ad_group_presto (id, name, advertiser_id, campaign_id, status, created_date, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      """

    val rowsAdGroups: List[Seq[Any]] = List(
      Seq(100, "adgroup100", 1, 10, "ON", staticTimestamp, staticTimestamp2)
      , Seq(101, "adgroup101", 1, 10, "ON", staticTimestamp, staticTimestamp2)
      , Seq(102, "adgroup102", 1, 11, "ON", staticTimestamp, staticTimestamp2)
      , Seq(103, "adgroup103", 1, 11, "ON", staticTimestamp, staticTimestamp2)
    )

    insertRows(insertSqlAdGroup, rowsAdGroups, "SELECT * FROM ad_group_presto")
    val sd = new Date(System.currentTimeMillis())

    val insertSqlAds =
      """
        INSERT INTO ad_presto (id, title, advertiser_id, campaign_id, ad_group_id, status, created_date, last_updated, load_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      """
    val rowsAds: List[Seq[Any]] = List(
      Seq(1000, "adtitle1000", 1, 10, 100, "ON", sd, staticTimestamp2, 2018)
      , Seq(1001, "adtitle1001", 1, 10, 100, "ON", sd, staticTimestamp2, 2018)
      , Seq(1002, "adtitle1002", 1, 10, 101, "ON", sd, staticTimestamp2, 2018)
      , Seq(1003, "adtitle1003", 1, 10, 101, "ON", sd, staticTimestamp2, 2018)
      , Seq(1004, "adtitle1004", 1, 11, 102, "ON", sd, staticTimestamp2, 2018)
      , Seq(1005, "adtitle1005", 1, 11, 102, "ON", sd, staticTimestamp2, 2018)
      , Seq(1006, "adtitle1006", 1, 11, 103, "ON", sd, staticTimestamp2, 2018)
      , Seq(1007, "adtitle1007", 1, 11, 103, "ON", sd, staticTimestamp2, 2018)
    )

    insertRows(insertSqlAds, rowsAds, "SELECT * FROM ad_presto")

    val insertSqlAdsStats =
      """
        INSERT INTO ad_stats_presto 
        (stats_date, ad_id, ad_group_id, campaign_id, advertiser_id, stats_source, price_type, impressions, clicks, spend, max_bid, country, network_type)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """
    val rowsAdsStats: List[Seq[Any]] = List(
      Seq(sd, 1000, 100, 10, 1, 1, 1, 1002, 2, 2.10, 0.21, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1000, 100, 10, 1, 1, 1, 1003, 3, 3.10, 0.31, 23424977, "CONTENT_S")
      , Seq(sd, 1000, 100, 10, 1, 2, 1, 1003, 3, 3.10, 0.31, 23424977, "EXTERNAL")
      , Seq(sd, 1000, 100, 10, 1, 2, 2, 1004, 4, 4.10, 0.41, 123, "TEST_PUBLISHER")
      , Seq(sd, 1001, 100, 10, 1, 1, 1, 1003, 3, 3.10, 0.31, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1001, 100, 10, 1, 1, 2, 1004, 4, 4.10, 0.41, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1001, 100, 10, 1, 2, 1, 1004, 4, 4.10, 0.41, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1001, 100, 10, 1, 2, 2, 1005, 5, 5.10, 0.51, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1002, 101, 10, 1, 1, 1, 1004, 4, 4.10, 0.41, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1002, 101, 10, 1, 1, 2, 1005, 5, 5.10, 0.51, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1002, 101, 10, 1, 2, 1, 1005, 5, 5.10, 0.51, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1002, 101, 10, 1, 2, 2, 1006, 6, 6.10, 0.61, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1003, 101, 10, 1, 1, 1, 1005, 5, 5.10, 0.51, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1003, 101, 10, 1, 1, 2, 1006, 6, 6.10, 0.61, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1003, 101, 10, 1, 2, 1, 1006, 6, 6.10, 0.61, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1003, 101, 10, 1, 2, 2, 1007, 7, 7.10, 0.71, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1004, 102, 11, 1, 1, 1, 1006, 6, 6.10, 0.61, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1004, 102, 11, 1, 1, 2, 1007, 7, 7.10, 0.71, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1004, 102, 11, 1, 2, 1, 1007, 7, 7.10, 0.71, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1004, 102, 11, 1, 2, 2, 1008, 8, 8.10, 0.81, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1005, 102, 11, 1, 1, 1, 1007, 7, 7.10, 0.71, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1005, 102, 11, 1, 1, 2, 1008, 8, 8.10, 0.81, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1005, 102, 11, 1, 2, 1, 1008, 8, 8.10, 0.81, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1005, 102, 11, 1, 2, 2, 1009, 9, 9.10, 0.91, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1006, 103, 11, 1, 1, 1, 1008, 8, 8.10, 0.81, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1006, 103, 11, 1, 1, 2, 1009, 9, 9.10, 0.91, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1006, 103, 11, 1, 2, 1, 1009, 9, 9.10, 0.91, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1006, 103, 11, 1, 2, 2, 1010, 10, 10.10, 1.01, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1007, 103, 11, 1, 1, 1, 1009, 9, 9.10, 0.91, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1007, 103, 11, 1, 1, 2, 1010, 10, 10.10, 1.01, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1007, 103, 11, 1, 2, 1, 1010, 10, 10.10, 1.01, 23424977, "TEST_PUBLISHER")
      , Seq(sd, 1007, 103, 11, 1, 2, 2, 1011, 11, 11.10, 1.11, 23424977, "TEST_PUBLISHER")
    )

    insertRows(insertSqlAdsStats, rowsAdsStats, "SELECT * FROM ad_stats_presto")
  }

  test("successfully execute async query for ad_stats with greater than filter") {
    val jsonString = s"""{
                          "cube": "ad_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Ad ID"},
                            {"field": "Count"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "1"},
                            {"field": "Impressions", "operator": ">", "value": "2014"}
                          ],
                          "sortBy": [
                            {"field": "Ad ID", "order": "Desc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestAsync(jsonString))
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipeline = queryPipelineFactory.builder(requestModel.toOption.get, QueryAttributes.empty).get.build()
    val sqlQuery =  queryPipeline.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString

    val result = queryPipeline.execute(queryExecutorContext)

    result match {
      case scala.util.Success(queryPipelineResult) =>
        val inmem = queryPipelineResult.rowList
        assert(!inmem.isEmpty)
        inmem.foreach({ row =>
          row.getValue("Ad ID").toString match {
            case "1004" | "1005" | "1006" | "1007" =>
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

  test("successfully execute async query for ad_stats with less than filter") {
    val jsonString = s"""{
                          "cube": "ad_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Ad ID"},
                            {"field": "Count"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "1"},
                            {"field": "Impressions", "operator": "<", "value": "2014"}
                          ],
                          "sortBy": [
                            {"field": "Ad ID", "order": "Desc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestAsync(jsonString))
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipeline = queryPipelineFactory.builder(requestModel.toOption.get, QueryAttributes.empty).get.build()
    val sqlQuery =  queryPipeline.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString

    val result = queryPipeline.execute(queryExecutorContext)

    result match {
      case scala.util.Success(queryPipelineResult) =>
        val inmem = queryPipelineResult.rowList
        assert(!inmem.isEmpty)
        inmem.foreach({ row =>
          row.getValue("Ad ID").toString match {
            case "1000" | "1001" | "1002" | "1003" =>
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


  test("successfully execute async query for ad_stats") {
    val jsonString = s"""{
                          "cube": "ad_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Campaign ID"},
                            {"field": "Ad Group ID"},
                            {"field": "Ad ID"},
                            {"field": "Ad Title"},
                            {"field": "Ad Status"},
                            {"field": "Ad Date Created"},
                            {"field": "Ad Date Modified"},
                            {"field": "Ad Date Modified Timestamp"},
                            {"field": "Pricing Type"},
                            {"field": "Network ID"},
                            {"field": "Impressions"},
                            {"field": "Max Bid"},
                            {"field": "Average CPC"},
                            {"field": "Spend"},
                            {"field": "CTR Percentage"},
                            {"field": "CTR"},
                            {"field": "Country"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "1"},
                            {"field": "Pricing Type", "operator": "in", "values": ["CPC","CPA"] }
                          ],
                          "sortBy": [
                            {"field": "Ad Title", "order": "Desc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestAsync(jsonString))
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipeline = queryPipelineFactory.builder(requestModel.toOption.get, QueryAttributes.empty, None, BucketParams())._1.get.build()
    val sqlQuery =  queryPipeline.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString

    val result = queryPipeline.execute(queryExecutorContext)

    result match {
      case scala.util.Success(queryPipelineResult) =>
        val inmem = queryPipelineResult.rowList
        assert(!inmem.isEmpty)
        inmem.foreach({ row =>
          if(row.getValue("Ad ID") == 1000 && row.getValue("Pricing Type") == "CPA") {
            assert(row.getValue("Day") == "01-03-2018")
            assert(row.getValue("Campaign ID") == 10)
            assert(row.getValue("Ad Group ID") == 100)
            assert(row.getValue("Ad Title") == "adtitle1000")
            assert(row.getValue("Ad Status") == "TEST")
            assert(row.getValue("Impressions") == 1004)
            assert(row.getValue("Max Bid") == 0.41)
            assert(row.getValue("Average CPC") == 1.02)
            assert(row.getValue("Spend") == 4.1)
            assert(row.getValue("CTR Percentage") == 0.3984063745)
            assert(row.getValue("CTR") == 0.0)
            assert(row.getValue("Country") == "NA")
          } else {
            assert(row.getValue("Country") == "United States")
          }
        })
      case any =>
        any.failed.get.printStackTrace()
        throw new UnsupportedOperationException(s"unexpected row list : $any")
    }

  }

  test("test invalid result") {
    val jsonString = s"""{
                          "cube": "ad_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Ad ID"},
                            {"field": "Ad Creation Date"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "1"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipeline = queryPipelineFactory.builder(requestModel.toOption.get, QueryAttributes.empty, None, BucketParams())._1.get.build()
    val sqlQuery =  queryPipeline.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString

    val result = queryPipeline.execute(queryExecutorContext)

    result match {
      case scala.util.Success(queryPipelineResult) =>
        fail("Expected to fail")
      case any =>
        assert(any.isFailure)
        any.failed.get.printStackTrace()
    }
  }

  test("test null result") {
    var resultSet: ResultSet = null
    //val executor : PrestoQueryExecutor = Mockito.spy(prestoQueryExecutor.get)
    val today = new Date(1515794890000L)
    jdbcConnection.get.queryForList("select * from ad_stats_presto where ad_id=1000 limit 1") {
      rs => {
        resultSet = Mockito.spy(rs)
        Mockito.doNothing().when(resultSet).close()
        Mockito.doReturn(null).when(resultSet).getBigDecimal(anyInt())
        Mockito.doReturn(null).when(resultSet).getDate(1)
        Mockito.doReturn(today).when(resultSet).getDate(2)
        Mockito.doReturn(null).when(resultSet).getTimestamp(anyInt())
      }
    }

    assert(columnValueExtractor.getBigDecimalSafely(resultSet, 1) == null)

    abstract class TestCol extends Column {
      override def alias: Option[String] = None
      override def filterOperationOverrides: Set[FilterOperation] = Set.empty
      override def isDerivedColumn: Boolean = false
      override def name: String = "test"
      override def annotations: Set[ColumnAnnotation] = Set.empty
      override def columnContext: ColumnContext = null
      override def dataType: DataType = ???
    }

    val dateCol = new TestCol {
      override def dataType: DataType = DateType()
    }
    assert(columnValueExtractor.getColumnValue(1, dateCol, resultSet) == null)
    assert(columnValueExtractor.getColumnValue(2, dateCol, resultSet) == "2018-01-12")

    val timestampCol = new TestCol {
      override def dataType: DataType = TimestampType()
    }
    val decCol = new TestCol {
      override def dataType : DataType = DecType()
    }
    val decWithLen = new TestCol {
      override def dataType : DataType = DecType(1, 0)
    }
    val decWithScaleAndLength = new TestCol {
      override def dataType : DataType = DecType(1, 1)
    }
    val invalidType = new TestCol {
      override def dataType : DataType = null
    }

    assert(columnValueExtractor.getColumnValue(1, timestampCol, resultSet) == null)
    assert(columnValueExtractor.getColumnValue(5, decCol, resultSet) == null)
    assert(columnValueExtractor.getColumnValue(5, decWithLen, resultSet) == null)
    assert(columnValueExtractor.getColumnValue(5, decWithScaleAndLength, resultSet) == null)
    assertThrows[UnsupportedOperationException](columnValueExtractor.getColumnValue(5, invalidType, resultSet))

  }

  test("test invalid query engine") {

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
    val result = Try(prestoQueryExecutor.get.execute(query, rowList, QueryAttributes.empty))
    assert(result.isFailure)
    assert(result.failed.get.isInstanceOf[UnsupportedOperationException])
  }

  test("Non-ogb query for presto_v1 should have aliasColumnMap populated for constant column") {
    val jsonString = s"""{
                          "cube": "ad_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Advertiser ID"},
                            {"field": "Campaign ID"},
                            {"field": "Source", "alias": "Source", "value": "2"},
                            {"field": "Spend"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "1"}
                          ],
                          "sortBy": [
                            {"field": "Spend", "order": "Desc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestAsync(jsonString))
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    assert(!query.contains("OgbQueryAlias"))
    val result = queryPipelineTry.get.execute(queryExecutorContext)

    result match {
      case scala.util.Success(queryPipelineResult) =>
        val inmem = queryPipelineResult.rowList
        assert(!inmem.isEmpty)
        inmem.foreach({ row =>
          assert(row.getValue("Day") == "01-03-2018")
          assert(row.getValue("Advertiser ID") == 1)
          assert(row.getValue("Source") == 2)
          if(row.getValue("Campaign ID") == 10) {
            assert(row.getValue("Spend") == 40.8)
          } else {
            assert(row.getValue("Spend") == 72.8)
            assert(row.getValue("Campaign ID") == 11)
          }
        })
      case any =>
        any.failed.get.printStackTrace()
        throw new UnsupportedOperationException(s"unexpected row list : $any")
    }

  }

  test("ogb query should have aliasColumnMap populated") {
    val jsonString = s"""{
                          "cube": "ad_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Advertiser ID"},
                            {"field": "Campaign Name"},
                            {"field": "Source", "alias": "Source", "value": "2"},
                            {"field": "Spend"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "1"}
                          ],
                          "sortBy": [
                            {"field": "Campaign Name", "order": "Desc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestAsync(jsonString))
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    assert(query.contains("OgbQueryAlias"))
    val result = queryPipelineTry.get.execute(queryExecutorContext)

    result match {
      case scala.util.Success(queryPipelineResult) =>
        val inmem = queryPipelineResult.rowList
        assert(!inmem.isEmpty)
        inmem.foreach({ row =>
          assert(row.getValue("Day") == "01-03-2018")
          assert(row.getValue("Advertiser ID") == 1)
          assert(row.getValue("Source") == 2)
          if(row.getValue("Campaign Name") == "campaign10") {
            assert(row.getValue("Spend") == 40.8)
          } else {
            assert(row.getValue("Spend") == 72.8)
            assert(row.getValue("Campaign Name") == "campaign11")
          }
        })
      case any =>
        any.failed.get.printStackTrace()
        throw new UnsupportedOperationException(s"unexpected row list : $any")
    }

  }
}
