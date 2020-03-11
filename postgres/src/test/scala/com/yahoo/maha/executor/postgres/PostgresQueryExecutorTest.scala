// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.executor.postgres

import java.sql.{Date, ResultSet, Timestamp}
import java.util.UUID

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core._
import com.yahoo.maha.core.bucketing.BucketParams
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.query.druid.DruidQueryGenerator
import com.yahoo.maha.core.query.postgres.PostgresQueryGenerator
import com.yahoo.maha.core.registry.RegistryBuilder
import com.yahoo.maha.core.request._
import com.yahoo.maha.executor.MockDruidQueryExecutor
import com.yahoo.maha.jdbc._
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.apache.commons.lang3.StringUtils
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

/**
 * Created by hiral on 1/25/16.
 */
class PostgresQueryExecutorTest extends FunSuite with Matchers with BeforeAndAfterAll with BaseQueryGeneratorTest {

  override protected def defaultFactEngine: Engine = PostgresEngine

  private var dataSource: Option[HikariDataSource] = None
  private var jdbcConnection: Option[JdbcConnection] = None
  private var postgresQueryExecutor : Option[PostgresQueryExecutor] = None
  private val columnValueExtractor = new ColumnValueExtractor
  private val queryExecutorContext : QueryExecutorContext = new QueryExecutorContext
  private val staticTimestamp = new Timestamp(System.currentTimeMillis())
  private val staticTimestamp2 = new Timestamp(System.currentTimeMillis() + 1)
  val userDir = System.getProperty("user.dir")
  if (StringUtils.isNotBlank(userDir)) {
    System.setProperty("java.io.tmpdir", userDir+"/target")
  }
  private val pg = EmbeddedPostgres.start()
  private val db = UUID.randomUUID().toString.replace("-", "")

  override protected def beforeAll(): Unit = {
    val config = new HikariConfig()
    val jdbcUrl = pg.getJdbcUrl("postgres", "postgres")
    config.setJdbcUrl(jdbcUrl)
//    config.setJdbcUrl("jdbc:h2:mem:" + UUID.randomUUID().toString.replace("-",
//      "") + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1")
    config.setUsername("postgres")
    config.setPassword("")
    config.setMaximumPoolSize(1)
    PostgresQueryGenerator.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer)
    DruidQueryGenerator.register(queryGeneratorRegistry)
    dataSource = Option(new HikariDataSource(config))
    jdbcConnection = dataSource.map(new JdbcConnection(_))
    postgresQueryExecutor = jdbcConnection.map(new PostgresQueryExecutor(_, new NoopExecutionLifecycleListener))
    postgresQueryExecutor.foreach(queryExecutorContext.register(_))
    initDdlsAndData()
  }
  
  override protected def afterAll(): Unit = {
    dataSource.foreach(_.close())
    postgresQueryExecutor.foreach(queryExecutorContext.remove(_))
  }

  override protected[this] def registerFacts(forcedFilters: Set[ForcedFilter], registryBuilder: RegistryBuilder): Unit = {
    registryBuilder.register(pubfact(forcedFilters))
    registryBuilder.register(pubfactInvalid(forcedFilters))
    registryBuilder.register(druidpubfact(forcedFilters))
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
        import PostgresExpression._
        Dimension.newDimension("ad_postgres", PostgresEngine, LevelFour, Set(AdvertiserSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("title", StrType(), annotations = Set(EscapingRequired, CaseInsensitive))
            , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
            , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
            , DimCol("status", StrType())
            , DimCol("unknown", StrType())
            , DimCol("created_date", TimestampType())
            , DimCol("last_updated", TimestampType())
            , PostgresDerDimCol("Ad Status", StrType(), DECODE_DIM("{status}", "'ON'", "'ON'", "'OFF'"))
            , PostgresDerDimCol("Ad Date Created", StrType(), FORMAT_DATE("{created_date}", "YYYY-MM-DD"), annotations = Set.empty)
            , PostgresDerDimCol("Ad Date Modified", StrType(),FORMAT_DATE("{last_updated}", "YYYY-MM-DD"), annotations = Set.empty)
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , annotations = Set(PostgresHashPartitioning)
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
          , PubCol("Ad Date Modified", "Ad Date Modified", InBetweenEquality)
          , PubCol("last_updated", "Ad Date Modified Timestamp", Set.empty)
          , PubCol("unknown", "Ad Unknown Column", Set.empty)
        ), highCardinalityFilters = Set(NotInFilter("Ad Status", List("DELETED"), isForceFilter = true), InFilter("Ad Status", List("ON"), isForceFilter = true), EqualityFilter("Ad Status", "ON", isForceFilter = true))
      )
  }

  private[this] def ad_group_dim: PublicDimension = {
    val builder : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        import PostgresExpression._
        Dimension.newDimension("ad_group_postgres", PostgresEngine, LevelThree, Set(AdvertiserSchema),
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
            , PostgresDerDimCol("Ad Group Status", StrType(), DECODE_DIM("{status}", "'ON'", "'ON'", "'OFF'"))
            , PostgresDerDimCol("Ad Group Start Date Full", StrType(),FORMAT_DATE("{start_time}", "YYYY-MM-dd HH:mm:ss"))
            , PostgresDerDimCol("Ad Group End Date Full", StrType(),FORMAT_DATE("{end_time}", "YYYY-MM-dd HH:mm:ss"))
            , PostgresDerDimCol("Ad Group Start Date", StrType(), FORMAT_DATE("{start_time}", "YYYY-MM-DD"), annotations = Set.empty)
            , PostgresDerDimCol("Ad Group End Date", StrType(),FORMAT_DATE("{end_time}", "YYYY-MM-DD"), annotations = Set.empty)
            , PostgresDerDimCol("Ad Group Date Created", StrType(), FORMAT_DATE("{created_date}", "YYYY-MM-DD"), annotations = Set.empty)
            , PostgresDerDimCol("Ad Group Date Modified", StrType(),FORMAT_DATE("{last_updated}", "YYYY-MM-DD"), annotations = Set.empty)
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , annotations = Set(PostgresHashPartitioning)
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
        import PostgresExpression._
        Dimension.newDimension("campaign_postgres", PostgresEngine, LevelTwo, Set(AdvertiserSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("name", StrType(), annotations = Set(EscapingRequired, CaseInsensitive))
            , DimCol("status", StrType())
            , PostgresDerDimCol("Campaign Status", StrType(), DECODE_DIM("{status}", "'ON'", "'ON'", "'OFF'"))
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , annotations = Set(PostgresHashPartitioning, DimensionPostgresStaticHint("CampaignHint"))
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
        import PostgresExpression._
        Dimension.newDimension("advertiser_postgres", PostgresEngine, LevelOne, Set(AdvertiserSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("name", StrType())
            , DimCol("status", StrType())
            , PostgresDerDimCol("Advertiser Status", StrType(), DECODE_DIM("{status}", "'ON'", "'ON'", "'OFF'"))
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , schemaColMap = Map(AdvertiserSchema -> "id")
          , annotations = Set(PostgresHashPartitioning)
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

  private[this] def pubfact(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFactTable = {
    import PostgresExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "ad_stats_postgres", DailyGrain, PostgresEngine, Set(AdvertiserSchema),
        Set(
          DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("stats_date", DateType())
          , PostgresDerDimCol("Year", StrType(), GET_INTERVAL_DATE("{stats_date}", "YR"))
          , PostgresDerDimCol("Hour", DateType("YYYY-MM-DD HH24"), "{stats_date}")
        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(8, 0, "0.0"))
          , FactCol("max_bid", DecType(0, 2, "0.0"), MaxRollup)
          , PostgresDerFactCol("average_cpc", DecType(0, 2, "0"), "{spend}" / "{clicks}")
          , FactCol("CTR", DecType(5, 2, "0"), PostgresCustomRollup(SUM("{clicks}" /- "{impressions}")))
          , PostgresDerFactCol("CTR Percentage", DecType(), "{clicks}" /- "{impressions}" * "100")
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
          PubCol("Year", "Year", InEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("stats_source", "Source", Equality),
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
          PublicFactCol("Count", "Count", InBetweenEquality)
        ),
        Set(EqualityFilter("Source", "2", isForceFilter = true)),
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }

  private[this] def pubfactInvalid(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFactTable = {
    import PostgresExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "ad_stats_postgres", DailyGrain, PostgresEngine, Set(AdvertiserSchema),
        Set(
          DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("stats_date", DateType())
          , PostgresDerDimCol("Year", StrType(), GET_INTERVAL_DATE("{stats_date}", "YR"))
          , PostgresDerDimCol("Hour", DateType("YYYY-MM-DD HH24"), "concat({stats_date},' 00')")
        ),
        Set(
          FactCol("impressions_invalid", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(8, 0, "0.0"))
          , FactCol("max_bid", DecType(0, 2, "0.0"), MaxRollup)
          , PostgresDerFactCol("average_cpc", DecType(0, 2, "0"), "{spend}" / "{clicks}")
          , FactCol("CTR", DecType(5, 2, "0"), PostgresCustomRollup(SUM("{clicks}" /- "{impressions_invalid}")))
          , PostgresDerFactCol("CTR Percentage", DecType(), "{clicks}" /- "{impressions_invalid}" * "100")
        ),
        annotations = Set()
      )
    }
      .toPublicFact("ad_stats_invalid",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("Hour", "Hour", InBetweenEquality),
          PubCol("ad_id", "Ad ID", InEquality),
          PubCol("Year", "Year", InEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In)
        ),
        Set(
          PublicFactCol("impressions_invalid", "Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("spend", "Spend", Set.empty),
          PublicFactCol("max_bid", "Max Bid", Set.empty),
          PublicFactCol("average_cpc", "Average CPC", InBetweenEquality),
          PublicFactCol("CTR Percentage", "CTR Percentage", Set.empty),
          PublicFactCol("CTR", "CTR", InBetweenEquality)
        ),
        Set(EqualityFilter("Source", "2", isForceFilter = true)),
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }

  private[this] def druidpubfact(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFactTable = {
    import DruidExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "ad_group_stats_postgres", DailyGrain, DruidEngine, Set(AdvertiserSchema),
        Set(
          DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("stats_date", DateType("YYYY-MM-dd"))

        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(8, 0, "0.0"))
          , FactCol("max_bid", DecType(0, 2, "0.0"), MaxRollup)
          , DruidDerFactCol("average_cpc", DecType(0, 2, "0"), "{spend}" / "{clicks}")
          , DruidDerFactCol("CTR", DecType(5, 2, "0"), "{clicks}" /- "{impressions}")
          , DruidDerFactCol("CTR Percentage", DecType(), "{clicks}" /- "{impressions}" * "100")
        ),
        annotations = Set()
      )
    }
      .toPublicFact("ad_group_stats",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("spend", "Spend", Set.empty),
          PublicFactCol("max_bid", "Max Bid", Set.empty),
          PublicFactCol("average_cpc", "Average CPC", InBetweenEquality),
          PublicFactCol("CTR Percentage", "CTR Percentage", Set.empty),
          PublicFactCol("CTR", "CTR", InBetweenEquality)
        ),
        Set.empty,
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
  
  private def withMockDruidQueryExecutor(rowListCallback: QueryRowList => Unit)(fn: => Unit) : Unit = {
    val qe = new MockDruidQueryExecutor(rowListCallback)
    try {
      queryExecutorContext.register(qe)
      fn
    } finally {
      queryExecutorContext.remove(qe)
    }
  }

  def initDdlsAndData (): Unit = {

    val resultAds = jdbcConnection.get.execute(
      """
        CREATE TABLE ad_postgres (
          id NUMERIC
          , title VARCHAR(255)
          , advertiser_id NUMERIC
          , campaign_id NUMERIC
          , ad_group_id NUMERIC
          , status VARCHAR(255)
          , created_date TIMESTAMP
          , last_updated TIMESTAMP)
      """
    )
    assert(resultAds.isSuccess && resultAds.toOption.get === false, resultAds)

    val resultAdGroup = jdbcConnection.get.execute(
      """
        CREATE TABLE ad_group_postgres (
          id NUMERIC
          , name  VARCHAR(255)
          , advertiser_id NUMERIC
          , campaign_id NUMERIC
          , status VARCHAR(255)
          , created_date TIMESTAMP
          , last_updated TIMESTAMP)
      """
    )
    assert(resultAdGroup.isSuccess && resultAdGroup.toOption.get === false)

    val resultCampaign = jdbcConnection.get.execute(
      """
        CREATE TABLE campaign_postgres (
          id NUMERIC
          , name  VARCHAR(255)
          , advertiser_id NUMERIC
          , status VARCHAR(255)
          , created_date TIMESTAMP
          , last_updated TIMESTAMP)
      """
    )
    assert(resultCampaign.isSuccess && resultCampaign.toOption.get === false)

    val resultAdvertiser = jdbcConnection.get.execute(
      """
        CREATE TABLE advertiser_postgres (
          id NUMERIC
          , name  VARCHAR(255)
          , status VARCHAR(255)
          , created_date TIMESTAMP
          , last_updated TIMESTAMP)
      """
    )
    assert(resultAdvertiser.isSuccess && resultAdvertiser.toOption.get === false)

    val resultAdsStats = jdbcConnection.get.execute(
      """
        CREATE TABLE ad_stats_postgres (
          stats_date TIMESTAMP
          , ad_id NUMERIC
          , ad_group_id NUMERIC
          , campaign_id NUMERIC
          , advertiser_id NUMERIC
          , stats_source NUMERIC(3)
          , price_type NUMERIC(3)
          , impressions NUMERIC(19)
          , clicks NUMERIC(19)
          , spend NUMERIC(21,6)
          , max_bid NUMERIC(21,6))
      """
    )
    assert(resultAdsStats.isSuccess && resultAdsStats.toOption.get === false)

    val insertSqlAdvertiser =
      """
        INSERT INTO advertiser_postgres (id, name, status, created_date, last_updated)
        VALUES (?, ?, ?, ?, ?)
      """

    val rowsAdvertiser: List[Seq[Any]] = List(
      Seq(1, "advertiser1", "ON", staticTimestamp, staticTimestamp2)
    )

    insertRows(insertSqlAdvertiser, rowsAdvertiser, "SELECT * FROM advertiser_postgres")

    val insertSqlCampaign =
      """
        INSERT INTO campaign_postgres (id, name, advertiser_id, status, created_date, last_updated)
        VALUES (?, ?, ?, ?, ?, ?)
      """

    val rowsCampaigns: List[Seq[Any]] = List(
      Seq(10, "campaign10", 1, "ON", staticTimestamp, staticTimestamp2)
      , Seq(11, "campaign11", 1, "ON", staticTimestamp, staticTimestamp2)
    )

    insertRows(insertSqlCampaign, rowsCampaigns, "SELECT * FROM campaign_postgres")

    val insertSqlAdGroup =
      """
        INSERT INTO ad_group_postgres (id, name, advertiser_id, campaign_id, status, created_date, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      """

    val rowsAdGroups: List[Seq[Any]] = List(
      Seq(100, "adgroup100", 1, 10, "ON", staticTimestamp, staticTimestamp2)
      , Seq(101, "adgroup101", 1, 10, "ON", staticTimestamp, staticTimestamp2)
      , Seq(102, "adgroup102", 1, 11, "ON", staticTimestamp, staticTimestamp2)
      , Seq(103, "adgroup103", 1, 11, "ON", staticTimestamp, staticTimestamp2)
    )

    insertRows(insertSqlAdGroup, rowsAdGroups, "SELECT * FROM ad_group_postgres")

    val insertSqlAds =
      """
        INSERT INTO ad_postgres (id, title, advertiser_id, campaign_id, ad_group_id, status, created_date, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      """
    val rowsAds: List[Seq[Any]] = List(
      Seq(1000, "adtitle1000", 1, 10, 100, "ON", staticTimestamp, staticTimestamp2)
      , Seq(1001, "adtitle1001", 1, 10, 100, "ON", staticTimestamp, staticTimestamp2)
      , Seq(1002, "adtitle1002", 1, 10, 101, "ON", staticTimestamp, staticTimestamp2)
      , Seq(1003, "adtitle1003", 1, 10, 101, "ON", staticTimestamp, staticTimestamp2)
      , Seq(1004, "adtitle1004", 1, 11, 102, "ON", staticTimestamp, staticTimestamp2)
      , Seq(1005, "adtitle1005", 1, 11, 102, "ON", staticTimestamp, staticTimestamp2)
      , Seq(1006, "adtitle1006", 1, 11, 103, "ON", staticTimestamp, staticTimestamp2)
      , Seq(1007, "adtitle1007", 1, 11, 103, "ON", staticTimestamp, staticTimestamp2)
    )

    insertRows(insertSqlAds, rowsAds, "SELECT * FROM ad_postgres")

    val insertSqlAdsStats =
      """
        INSERT INTO ad_stats_postgres 
        (stats_date, ad_id, ad_group_id, campaign_id, advertiser_id, stats_source, price_type, impressions, clicks, spend, max_bid)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """
    val sd = new Date(System.currentTimeMillis())
    val rowsAdsStats: List[Seq[Any]] = List(
      Seq(sd, 1000, 100, 10, 1, 1, 1, 1002, 2, 2.10, 0.21)
      , Seq(sd, 1000, 100, 10, 1, 1, 2, 1003, 3, 3.10, 0.31)
      , Seq(sd, 1000, 100, 10, 1, 2, 1, 1003, 3, 3.10, 0.31)
      , Seq(sd, 1000, 100, 10, 1, 2, 2, 1004, 4, 4.10, 0.41)
      , Seq(sd, 1001, 100, 10, 1, 1, 1, 1003, 3, 3.10, 0.31)
      , Seq(sd, 1001, 100, 10, 1, 1, 2, 1004, 4, 4.10, 0.41)
      , Seq(sd, 1001, 100, 10, 1, 2, 1, 1004, 4, 4.10, 0.41)
      , Seq(sd, 1001, 100, 10, 1, 2, 2, 1005, 5, 5.10, 0.51)
      , Seq(sd, 1002, 101, 10, 1, 1, 1, 1004, 4, 4.10, 0.41)
      , Seq(sd, 1002, 101, 10, 1, 1, 2, 1005, 5, 5.10, 0.51)
      , Seq(sd, 1002, 101, 10, 1, 2, 1, 1005, 5, 5.10, 0.51)
      , Seq(sd, 1002, 101, 10, 1, 2, 2, 1006, 6, 6.10, 0.61)
      , Seq(sd, 1003, 101, 10, 1, 1, 1, 1005, 5, 5.10, 0.51)
      , Seq(sd, 1003, 101, 10, 1, 1, 2, 1006, 6, 6.10, 0.61)
      , Seq(sd, 1003, 101, 10, 1, 2, 1, 1006, 6, 6.10, 0.61)
      , Seq(sd, 1003, 101, 10, 1, 2, 2, 1007, 7, 7.10, 0.71)
      , Seq(sd, 1004, 102, 11, 1, 1, 1, 1006, 6, 6.10, 0.61)
      , Seq(sd, 1004, 102, 11, 1, 1, 2, 1007, 7, 7.10, 0.71)
      , Seq(sd, 1004, 102, 11, 1, 2, 1, 1007, 7, 7.10, 0.71)
      , Seq(sd, 1004, 102, 11, 1, 2, 2, 1008, 8, 8.10, 0.81)
      , Seq(sd, 1005, 102, 11, 1, 1, 1, 1007, 7, 7.10, 0.71)
      , Seq(sd, 1005, 102, 11, 1, 1, 2, 1008, 8, 8.10, 0.81)
      , Seq(sd, 1005, 102, 11, 1, 2, 1, 1008, 8, 8.10, 0.81)
      , Seq(sd, 1005, 102, 11, 1, 2, 2, 1009, 9, 9.10, 0.91)
      , Seq(sd, 1006, 103, 11, 1, 1, 1, 1008, 8, 8.10, 0.81)
      , Seq(sd, 1006, 103, 11, 1, 1, 2, 1009, 9, 9.10, 0.91)
      , Seq(sd, 1006, 103, 11, 1, 2, 1, 1009, 9, 9.10, 0.91)
      , Seq(sd, 1006, 103, 11, 1, 2, 2, 1010, 10, 10.10, 1.01)
      , Seq(sd, 1007, 103, 11, 1, 1, 1, 1009, 9, 9.10, 0.91)
      , Seq(sd, 1007, 103, 11, 1, 1, 2, 1010, 10, 10.10, 1.01)
      , Seq(sd, 1007, 103, 11, 1, 2, 1, 1010, 10, 10.10, 1.01)
      , Seq(sd, 1007, 103, 11, 1, 2, 2, 1011, 11, 11.10, 1.11)
    )

    insertRows(insertSqlAdsStats, rowsAdsStats, "SELECT * FROM ad_stats_postgres")
  }

  lazy val defaultRegistry = getDefaultRegistry()

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
                            {"field": "Impressions"},
                            {"field": "Clicks"},
                            {"field": "Max Bid"},
                            {"field": "Average CPC"},
                            {"field": "Spend"},
                            {"field": "CTR Percentage"},
                            {"field": "CTR"},
                            {"field": "Year"},
                            {"field": "Count"}
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

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    
    val queryPipeline = queryPipelineTry.toOption.get
    val sqlQuery =  queryPipeline.queryChain.drivingQuery.asInstanceOf[PostgresQuery].asString
    
    val result = queryPipeline.execute(queryExecutorContext)
    result match {
      case scala.util.Success(queryPipelineResult) =>
        assert(!queryPipelineResult.rowList.isEmpty)
      case any =>
        throw new UnsupportedOperationException(s"unexpected row list : $any")
    }
  }

  test("successfully execute sync query for ad_stats") {
    val jsonString = s"""{
                          "cube": "ad_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Hour"},
                            {"field": "Campaign ID"},
                            {"field": "Ad Group ID"},
                            {"field": "Ad ID"},
                            {"field": "Ad Title"},
                            {"field": "Ad Status"},
                            {"field": "Ad Date Created"},
                            {"field": "Ad Date Modified"},
                            {"field": "Ad Date Modified Timestamp"},
                            {"field": "Pricing Type"},
                            {"field": "Impressions"},
                            {"field": "Clicks"},
                            {"field": "Max Bid"},
                            {"field": "Average CPC"},
                            {"field": "Spend"},
                            {"field": "CTR"},
                            {"field": "Count"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "1"},
                            {"field": "Pricing Type", "operator": "in", "values": ["CPC","CPA"] }
                          ],
                          "sortBy": [
                            {"field": "Ad Title", "order": "Desc"}
                          ],
                          "paginationStartIndex":1,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val sqlQuery =  queryPipeline.queryChain.drivingQuery.asInstanceOf[PostgresQuery].asString

    val result = queryPipeline.execute(queryExecutorContext)
    result match {
      case scala.util.Success(queryPipelineResult) =>
        assert(!queryPipelineResult.rowList.isEmpty)
      case any =>
        throw new UnsupportedOperationException(s"unexpected row list : $any")
    }
  }

  test("successfully execute sync query for ad_stats with greater than filter") {
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
                            {"field": "Impressions"},
                            {"field": "Clicks"},
                            {"field": "Max Bid"},
                            {"field": "Average CPC"},
                            {"field": "Spend"},
                            {"field": "CTR"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "1"},
                            {"field": "Impressions", "operator": ">", "value": "1007" },
                            {"field": "Pricing Type", "operator": "in", "values": ["CPC","CPA"] }
                          ],
                          "sortBy": [
                            {"field": "Ad Title", "order": "Desc"}
                          ],
                          "paginationStartIndex":1,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val sqlQuery =  queryPipeline.queryChain.drivingQuery.asInstanceOf[PostgresQuery].asString

    val result = queryPipeline.execute(queryExecutorContext)

    result match {
      case scala.util.Success(queryPipelineResult) =>
        assert(!queryPipelineResult.rowList.isEmpty)
        val inmem = queryPipelineResult.rowList
        assert(!inmem.isEmpty)
        inmem.foreach {
          row =>
            row.getValue("Ad ID").toString match {
              case "1004" | "1005" | "1006" | "1007" =>
                assert(true)
              case any =>
                assert(false)
            }
        }
      case any =>
        throw new UnsupportedOperationException(s"unexpected row list : $any")
    }
  }

  test("successfully execute sync query for ad_stats with less than filter") {
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
                            {"field": "Impressions"},
                            {"field": "Clicks"},
                            {"field": "Max Bid"},
                            {"field": "Average CPC"},
                            {"field": "Spend"},
                            {"field": "CTR"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "1"},
                            {"field": "Impressions", "operator": "<", "value": "1007" },
                            {"field": "Pricing Type", "operator": "in", "values": ["CPC","CPA"] }
                          ],
                          "sortBy": [
                            {"field": "Ad Title", "order": "Desc"}
                          ],
                          "paginationStartIndex":1,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val sqlQuery =  queryPipeline.queryChain.drivingQuery.asInstanceOf[PostgresQuery].asString

    val result = queryPipeline.execute(queryExecutorContext)

    result match {
      case scala.util.Success(queryPipelineResult) =>
        assert(!queryPipelineResult.rowList.isEmpty)
        val inmem = queryPipelineResult.rowList
        assert(!inmem.isEmpty)
        inmem.foreach {
          row =>
            row.getValue("Ad ID").toString match {
              case "1000" | "1001" | "1002" =>
                assert(true)
              case any =>
                assert(false)
            }
        }
      case any =>
        throw new UnsupportedOperationException(s"unexpected row list : $any")
    }
  }


  test("successfully execute dim driven sync query for ad_group_stats") {
    withMockDruidQueryExecutor(rl => rl.foreach(r => r)) {
      val jsonString =
        s"""{
                          "cube": "ad_group_stats",
                          "forceDimensionDriven": true,
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Campaign ID"},
                            {"field": "Ad Group ID"},
                            {"field": "Ad Group Name"},
                            {"field": "Ad Group Status"},
                            {"field": "Ad Group Date Created"},
                            {"field": "Ad Group Date Modified"},
                            {"field": "Pricing Type"},
                            {"field": "Impressions"},
                            {"field": "Clicks"},
                            {"field": "Max Bid"},
                            {"field": "Average CPC"},
                            {"field": "Spend"},
                            {"field": "CTR Percentage"},
                            {"field": "CTR"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "1"}
                          ],
                          "sortBy": [
                            {"field": "Ad Group Name", "order": "Desc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""

      val request: ReportingRequest = getReportingRequestSync(jsonString).copy(additionalParameters = Map(Parameter.Debug -> DebugValue(true)))
      val registry = defaultRegistry
      val requestModel = RequestModel.from(request, registry)
      assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

      val queryPipelineTry = generatePipeline(requestModel.toOption.get)
      assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

      val queryPipeline = queryPipelineTry.toOption.get
      val sqlQuery = queryPipeline.queryChain.drivingQuery.asInstanceOf[PostgresQuery].asString

      val result = queryPipeline.execute(queryExecutorContext)
      result match {
        case scala.util.Success(queryPipelineResult) =>
          assert(!queryPipelineResult.rowList.isEmpty)
        case any =>
          throw new UnsupportedOperationException(s"unexpected row list : $any")
      }
    }
  }

  test("successfully execute fact driven sync query for ad_group_stats") {
    withMockDruidQueryExecutor{ 
      rl => 
        for(i <- Set(100, 101, 103)) {
          val row = rl.newRow
          row.addValue("Ad Group ID", i)
          row.addValue("Impressions", i * 10)
          rl.addRow(row)
        }
    } {
      val jsonString = s"""{
                          "cube": "ad_group_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Campaign ID"},
                            {"field": "Ad Group ID"},
                            {"field": "Ad Group Name"},
                            {"field": "Ad Group Status"},
                            {"field": "Ad Group Date Created"},
                            {"field": "Ad Group Date Modified"},
                            {"field": "Pricing Type"},
                            {"field": "Impressions"},
                            {"field": "Clicks"},
                            {"field": "Max Bid"},
                            {"field": "Average CPC"},
                            {"field": "Spend"},
                            {"field": "CTR Percentage"},
                            {"field": "CTR"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "1"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""

      val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
      val registry = defaultRegistry
      val requestModel = RequestModel.from(request, registry)
      assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

      val queryPipelineTry = generatePipeline(requestModel.toOption.get)
      assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

      val queryPipeline = queryPipelineTry.toOption.get
      val result = queryPipeline.execute(queryExecutorContext)
      result match {
        case scala.util.Success(queryPipelineResult) =>
          val inmem = queryPipelineResult.rowList
          assert(!inmem.isEmpty)
          inmem.foreach {
            row =>
              
              row.getValue("Ad Group ID").toString match {
                case "100" | "101" | "103" =>
                  assert(row.getValue("Impressions") != null)
                case "102" =>
                  assert(row.getValue("Impressions") == null)
              }
            
          }
        case any =>
          throw new UnsupportedOperationException(s"unexpected row list : $any")
      }
    }
  }

  test("successfully execute dim driven dim only sync query for ad_stats") {
    val jsonString = s"""{
                          "cube": "ad_stats",
                          "selectFields": [
                            {"field": "Campaign ID"},
                            {"field": "Ad Group ID"},
                            {"field": "Ad ID"},
                            {"field": "Ad Title"},
                            {"field": "Ad Status"},
                            {"field": "Ad Date Created"},
                            {"field": "Ad Date Modified"},
                            {"field": "Ad Date Modified Timestamp"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "1"}
                          ],
                          "sortBy": [
                            {"field": "Ad Title", "order": "Desc"}
                          ],
                          "paginationStartIndex":1,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val sqlQuery =  queryPipeline.queryChain.drivingQuery.asInstanceOf[PostgresQuery].asString
    
    val result = queryPipeline.execute(queryExecutorContext)
    result match {
      case scala.util.Success(queryPipelineResult) =>
        val inmem = queryPipelineResult.rowList
        assert(!inmem.isEmpty)
      case any =>
        throw new UnsupportedOperationException(s"unexpected row list : $any")
    }
  }


  test("Execution Failure test") {
    val jsonString = s"""{
                          "cube": "ad_stats",
                          "selectFields": [
                            {"field": "Campaign ID"},
                            {"field": "Ad Group ID"},
                            {"field": "Ad ID"},
                            {"field": "Ad Title"},
                            {"field": "Ad Status"},
                            {"field": "Ad Unknown Column"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "1"}
                          ],
                          "sortBy": [
                            {"field": "Ad Title", "order": "Desc"}
                          ],
                          "paginationStartIndex":1,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val sqlQuery =  queryPipeline.queryChain.drivingQuery.asInstanceOf[PostgresQuery].asString
    
    val result = queryPipeline.execute(queryExecutorContext)
    assert(result.isFailure)
    assert(result.failed.get.getMessage.contains("""ERROR: column "unknown" does not exist"""))
  }


  test("Invalid definition test") {
    val jsonString = s"""{
                          "cube": "ad_stats_invalid",
                          "selectFields": [
                            {"field": "Campaign ID"},
                            {"field": "Ad Group ID"},
                            {"field": "Ad ID"},
                            {"field": "Ad Title"},
                            {"field": "Ad Status"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "1"}
                          ],
                          "sortBy": [
                            {"field": "Ad Title", "order": "Desc"}
                          ],
                          "paginationStartIndex":1,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

      //override def query: Query = {q}
    val queryPipeline = queryPipelineFactory.builder(requestModel.toOption.get, QueryAttributes.empty, None, BucketParams())._1.get
      .withRowListFunction(q => new DimDrivenPartialRowList(RowGrouping("Campaign ID", List.empty), q) {
        (q)
      }).build()

    val sqlQuery =  queryPipeline.queryChain.drivingQuery.asInstanceOf[PostgresQuery].asString
    println(sqlQuery)
    val result = queryPipeline.execute(queryExecutorContext)
    assert(result.isFailure)
    assert(result.failed.get.getMessage.contains("""column "impressions_invalid" does not exist"""))
  }

  test("test null result") {
    import org.mockito.Matchers._
    import org.mockito.Mockito
    var resultSet: ResultSet = null
    var executor : PostgresQueryExecutor = Mockito.spy(postgresQueryExecutor.get)
    val today = new Date(1515794890000L)
    jdbcConnection.get.queryForList("select * from ad_stats_postgres where ad_id=1000 limit 1") {
      rs => {
        resultSet = Mockito.spy(rs)
        Mockito.doNothing().when(resultSet).close()
        Mockito.doReturn(null).when(resultSet).getBigDecimal(anyInt())
        Mockito.doReturn(null).when(resultSet).getDate(1)
        Mockito.doReturn(today).when(resultSet).getDate(2)
        Mockito.doReturn(null).when(resultSet).getTimestamp(anyInt())
      }
    }

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

  test("successfully execute driving and non union dim only query with pagination") {
    val jsonString = s"""{
                          "cube": "ad_stats",
                          "selectFields": [
                            {"field": "Ad ID"},
                            {"field": "Ad Title"},
                            {"field": "Ad Status"},
                            {"field": "Ad Date Created"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "1"}
                          ],
                          "sortBy": [
                            {"field": "Ad Title", "order": "Desc"}
                          ],
                          "paginationStartIndex":2,
                          "rowsPerPage":10,
                          "forceDimensionDriven" : true
                        }"""

    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val sqlQuery =  queryPipeline.queryChain.drivingQuery.asInstanceOf[PostgresQuery].asString
    println(sqlQuery)

    val result = queryPipeline.execute(queryExecutorContext)
    result match {
      case scala.util.Success(queryPipelineResult) =>
        val inmem = queryPipelineResult.rowList
        assert(!inmem.isEmpty)
      case any =>
        throw new UnsupportedOperationException(s"unexpected row list : $any")
    }
  }
}
