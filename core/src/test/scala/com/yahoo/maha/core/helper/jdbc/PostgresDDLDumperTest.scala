package com.yahoo.maha.core.helper.jdbc

import java.io.{BufferedOutputStream, File, FileOutputStream, PrintWriter}

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import com.yahoo.maha.core.CoreSchema.AdvertiserSchema
import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core.dimension.{PostgresDerDimCol, PubCol}
import com.yahoo.maha.core.fact.{Fact, FactCol, PostgresCustomRollup, PostgresFactDimDrivenHint, PostgresFactStaticHint, PostgresPartitioningScheme, PublicFactCol, RoaringBitmapFkFactMapStorage, SumRollup}
import com.yahoo.maha.core.registry.RegistryBuilder
import com.yahoo.maha.core.request.SyncRequest
import com.yahoo.maha.core.{ColumnContext, DailyGrain, DateType, DecType, PostgresEngine, StrType}
import com.yahoo.maha.jdbc.JdbcConnection
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.apache.commons.lang3.StringUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.SortedSet
import scala.io.Source

class PostgresDDLDumperTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {
  private var dataSource: Option[HikariDataSource] = None
  private var jdbcConnection: Option[JdbcConnection] = None
  val userDir = System.getProperty("user.dir")
  if (StringUtils.isNotBlank(userDir)) {
    System.setProperty("java.io.tmpdir", userDir + "/target")
  }
  private val pg = EmbeddedPostgres.builder().setPort(5433).start()
  val ddlFile = new File("src/test/resources/pg-ddl-dumper.sql")
  val ddlString = Source.fromFile(ddlFile).getLines.mkString

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val config = new HikariConfig()
    val jdbcUrl = pg.getJdbcUrl("postgres", "postgres")
    config.setJdbcUrl(jdbcUrl)
    config.setUsername("postgres")
    config.setPassword("")
    config.setMaximumPoolSize(1)
    dataSource = Option(new HikariDataSource(config))
    jdbcConnection = dataSource.map(new JdbcConnection(_))
    val tryExecuteDDL = jdbcConnection.get.execute(ddlString)
    require(tryExecuteDDL.isSuccess, s"Failed to run DDL : $tryExecuteDDL")
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    dataSource.foreach(_.close())
  }

  test("successfully dump ddl to file") {
    val schemaDumpTry = JdbcSchemaDumper.dump(jdbcConnection.get)
    assert(schemaDumpTry.isSuccess, s"Failed to dump schema : ${schemaDumpTry}")
    val config = DDLDumpConfig(IndexedSeq(LikeCriteria("public", "account_stats%"))
      , IndexedSeq(LikeCriteria("public", "update_%")), IndexedSeq(LikeCriteria("public", "update_%")))
    val ddlOutFile = new java.io.File("src/test/resources/pg-ddl-dumper-generated.sql")
    val ddlOutputStream = new BufferedOutputStream(new FileOutputStream(ddlOutFile))
    val ddlWriter = new PrintWriter(ddlOutputStream)
    PostgresDDLDumper.dump(jdbcConnection.get, schemaDumpTry.get, ddlWriter, config)
    ddlWriter.close()
    val ddlGeneratedString = Source.fromFile(ddlOutFile).getLines.mkString
    assert(ddlString === ddlGeneratedString)

    import com.yahoo.maha.core.PostgresExpression._
    val dimConfig = DimConfig(
      defaults = DimDefaultConfig(PostgresEngine, SortedSet(AdvertiserSchema), None)
      , tables = SortedSet("advertiser", "campaign", "ad_group", "ad", "targetingattribute")
      , tableOverrides = Map(
        "advertiser" -> DimTableOverrides(
          ignoreColumns = Set("device_id")
          , derivedColumns = { implicit cc: ColumnContext =>
            IndexedSeq(
              DerivedDimColumnDef(
                PostgresDerDimCol("Advertiser Status", StrType(), DECODE_DIM("{status}", "'ON'", "'ON'", "'OFF'"))
                , PubCol("Advertiser Status", "Advertiser Status", InEquality)
              )
            )
          }
        )
      )
    )
    val dims = DimGenerator.buildPublicDimensions(schemaDumpTry.toOption.get, dimConfig, true)
    assert(dims.size === 5)
    val factConfig = FactConfig(
      defaults = FactDefaultConfig(
        engine = PostgresEngine
        , schemas = Set(AdvertiserSchema)
        , dayField = "stats_date"
        , grain = DailyGrain
        , publicDefaults = PublicFactDefaultConfig(
          forcedFilters = Set.empty
          , maxDaysWindow = Map((SyncRequest, DailyGrain) -> 30)
          , maxDaysLookBack = Map((SyncRequest, DailyGrain) -> 7)
          , enableUTCTimeConversion = true
          , renderLocalTimeFilter = true
          , revision = 0
          , dimRevision = 0
          , dimToRevisionMap = Map.empty
          , requiredFilterColumns = Map(AdvertiserSchema -> Set("Advertiser Id"))
          , powerSetStorage = RoaringBitmapFkFactMapStorage()
        )
        , annotations = Set.empty
        , defaultRollupExpression = SumRollup
        , defaultPublicFactFilterOperations = InBetweenEquality
        , costMultiplierMap = Fact.DEFAULT_COST_MULTIPLIER_MAP
        , forceFilters = Set.empty
        , defaultCardinality = 100
        , defaultRowCount = 1000
      )
      , tables = SortedSet(CubeName("campaign_stats", "campaign_stats"))
      , tableOverrides = Map(
        "campaign_stats" -> FactTableOverrides(
          annotations = Option(
            Set(
              PostgresFactStaticHint("PARALLEL_INDEX(cb_campaign_k_stats 4)"),
              PostgresFactDimDrivenHint("PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4)"),
              PostgresPartitioningScheme("frequency")
            )
          )
          , derivedDimColumns = { implicit cc: ColumnContext =>
            IndexedSeq(
              DerivedDimColumnDef(
                PostgresDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
                , PubCol("Month", "Month", Equality)
              )
              , DerivedDimColumnDef(
                PostgresDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "W"))
                , PubCol("Week", "Week", Equality)
              )
            )
          }
          , derivedFactColumns = { implicit cc: ColumnContext =>
            IndexedSeq(
              DerivedFactColumnDef(
                FactCol("Average CPC", DecType(), PostgresCustomRollup(SUM("{spend}") / SUM("{clicks}")))
                , PublicFactCol("Average CPC", "Average CPC", InBetweenEquality)
              )
            )
          }
        )
      )
    )
    val facts = FactGenerator.buildPublicFacts(schemaDumpTry.toOption.get, factConfig, true)
    assert(facts.size === 1)

    val rb = new RegistryBuilder
    SchemaGenerator.generate(rb, schemaDumpTry.toOption.get, dimConfig, factConfig)
    val registry = rb.build()
    println(registry.domainJsonAsString)
  }
}

