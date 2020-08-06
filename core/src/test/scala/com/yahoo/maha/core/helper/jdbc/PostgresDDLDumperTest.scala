package com.yahoo.maha.core.helper.jdbc

import java.io.{BufferedOutputStream, File, FileOutputStream, PrintWriter}

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import com.yahoo.maha.jdbc.JdbcConnection
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.apache.commons.lang3.StringUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.io.Source

class PostgresDDLDumperTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {
  private var dataSource: Option[HikariDataSource] = None
  private var jdbcConnection: Option[JdbcConnection] = None
  val userDir = System.getProperty("user.dir")
  if (StringUtils.isNotBlank(userDir)) {
    System.setProperty("java.io.tmpdir", userDir+"/target")
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
    val config = DDLDumpConfig(IndexedSeq(LikeCriteria("public","account_stats%"))
      , IndexedSeq(LikeCriteria("public", "update_%")), IndexedSeq(LikeCriteria("public", "update_%")))
    val ddlOutFile = new java.io.File("src/test/resources/pg-ddl-dumper-generated.sql")
    val ddlOutputStream = new BufferedOutputStream(new FileOutputStream(ddlOutFile))
    val ddlWriter = new PrintWriter(ddlOutputStream)
    PostgresDDLDumper.dump(jdbcConnection.get, schemaDumpTry.get, ddlWriter, config)
    ddlWriter.close()
    val ddlGeneratedString = Source.fromFile(ddlOutFile).getLines.mkString
    assert(ddlString === ddlGeneratedString)
  }
}

