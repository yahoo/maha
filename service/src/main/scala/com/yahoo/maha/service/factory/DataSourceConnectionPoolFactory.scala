// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import java.util.Properties

import javax.sql.DataSource
import com.yahoo.maha.service.{MahaServiceConfig, MahaServiceConfigContext}
import com.yahoo.maha.service.MahaServiceConfig.MahaConfigResult
import com.yahoo.maha.service.config.PasswordProvider
import com.yahoo.maha.core.request._
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.json4s.JValue
import org.json4s.scalaz.JsonScalaz
import org.json4s.scalaz.JsonScalaz._
import _root_.scalaz._
import syntax.applicative._
import Validation.FlatMap._

/**
 * Created by pranavbhole on 31/05/17.
 */

case class KvPair(key: String, value: String)

// For Data Source Properties deserialising
object KvPair {
  implicit def fieldJSONW : JSONW[KvPair] = new JSONW[KvPair] {
    override def write(pair: KvPair): JValue =
      makeObj(
        ("key" -> toJSON(pair.key))
          :: ("value" -> toJSON(pair.value))
          :: Nil)
  }

  implicit def fieldJSONR: JSONR[KvPair] = new JSONR[KvPair] {
    override def read(json: JValue): JsonScalaz.Result[KvPair] = {
      val keyResult: JsonScalaz.Result[String] = fieldExtended[String]("key")(json)
      val valueResult: JsonScalaz.Result[String] = fieldExtended[String]("value")(json)
      (keyResult |@| valueResult) {
       (a, b) =>
       new KvPair(a, b)
      }
    }
  }
}


class HikariDataSourceFactory extends DataSourceFactory {
  """
    |{
    |"driverClassName" : "",
    |"jdbcUrl" : "",
    |"username" : "",
    |"passwordProviderFactoryClassName" : "com.yahoo.maha.service.factory.PassThroughPasswordProviderFactory",
    |"passwordProviderConfig" : "[{"key" : "value"}]",
    |"passwordKey" : "h2.test.database.password",
    |"poolName" : "",
    |"maximumPoolSize" : "",
    |"minimumIdle" : "",
    |"autoCommit": true,
    |"connectionTestQuery" : "select * from dual",
    |"validationTimeout" : "",
    |"idleTimeout" : "",
    |"maxLifetime" : "",
    |"dataSourceProperties": [{"key": "value"}]
    |}
  """.stripMargin

  override def fromJson(configJson: JValue)(implicit context: MahaServiceConfigContext): MahaConfigResult[DataSource] = {
    import org.json4s.scalaz.JsonScalaz._
    val driverClassNameResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("driverClassName")(configJson)
    val jdbcUrlResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("jdbcUrl")(configJson)
    val usernameResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("username")(configJson)
    val passwordProviderFactoryClassNameResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("passwordProviderFactoryClassName")(configJson)
    val passwordProviderConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("passwordProviderConfig")(configJson)
    val passwordKeyResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("passwordKey")(configJson)
    val poolNameResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("poolName")(configJson)
    val maximumPoolSizeResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("maximumPoolSize")(configJson)
    val minimumIdleResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("minimumIdle")(configJson)
    val autoCommitResult: MahaServiceConfig.MahaConfigResult[Boolean] = fieldExtended[Boolean]("autoCommit")(configJson)
    val connectionTestQueryResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("connectionTestQuery")(configJson)
    val validationTimeoutResult: MahaServiceConfig.MahaConfigResult[Long] = fieldExtended[Long]("validationTimeout")(configJson)
    val idleTimeoutResult: MahaServiceConfig.MahaConfigResult[Long] = fieldExtended[Long]("idleTimeout")(configJson)
    val maxLifetimeResult: MahaServiceConfig.MahaConfigResult[Long] = fieldExtended[Long]("maxLifetime")(configJson)
    val kvPairResult: MahaServiceConfig.MahaConfigResult[List[KvPair]] = fieldExtended[List[KvPair]]("dataSourceProperties")(configJson)

    val passwordProviderResult :MahaServiceConfig.MahaConfigResult[PasswordProvider] = for {
      passwordProviderFactoryClassName <- passwordProviderFactoryClassNameResult
      passwordProviderFactory <- getFactory[PasswordProviderFactory](passwordProviderFactoryClassName, this.closer)
      passwordProviderConfig <- passwordProviderConfigResult
      passwordProviderResult <- passwordProviderFactory.fromJson(passwordProviderConfig)
    } yield passwordProviderResult

    val dataSourcePropertiesResult: MahaServiceConfig.MahaConfigResult[Properties] =
      for {
        kvPairs <- kvPairResult
      } yield {
        val properties = new Properties()
        kvPairs.foreach {
          kvPair=>
            properties.put(kvPair.key, kvPair.value)
        }
        properties
      }

    val tuple6Result = (driverClassNameResult |@| jdbcUrlResult |@| usernameResult |@| passwordProviderResult |@| passwordKeyResult |@| poolNameResult) {
    (a,b,c,d,e,f) => (a,b,c,d,e,f)
    }
    (tuple6Result |@| maximumPoolSizeResult |@| minimumIdleResult |@| autoCommitResult
    |@| connectionTestQueryResult |@| validationTimeoutResult |@| idleTimeoutResult |@| maxLifetimeResult |@| dataSourcePropertiesResult) {
      case ((driverClassName, jdbcUrl, username, passwordProvider, passwordKey, poolName), maximumPoolSize, minimumIdle, autoCommit, connectionTestQuery,
        validationTimeout,idleTimeout, maxLifetime, dataSourceProperties) =>
        val hikariConfig = new HikariConfig()
        hikariConfig.setDriverClassName(driverClassName)
        hikariConfig.setJdbcUrl(jdbcUrl)
        hikariConfig.setUsername(username)
        hikariConfig.setPassword(passwordProvider.getPassword(passwordKey))
        hikariConfig.setPoolName(poolName)
        hikariConfig.setMaximumPoolSize(maximumPoolSize)
        hikariConfig.setMinimumIdle(minimumIdle)
        hikariConfig.setAutoCommit(autoCommit)
        hikariConfig.setConnectionInitSql(connectionTestQuery)
        hikariConfig.setValidationTimeout(validationTimeout)
        hikariConfig.setIdleTimeout(idleTimeout)
        hikariConfig.setMaxLifetime(maxLifetime)
        hikariConfig.setDataSourceProperties(dataSourceProperties)  //TO_DO
        val dataSource = new HikariDataSource(hikariConfig)
        closer.register(dataSource)
        dataSource
      }
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}
