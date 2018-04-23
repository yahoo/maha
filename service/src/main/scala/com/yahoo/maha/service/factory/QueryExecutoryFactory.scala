// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import com.yahoo.maha.executor.oracle.OracleQueryExecutor
import com.yahoo.maha.executor.presto.{PrestoQueryExecutor, PrestoQueryTemplate}
import com.yahoo.maha.service.MahaServiceConfig
import com.yahoo.maha.core.query.{ExecutionLifecycleListener, QueryAttributes, QueryContext, QueryExecutor}
import com.yahoo.maha.core.request._
import com.yahoo.maha.executor.druid.{DruidQueryExecutor, DruidQueryExecutorConfig, ResultSetTransformers}
import com.yahoo.maha.jdbc.JdbcConnection
import org.json4s.JValue

import scalaz.Validation.FlatMap._
import scalaz.syntax.applicative._

/**
 * Created by pranavbhole on 25/05/17.
 */
class OracleQueryExecutoryFactory extends QueryExecutoryFactory {
  """
    |{
    |"dataSourceFactoryClass": "",
    |"dataSourceFactoryConfig": [],
    |"jdbcConnectionFetchSize" 10,
    |"lifecycleListenerFactoryClass": "",
    |"lifecycleListenerFactoryConfig" : []
    |}
  """.stripMargin
  override def fromJson(configJson: JValue): MahaServiceConfig.MahaConfigResult[QueryExecutor] =  {
    import org.json4s.scalaz.JsonScalaz._
    val dataSourceFactoryClassResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("dataSourceFactoryClass")(configJson)
    val dataSourceFactoryConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("dataSourceFactoryConfig")(configJson)
    val jdbcConnectionFetchSizeOptionResult: MahaServiceConfig.MahaConfigResult[Option[Int]] = fieldExtended[Option[Int]]("jdbcConnectionFetchSize")(configJson)
    val lifecycleListenerFactoryClassResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("lifecycleListenerFactoryClass")(configJson)
    val lifecycleListenerFactoryConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("lifecycleListenerFactoryConfig")(configJson)

    val jdbcConnetionResult : MahaServiceConfig.MahaConfigResult[JdbcConnection] = for {
      dataSourceFactoryClass <- dataSourceFactoryClassResult
      dataSourceFactoryConfig <- dataSourceFactoryConfigResult
      dataSourceFactory <-  getFactory[DataSourceFactory](dataSourceFactoryClass, this.closer)
      dataSource <- dataSourceFactory.fromJson(dataSourceFactoryConfig)
      jdbcConnectionFetchSizeOption <- jdbcConnectionFetchSizeOptionResult
    } yield {
        if(jdbcConnectionFetchSizeOption.isDefined) {
          new JdbcConnection(dataSource, jdbcConnectionFetchSizeOption.get)
        } else {
          new JdbcConnection(dataSource)
        }
      }

    val lifecycleListener : MahaServiceConfig.MahaConfigResult[ExecutionLifecycleListener] = for {
      lifecycleListenerFactoryClass <- lifecycleListenerFactoryClassResult
      lifecycleListenerFactoryConfig <- lifecycleListenerFactoryConfigResult
      lifecycleListenerFactory <- getFactory[ExecutionLifecycleListenerFactory](lifecycleListenerFactoryClass, this.closer)
      lifecycleListener <- lifecycleListenerFactory.fromJson(lifecycleListenerFactoryConfig)
    } yield lifecycleListener

    (jdbcConnetionResult |@| lifecycleListener) {
      (a, b) =>
      new OracleQueryExecutor(a, b)
    }
  }

  override def supportedProperties: List[(String, Boolean)] = ???
}

class DruidQueryExecutoryFactory extends QueryExecutoryFactory {

  """
    |{
    |"druidQueryExecutorConfigFactoryClassName" : "",
    |"druidQueryExecutorConfigJsonConfig" :{},
    |"lifecycleListenerFactoryClass" : "",
    |"lifecycleListenerFactoryConfig" : [],
    |"resultSetTransformersFactoryClassName": "",
    |"resultSetTransformersFactoryConfig": {}
    |}
  """.stripMargin

  override def fromJson(configJson: JValue): MahaServiceConfig.MahaConfigResult[QueryExecutor] =  {
    import org.json4s.scalaz.JsonScalaz._
    val druidQueryExecutorConfigFactoryClassNameResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("druidQueryExecutorConfigFactoryClassName")(configJson)
    val druidQueryExecutorConfigJsonConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("druidQueryExecutorConfigJsonConfig")(configJson)
    val lifecycleListenerFactoryClassResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("lifecycleListenerFactoryClass")(configJson)
    val lifecycleListenerFactoryConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("lifecycleListenerFactoryConfig")(configJson)
    val resultSetTransformersFactoryClassNameResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("resultSetTransformersFactoryClassName")(configJson)
    val resultSetTransformersFactoryConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("resultSetTransformersFactoryConfig")(configJson)

    val druidQueryExecutorConfig :  MahaServiceConfig.MahaConfigResult[DruidQueryExecutorConfig] = for {
      druidQueryExecutorConfigFactoryClassName <- druidQueryExecutorConfigFactoryClassNameResult
      druidQueryExecutorConfigJsonConfig <- druidQueryExecutorConfigJsonConfigResult
      druidQueryExecutorConfigFactory <- getFactory[DruidQueryExecutorConfigFactory](druidQueryExecutorConfigFactoryClassName, this.closer)
      druidQueryExecutorConfig <- druidQueryExecutorConfigFactory.fromJson(druidQueryExecutorConfigJsonConfig)
    } yield  druidQueryExecutorConfig

    val lifecycleListener : MahaServiceConfig.MahaConfigResult[ExecutionLifecycleListener] = for {
      lifecycleListenerFactoryClass <- lifecycleListenerFactoryClassResult
      lifecycleListenerFactoryConfig <- lifecycleListenerFactoryConfigResult
      lifecycleListenerFactory <- getFactory[ExecutionLifecycleListenerFactory](lifecycleListenerFactoryClass, this.closer)
      lifecycleListener <- lifecycleListenerFactory.fromJson(lifecycleListenerFactoryConfig)
    } yield lifecycleListener

    val resultSetTransformers : MahaServiceConfig.MahaConfigResult[List[ResultSetTransformers]] = for {
      resultSetTransformersFactoryClassName <- resultSetTransformersFactoryClassNameResult
      resultSetTransformersFactoryConfig <- resultSetTransformersFactoryConfigResult
      resultSetTransformersFactory <- getFactory[ResultSetTransformersFactory](resultSetTransformersFactoryClassName, this.closer)
      resultSetTransformers <- resultSetTransformersFactory.fromJson(resultSetTransformersFactoryConfig)
    } yield resultSetTransformers

    (druidQueryExecutorConfig |@| lifecycleListener |@| resultSetTransformers) {
      (a, b, c) => {

            val executor = new DruidQueryExecutor(a, b, c)
            closer.register(executor)
            executor
          }
    }
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class PrestoQueryExecutoryFactory extends QueryExecutoryFactory {

  """
    |{
    |"dataSourceFactoryClass": "",
    |"dataSourceFactoryConfig": [],
    |"jdbcConnectionFetchSize" 10,
    |"lifecycleListenerFactoryClass" : "",
    |"lifecycleListenerFactoryConfig" : []
    |}
  """.stripMargin

  override def fromJson(configJson: JValue): MahaServiceConfig.MahaConfigResult[QueryExecutor] =  {
    import org.json4s.scalaz.JsonScalaz._

    val dataSourceFactoryClassResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("dataSourceFactoryClass")(configJson)
    val dataSourceFactoryConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("dataSourceFactoryConfig")(configJson)
    val jdbcConnectionFetchSizeOptionResult: MahaServiceConfig.MahaConfigResult[Option[Int]] = fieldExtended[Option[Int]]("jdbcConnectionFetchSize")(configJson)
    val lifecycleListenerFactoryClassResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("lifecycleListenerFactoryClass")(configJson)
    val lifecycleListenerFactoryConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("lifecycleListenerFactoryConfig")(configJson)
    val prestoQueryStringResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("prestoQueryString")(configJson)

    val jdbcConnetionResult : MahaServiceConfig.MahaConfigResult[JdbcConnection] = for {
      dataSourceFactoryClass <- dataSourceFactoryClassResult
      dataSourceFactoryConfig <- dataSourceFactoryConfigResult
      dataSourceFactory <-  getFactory[DataSourceFactory](dataSourceFactoryClass, this.closer)
      dataSource <- dataSourceFactory.fromJson(dataSourceFactoryConfig)
      jdbcConnectionFetchSizeOption <- jdbcConnectionFetchSizeOptionResult
    } yield {
      if(jdbcConnectionFetchSizeOption.isDefined) {
        new JdbcConnection(dataSource, jdbcConnectionFetchSizeOption.get)
      } else {
        new JdbcConnection(dataSource)
      }
    }

    val lifecycleListener : MahaServiceConfig.MahaConfigResult[ExecutionLifecycleListener] = for {
      lifecycleListenerFactoryClass <- lifecycleListenerFactoryClassResult
      lifecycleListenerFactoryConfig <- lifecycleListenerFactoryConfigResult
      lifecycleListenerFactory <- getFactory[ExecutionLifecycleListenerFactory](lifecycleListenerFactoryClass, this.closer)
      lifecycleListener <- lifecycleListenerFactory.fromJson(lifecycleListenerFactoryConfig)
    } yield lifecycleListener

    val prestoQueryTemplate : MahaServiceConfig.MahaConfigResult[PrestoQueryTemplate] = for {
      prestoQueryString <- prestoQueryStringResult
      prestoQueryTemplate <- new PrestoQueryTemplate {
        override def buildFinalQuery(query: String, queryContext: QueryContext, queryAttributes: QueryAttributes): String = query
      }
    } yield prestoQueryTemplate

    (jdbcConnetionResult |@| prestoQueryTemplate |@| lifecycleListener) {
      (a, b, c) => {

        val executor = new PrestoQueryExecutor(a, b, c)
        closer.register(executor)
        executor
      }
    }
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}
