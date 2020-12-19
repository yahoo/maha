// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import javax.sql.DataSource
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.request._
import com.yahoo.maha.executor.bigquery.{BigqueryQueryExecutor, BigqueryQueryExecutorConfig}
import com.yahoo.maha.executor.druid.{AuthHeaderProvider, DruidQueryExecutor, DruidQueryExecutorConfig}
import com.yahoo.maha.executor.oracle.OracleQueryExecutor
import com.yahoo.maha.executor.postgres.PostgresQueryExecutor
import com.yahoo.maha.executor.presto.{PrestoQueryExecutor, PrestoQueryTemplate}
import com.yahoo.maha.jdbc.JdbcConnection
import com.yahoo.maha.service.MahaServiceConfig.MahaConfigResult
import com.yahoo.maha.service.error.ServiceConfigurationError
import com.yahoo.maha.service.{MahaServiceConfig, MahaServiceConfigContext}
import org.json4s.JValue
import scalaz.Validation.FlatMap._
import scalaz.syntax.applicative._

/**
 * Created by pranavbhole on 25/05/17.
 */
class OracleQueryExecutoryFactory extends QueryExecutoryFactory {
  """
    |{
    |"dataSourceName": "",
    |"jdbcConnectionFetchSize" 10,
    |"lifecycleListenerFactoryClass": "",
    |"lifecycleListenerFactoryConfig" : []
    |}
  """.stripMargin
  override def fromJson(configJson: JValue)(implicit context:MahaServiceConfigContext): MahaServiceConfig.MahaConfigResult[QueryExecutor] =  {
    import org.json4s.scalaz.JsonScalaz._

    import scalaz.Validation.FlatMap._
    import scalaz.syntax.applicative._

    val dataSourceNameResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("dataSourceName")(configJson).map(_.toLowerCase)
    val jdbcConnectionFetchSizeOptionResult: MahaServiceConfig.MahaConfigResult[Option[Int]] = fieldExtended[Option[Int]]("jdbcConnectionFetchSize")(configJson)
    val lifecycleListenerFactoryClassResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("lifecycleListenerFactoryClass")(configJson)
    val lifecycleListenerFactoryConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("lifecycleListenerFactoryConfig")(configJson)

    import _root_.scalaz._
    import syntax.validation._

    for {
      dataSourceName <- dataSourceNameResult
    } yield  {
      if(!context.dataSourceMap.contains(dataSourceName)) {
        return Failure(List(ServiceConfigurationError(s"Failed to find Oracle dataSourceName $dataSourceName in dataSourceMap"))).toValidationNel.asInstanceOf[MahaConfigResult[QueryExecutor]]
      }
    }

    val jdbcConnetionResult : MahaServiceConfig.MahaConfigResult[JdbcConnection] = for {
      dataSourceName <- dataSourceNameResult
      dataSource <- context.dataSourceMap.get(dataSourceName).successNel[Option[DataSource]].asInstanceOf[MahaServiceConfig.MahaConfigResult[Option[DataSource]]]
      jdbcConnectionFetchSizeOption <- jdbcConnectionFetchSizeOptionResult
    } yield {
        if(jdbcConnectionFetchSizeOption.isDefined) {
          new JdbcConnection(dataSource.get, jdbcConnectionFetchSizeOption.get)
        } else {
          new JdbcConnection(dataSource.get)
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
    |"resultSetTransformersFactoryConfig": {},
    |"authHeaderProviderFactoryClassName": "",
    |"authHeaderProviderFactoryConfig": ""
    |}
  """.stripMargin

  override def fromJson(configJson: JValue)(implicit context:MahaServiceConfigContext): MahaServiceConfig.MahaConfigResult[QueryExecutor] =  {
    import org.json4s.scalaz.JsonScalaz._
    val druidQueryExecutorConfigFactoryClassNameResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("druidQueryExecutorConfigFactoryClassName")(configJson)
    val druidQueryExecutorConfigJsonConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("druidQueryExecutorConfigJsonConfig")(configJson)
    val lifecycleListenerFactoryClassResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("lifecycleListenerFactoryClass")(configJson)
    val lifecycleListenerFactoryConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("lifecycleListenerFactoryConfig")(configJson)
    val resultSetTransformersFactoryClassNameResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("resultSetTransformersFactoryClassName")(configJson)
    val resultSetTransformersFactoryConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("resultSetTransformersFactoryConfig")(configJson)
    val authHeaderProviderFactoryClassNameResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("authHeaderProviderFactoryClassName")(configJson)
    val authHeaderProviderFactoryConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("authHeaderProviderFactoryConfig")(configJson)

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

    val resultSetTransformers : MahaServiceConfig.MahaConfigResult[List[ResultSetTransformer]] = for {
      resultSetTransformersFactoryClassName <- resultSetTransformersFactoryClassNameResult
      resultSetTransformersFactoryConfig <- resultSetTransformersFactoryConfigResult
      resultSetTransformersFactory <- getFactory[ResultSetTransformersFactory](resultSetTransformersFactoryClassName, this.closer)
      resultSetTransformers <- resultSetTransformersFactory.fromJson(resultSetTransformersFactoryConfig)
    } yield resultSetTransformers

    val authHeaderProvider : MahaServiceConfig.MahaConfigResult[AuthHeaderProvider] = for {
      authHeaderProviderFactoryClassName <- authHeaderProviderFactoryClassNameResult
      authHeaderProviderFactoryConfig <- authHeaderProviderFactoryConfigResult
      authHeaderProviderFactory <- getFactory[AuthHeaderProviderFactory](authHeaderProviderFactoryClassName, this.closer)
      authHeaderProvider <- authHeaderProviderFactory.fromJson(authHeaderProviderFactoryConfig)
    } yield authHeaderProvider

    (druidQueryExecutorConfig |@| lifecycleListener |@| resultSetTransformers |@| authHeaderProvider) {
      (a, b, c, d) => {

        val executor = new DruidQueryExecutor(a, b, c, d)
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
    |"dataSourceName": "",
    |"jdbcConnectionFetchSize": 10,
    |"lifecycleListenerFactoryClass" : "",
    |"lifecycleListenerFactoryConfig" : [],
    |"prestoQueryTemplateFactoryName": "",
    |"prestoQueryTemplateFactoryConfig": []
    |}
  """.stripMargin

  override def fromJson(configJson: JValue)(implicit context:MahaServiceConfigContext): MahaServiceConfig.MahaConfigResult[QueryExecutor] =  {
    import org.json4s.scalaz.JsonScalaz._

    val dataSourceNameResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("dataSourceName")(configJson).map(_.toLowerCase)
    val jdbcConnectionFetchSizeOptionResult: MahaServiceConfig.MahaConfigResult[Option[Int]] = fieldExtended[Option[Int]]("jdbcConnectionFetchSize")(configJson)
    val lifecycleListenerFactoryClassResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("lifecycleListenerFactoryClass")(configJson)
    val lifecycleListenerFactoryConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("lifecycleListenerFactoryConfig")(configJson)
    val prestoQueryTemplateFactoryNameResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("prestoQueryTemplateFactoryName")(configJson)
    val prestoQueryTemplateFactoryConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("prestoQueryTemplateFactoryConfig")(configJson)

    import _root_.scalaz._
    import syntax.validation._

    for {
      dataSourceName <- dataSourceNameResult
    } yield  {
      if(!context.dataSourceMap.contains(dataSourceName)) {
        return Failure(List(ServiceConfigurationError(s"Failed to find presto dataSourceName $dataSourceName in dataSourceMap"))).toValidationNel.asInstanceOf[MahaConfigResult[QueryExecutor]]
      }
    }

    val jdbcConnetionResult : MahaServiceConfig.MahaConfigResult[JdbcConnection] = for {
      dataSourceName <- dataSourceNameResult
      dataSource <- context.dataSourceMap.get(dataSourceName).successNel[Option[DataSource]].asInstanceOf[MahaServiceConfig.MahaConfigResult[Option[DataSource]]]
      jdbcConnectionFetchSizeOption <- jdbcConnectionFetchSizeOptionResult
    } yield {
      if(jdbcConnectionFetchSizeOption.isDefined) {
        new JdbcConnection(dataSource.get, jdbcConnectionFetchSizeOption.get)
      } else {
        new JdbcConnection(dataSource.get)
      }
    }

    val lifecycleListener : MahaServiceConfig.MahaConfigResult[ExecutionLifecycleListener] = for {
      lifecycleListenerFactoryClass <- lifecycleListenerFactoryClassResult
      lifecycleListenerFactoryConfig <- lifecycleListenerFactoryConfigResult
      lifecycleListenerFactory <- getFactory[ExecutionLifecycleListenerFactory](lifecycleListenerFactoryClass, this.closer)
      lifecycleListener <- lifecycleListenerFactory.fromJson(lifecycleListenerFactoryConfig)
    } yield lifecycleListener



    val prestoQueryTemplate : MahaServiceConfig.MahaConfigResult[PrestoQueryTemplate] = for {
      prestoQueryTemplateFactoryClassName <- prestoQueryTemplateFactoryNameResult
      prestoQueryTemplateFactoryConfig <- prestoQueryTemplateFactoryConfigResult
      prestoQueryTemplateFactory <- getFactory[PrestoQueryTemplateFactory](prestoQueryTemplateFactoryClassName, this.closer)
      prestoQueryTemplate <- prestoQueryTemplateFactory.fromJson(prestoQueryTemplateFactoryConfig)
    } yield prestoQueryTemplate

    (jdbcConnetionResult |@| prestoQueryTemplate |@| lifecycleListener) {
      (jdbc, queryTemplate, listener) => {

        val executor = new PrestoQueryExecutor(jdbc, queryTemplate, listener)
        //PrestoQueryExecutor is not closeable.
        //closer.register(executor)
        executor
      }
    }
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class PostgresQueryExecutoryFactory extends QueryExecutoryFactory {
  """
    |{
    |"dataSourceName": "",
    |"jdbcConnectionFetchSize" 10,
    |"lifecycleListenerFactoryClass": "",
    |"lifecycleListenerFactoryConfig" : []
    |}
  """.stripMargin
  override def fromJson(configJson: JValue)(implicit context:MahaServiceConfigContext): MahaServiceConfig.MahaConfigResult[QueryExecutor] =  {
    import org.json4s.scalaz.JsonScalaz._

    import scalaz.Validation.FlatMap._
    import scalaz.syntax.applicative._

    val dataSourceNameResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("dataSourceName")(configJson).map(_.toLowerCase)
    val jdbcConnectionFetchSizeOptionResult: MahaServiceConfig.MahaConfigResult[Option[Int]] = fieldExtended[Option[Int]]("jdbcConnectionFetchSize")(configJson)
    val lifecycleListenerFactoryClassResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("lifecycleListenerFactoryClass")(configJson)
    val lifecycleListenerFactoryConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("lifecycleListenerFactoryConfig")(configJson)

    import _root_.scalaz._
    import syntax.validation._

    for {
      dataSourceName <- dataSourceNameResult
    } yield  {
      if(!context.dataSourceMap.contains(dataSourceName)) {
        return Failure(List(ServiceConfigurationError(s"Failed to find Postgres dataSourceName $dataSourceName in dataSourceMap"))).toValidationNel.asInstanceOf[MahaConfigResult[QueryExecutor]]
      }
    }

    val jdbcConnetionResult : MahaServiceConfig.MahaConfigResult[JdbcConnection] = for {
      dataSourceName <- dataSourceNameResult
      dataSource <- context.dataSourceMap.get(dataSourceName).successNel[Option[DataSource]].asInstanceOf[MahaServiceConfig.MahaConfigResult[Option[DataSource]]]
      jdbcConnectionFetchSizeOption <- jdbcConnectionFetchSizeOptionResult
    } yield {
      if(jdbcConnectionFetchSizeOption.isDefined) {
        new JdbcConnection(dataSource.get, jdbcConnectionFetchSizeOption.get)
      } else {
        new JdbcConnection(dataSource.get)
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
        new PostgresQueryExecutor(a, b)
    }
  }

  override def supportedProperties: List[(String, Boolean)] = ???
}

class BigqueryQueryExecutoryFactory extends QueryExecutoryFactory {
  """
    |{
    |"bigqueryQueryExecutorConfigFactoryClassName": ""
    |"bigqueryQueryExecutorConfigJson": {
    |  "gcpCredentialsFilePath": "",
    |  "gcpProjectId": "",
    |  "enableProxy": false,
    |  "proxyCredentialsFilePath": "", (optional)
    |  "proxyHost": "", (optional)
    |  "proxyPort": "", (optional)
    |  "retries": 0
    |},
    |"lifecycleListenerFactoryClass": "",
    |"lifecycleListenerFactoryConfig" : []
    |}
  """.stripMargin
  override def fromJson(configJson: JValue)(implicit context:MahaServiceConfigContext): MahaServiceConfig.MahaConfigResult[QueryExecutor] =  {
    import org.json4s.scalaz.JsonScalaz._

    import scalaz.Validation.FlatMap._
    import scalaz.syntax.applicative._

    val bigqueryQueryExecutorConfigFactoryClassNameResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("bigqueryQueryExecutorConfigFactoryClassName")(configJson)
    val bigqueryQueryExecutorConfigJsonResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("bigqueryQueryExecutorConfigJson")(configJson)
    val lifecycleListenerFactoryClassResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("lifecycleListenerFactoryClass")(configJson)
    val lifecycleListenerFactoryConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("lifecycleListenerFactoryConfig")(configJson)

    val bigqueryQueryExecutorConfig:  MahaServiceConfig.MahaConfigResult[BigqueryQueryExecutorConfig] = for {
      bigqueryQueryExecutorConfigFactoryClassName <- bigqueryQueryExecutorConfigFactoryClassNameResult
      bigqueryQueryExecutorConfigJson <- bigqueryQueryExecutorConfigJsonResult
      bigqueryQueryExecutorConfigFactory <- getFactory[BigqueryQueryExecutorConfigFactory](bigqueryQueryExecutorConfigFactoryClassName, this.closer)
      bigqueryQueryExecutorConfig <- bigqueryQueryExecutorConfigFactory.fromJson(bigqueryQueryExecutorConfigJson)
    } yield  bigqueryQueryExecutorConfig

    val lifecycleListener : MahaServiceConfig.MahaConfigResult[ExecutionLifecycleListener] = for {
      lifecycleListenerFactoryClass <- lifecycleListenerFactoryClassResult
      lifecycleListenerFactoryConfig <- lifecycleListenerFactoryConfigResult
      lifecycleListenerFactory <- getFactory[ExecutionLifecycleListenerFactory](lifecycleListenerFactoryClass, this.closer)
      lifecycleListener <- lifecycleListenerFactory.fromJson(lifecycleListenerFactoryConfig)
    } yield lifecycleListener

    (bigqueryQueryExecutorConfig |@| lifecycleListener) {
      (a, b) =>
        new BigqueryQueryExecutor(a, b)
    }
  }

  override def supportedProperties: List[(String, Boolean)] = ???
}
