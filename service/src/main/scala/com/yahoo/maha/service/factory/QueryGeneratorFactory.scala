// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import com.yahoo.maha.core._
import com.yahoo.maha.core.query.{QueryGenerator, Version}
import com.yahoo.maha.core.query.bigquery.BigqueryQueryGenerator
import com.yahoo.maha.core.query.druid.{DruidQueryGenerator, DruidQueryOptimizer}
import com.yahoo.maha.core.query.hive.{HiveQueryGenerator, HiveQueryGeneratorV2}
import com.yahoo.maha.core.query.oracle.OracleQueryGenerator
import com.yahoo.maha.core.query.postgres.PostgresQueryGenerator
import com.yahoo.maha.core.query.presto.{PrestoQueryGenerator, PrestoQueryGeneratorV1}
import com.yahoo.maha.core.request._
import com.yahoo.maha.service.{MahaServiceConfig, MahaServiceConfigContext}
import org.json4s.JValue
import scalaz.Validation.FlatMap._
import scalaz.syntax.applicative._

/**
 * Created by pranavbhole on 25/05/17.
 */
class OracleQueryGeneratorFactory extends QueryGeneratorFactory {

  """
    |{
    |"partitionColumnRendererClass" : "DefaultPartitionColumnRendererFactory",
    |"partitionColumnRendererConfig" : [{"key": "value"}],
    |"literalMapperClass" : "OracleLiteralMapper",
    |"literalMapperConfig": {}
    |}
  """.stripMargin

  override def fromJson(configJson: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[QueryGenerator[_ <: EngineRequirement]] = {
    import org.json4s.scalaz.JsonScalaz._
    val partitionColumnRendererClassResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("partitionColumnRendererClass")(configJson)
    val partitionColumnRendererConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("partitionColumnRendererConfig")(configJson)
    val literalMapperClassResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("literalMapperClass")(configJson)
    val literalMapperConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("literalMapperConfig")(configJson)

    val partitionColumnRenderer: MahaServiceConfig.MahaConfigResult[PartitionColumnRenderer] = for {
      partitionColumnRendererClass <- partitionColumnRendererClassResult
      partitionColumnRendererFactory <- getFactory[PartitionColumnRendererFactory](partitionColumnRendererClass, this.closer)
      partitionColumnRendererConfig <- partitionColumnRendererConfigResult
      partitionColumnRenderer <- partitionColumnRendererFactory.fromJson(partitionColumnRendererConfig)
    } yield partitionColumnRenderer

    val literalMapper: MahaServiceConfig.MahaConfigResult[OracleLiteralMapper] = for {
      literalMapperClass <- literalMapperClassResult
      literalMapperFactory <- getFactory[OracleLiteralMapperFactory](literalMapperClass, this.closer)
      literalMapperConfig <- literalMapperConfigResult
      literalMapper <- literalMapperFactory.fromJson(literalMapperConfig)

    } yield literalMapper

    (partitionColumnRenderer |@| literalMapper) {
      (psr, lm) => new OracleQueryGenerator(psr, lm)
    }
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class DruidQueryGeneratorFactory extends QueryGeneratorFactory {
  """
    |{
    |"queryOptimizerClass": "SyncDruidQueryOptimizer",
    |"queryOptimizerConfig" : {},
    |"defaultDimCardinality" : "40000",
    |"maximumMaxRows" : "5000",
    |"maximumTopNMaxRows" : "400",
    |"maximumMaxRowsAsync" : "100000",
    |"shouldLimitInnerQueries" : true,
    |"useCustomRoundingSumAggregator" : true
    |}
  """.stripMargin

  override def fromJson(configJson: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[QueryGenerator[_ <: EngineRequirement]] = {
    import org.json4s.scalaz.JsonScalaz._
    val queryOptimizerClassResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("queryOptimizerClass")(configJson)
    val queryOptimizerConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("queryOptimizerConfig")(configJson)
    val dimCardinalityResult: MahaServiceConfig.MahaConfigResult[Long] = fieldExtended[Long]("dimCardinality")(configJson)
    val maximumMaxRowsResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("maximumMaxRows")(configJson)
    val maximumTopNMaxRowsResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("maximumTopNMaxRows")(configJson)
    val maximumMaxRowsAsyncResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("maximumMaxRowsAsync")(configJson)
    val shouldLimitInnerQueries: MahaServiceConfig.MahaConfigResult[Boolean] = fieldExtended[Boolean]("shouldLimitInnerQueries")(configJson)
    val useCustomRoundingSumAggregatorResult: MahaServiceConfig.MahaConfigResult[Boolean] = fieldExtended[Boolean]("useCustomRoundingSumAggregator")(configJson)


    val queryOptimizer: MahaServiceConfig.MahaConfigResult[DruidQueryOptimizer] = for {
      queryOptimizerClass <- queryOptimizerClassResult
      queryOptimizerFactory <- getFactory[DruidQueryOptimizerFactory](queryOptimizerClass, this.closer)
      queryOptimizerConfig <- queryOptimizerConfigResult
      queryOptimizer <- queryOptimizerFactory.fromJson(queryOptimizerConfig)
    } yield queryOptimizer

    (queryOptimizer |@| dimCardinalityResult |@| maximumMaxRowsResult |@| maximumTopNMaxRowsResult |@| maximumMaxRowsAsyncResult |@| shouldLimitInnerQueries |@| useCustomRoundingSumAggregatorResult) {
      (a, b, c ,d, e, f, g) => new DruidQueryGenerator(a, b, c, d, e, f, g)
    }
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class HiveQueryGeneratorFactory extends QueryGeneratorFactory {
  """
    |{
    |"partitionColumnRendererClass" : "DefaultPartitionColumnRendererFactory",
    |"partitionColumnRendererConfig" : [{"key": "value"}],
    |"udfRegistrationFactoryName" : "",
    |"udfRegistrationFactoryConfig" : [],
    |"version": 0
    |}
  """.stripMargin

  override def fromJson(configJson: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[QueryGenerator[_ <: EngineRequirement]] = {
    import org.json4s.scalaz.JsonScalaz._
    val partitionColumnRendererClassResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("partitionColumnRendererClass")(configJson)
    val partitionColumnRendererConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("partitionColumnRendererConfig")(configJson)
    val udfRegistrationFactoryNameResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("udfRegistrationFactoryName")(configJson)
    val udfRegistrationFactoryConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("udfRegistrationFactoryConfig")(configJson)
    val versionResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("version")(configJson)

    val partitionColumnRenderer: MahaServiceConfig.MahaConfigResult[PartitionColumnRenderer] = for {
      partitionColumnRendererClass <- partitionColumnRendererClassResult
      partitionColumnRendererFactory <- getFactory[PartitionColumnRendererFactory](partitionColumnRendererClass, this.closer)
      partitionColumnRendererConfig <- partitionColumnRendererConfigResult
      partitionColumnRenderer <- partitionColumnRendererFactory.fromJson(partitionColumnRendererConfig)
    } yield partitionColumnRenderer

    val udfStatements: MahaServiceConfig.MahaConfigResult[Set[UDFRegistration]] = for {
      udfStatementsFactory <- getFactory[MahaUDFRegistrationFactory](udfRegistrationFactoryNameResult.toOption.get, this.closer)
      udfRegistrationFactoryConfig <- udfRegistrationFactoryConfigResult
      udfStatements <- udfStatementsFactory.fromJson(udfRegistrationFactoryConfig)
    } yield udfStatements

    val version: Version = {
      if (versionResult.isSuccess) {
        Version.from(versionResult.toOption.get).getOrElse(Version.DEFAULT)
      } else {
        Version.DEFAULT
      }
    }
    (partitionColumnRenderer |@| udfStatements) {
      (renderer, stmt) => {
        version match {
          case Version.v2 =>
            new HiveQueryGeneratorV2(renderer, stmt)
          case _ =>
            new HiveQueryGenerator(renderer, stmt)
        }
      }
    }
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class PrestoQueryGeneratorFactory extends QueryGeneratorFactory {
  """
    |{
    |"partitionColumnRendererClass" : "DefaultPartitionColumnRendererFactory",
    |"partitionColumnRendererConfig" : [{"key": "value"}],
    |"udfRegistrationFactoryName" : "",
    |"udfRegistrationFactoryConfig" : [],
    |"version": 0
    |}
  """.stripMargin

  override def fromJson(configJson: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[QueryGenerator[_ <: EngineRequirement]] = {
    import org.json4s.scalaz.JsonScalaz._
    val partitionColumnRendererClassResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("partitionColumnRendererClass")(configJson)
    val partitionColumnRendererConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("partitionColumnRendererConfig")(configJson)
    val udfRegistrationFactoryNameResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("udfRegistrationFactoryName")(configJson)
    val udfRegistrationFactoryConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("udfRegistrationFactoryConfig")(configJson)
    val versionResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("version")(configJson)

    val partitionColumnRenderer: MahaServiceConfig.MahaConfigResult[PartitionColumnRenderer] = for {
      partitionColumnRendererClass <- partitionColumnRendererClassResult
      partitionColumnRendererFactory <- getFactory[PartitionColumnRendererFactory](partitionColumnRendererClass, this.closer)
      partitionColumnRendererConfig <- partitionColumnRendererConfigResult
      partitionColumnRenderer <- partitionColumnRendererFactory.fromJson(partitionColumnRendererConfig)
    } yield partitionColumnRenderer

    val udfStatements: MahaServiceConfig.MahaConfigResult[Set[UDFRegistration]] = for {
      udfStatementsFactory <- getFactory[MahaUDFRegistrationFactory](udfRegistrationFactoryNameResult.toOption.get)
      udfRegistrationFactoryConfig <- udfRegistrationFactoryConfigResult
      udfStatements <- udfStatementsFactory.fromJson(udfRegistrationFactoryConfig)
    } yield udfStatements

    val version: Version = {
      if (versionResult.isSuccess) {
        Version.from(versionResult.toOption.get).getOrElse(Version.DEFAULT)
      } else {
        Version.DEFAULT
      }
    }

    (partitionColumnRenderer |@| udfStatements) {
      (renderer, stmt) => {
        version match {
          case Version.v1 =>
            new PrestoQueryGeneratorV1(renderer, stmt)
          case _ =>
            new PrestoQueryGenerator(renderer, stmt)
        }
      }
    }
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class PostgresQueryGeneratorFactory extends QueryGeneratorFactory {

  """
    |{
    |"partitionColumnRendererClass" : "DefaultPartitionColumnRendererFactory",
    |"partitionColumnRendererConfig" : [{"key": "value"}],
    |"literalMapperClass" : "PostgresLiteralMapper",
    |"literalMapperConfig": {}
    |}
  """.stripMargin

  override def fromJson(configJson: org.json4s.JValue)(implicit context: MahaServiceConfigContext): MahaServiceConfig.MahaConfigResult[QueryGenerator[_ <: EngineRequirement]] = {
    import org.json4s.scalaz.JsonScalaz._
    val partitionColumnRendererClassResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("partitionColumnRendererClass")(configJson)
    val partitionColumnRendererConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("partitionColumnRendererConfig")(configJson)
    val literalMapperClassResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("literalMapperClass")(configJson)
    val literalMapperConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("literalMapperConfig")(configJson)

    val partitionColumnRenderer: MahaServiceConfig.MahaConfigResult[PartitionColumnRenderer] = for {
      partitionColumnRendererClass <- partitionColumnRendererClassResult
      partitionColumnRendererFactory <- getFactory[PartitionColumnRendererFactory](partitionColumnRendererClass, this.closer)
      partitionColumnRendererConfig <- partitionColumnRendererConfigResult
      partitionColumnRenderer <- partitionColumnRendererFactory.fromJson(partitionColumnRendererConfig)
    } yield partitionColumnRenderer

    val literalMapper: MahaServiceConfig.MahaConfigResult[PostgresLiteralMapper] = for {
      literalMapperClass <- literalMapperClassResult
      literalMapperFactory <- getFactory[PostgresLiteralMapperFactory](literalMapperClass, this.closer)
      literalMapperConfig <- literalMapperConfigResult
      literalMapper <- literalMapperFactory.fromJson(literalMapperConfig)

    } yield literalMapper

    (partitionColumnRenderer |@| literalMapper) {
      (psr, lm) => new PostgresQueryGenerator(psr, lm)
    }
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class BigqueryQueryGeneratorFactory extends QueryGeneratorFactory {
  """
    |{
    |"partitionColumnRendererClass" : "DefaultPartitionColumnRendererFactory",
    |"partitionColumnRendererConfig" : [{"key": "value"}],
    |"udfRegistrationFactoryName" : "",
    |"udfRegistrationFactoryConfig" : [],
    |"version": 0
    |}
  """.stripMargin

  override def fromJson(configJson: org.json4s.JValue)
    (implicit context: MahaServiceConfigContext): MahaServiceConfig.MahaConfigResult[QueryGenerator[_ <: EngineRequirement]] = {
    import org.json4s.scalaz.JsonScalaz._
    val partitionColumnRendererClassResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("partitionColumnRendererClass")(configJson)
    val partitionColumnRendererConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("partitionColumnRendererConfig")(configJson)
    val udfRegistrationFactoryNameResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("udfRegistrationFactoryName")(configJson)
    val udfRegistrationFactoryConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("udfRegistrationFactoryConfig")(configJson)
    val versionResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("version")(configJson)

    val partitionColumnRenderer: MahaServiceConfig.MahaConfigResult[PartitionColumnRenderer] = for {
      partitionColumnRendererClass <- partitionColumnRendererClassResult
      partitionColumnRendererFactory <- getFactory[PartitionColumnRendererFactory](partitionColumnRendererClass, this.closer)
      partitionColumnRendererConfig <- partitionColumnRendererConfigResult
      partitionColumnRenderer <- partitionColumnRendererFactory.fromJson(partitionColumnRendererConfig)
    } yield partitionColumnRenderer

    val udfStatements: MahaServiceConfig.MahaConfigResult[Set[UDFRegistration]] = for {
      udfStatementsFactory <- getFactory[MahaUDFRegistrationFactory](udfRegistrationFactoryNameResult
        .toOption
        .get, this.closer)
      udfRegistrationFactoryConfig <- udfRegistrationFactoryConfigResult
      udfStatements <- udfStatementsFactory.fromJson(udfRegistrationFactoryConfig)
    } yield udfStatements


    (partitionColumnRenderer |@| udfStatements) {
      (renderer, stmt) => {
        new BigqueryQueryGenerator(renderer, stmt)
      }
    }
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

