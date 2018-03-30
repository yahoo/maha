// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.config

import com.yahoo.maha.core.request._
import org.json4s.JValue
import org.json4s.scalaz.JsonScalaz
import org.json4s.scalaz.JsonScalaz._

/**
  * Created by hiral on 5/25/17.
  */


object JsonHelpers {
  def nonEmptyString(string: String, fieldName : String, jsonFieldName: String) : JsonScalaz.Result[Boolean] = {
    import _root_.scalaz.syntax.validation._
    if (string.isEmpty) {
      Fail.apply(jsonFieldName, s"$fieldName cannot have empty string")
    } else {
      true.successNel
    }
  }

  def nonEmptyList(string: List[Any], fieldName : String, jsonFieldName: String) : JsonScalaz.Result[Boolean] = {
    import _root_.scalaz.syntax.validation._
    if (string.isEmpty) {
      Fail.apply(jsonFieldName, s"$fieldName cannot have empty list")
    } else {
      true.successNel
    }
  }
}

import scalaz.syntax.applicative._

case class JsonBucketingConfig(className: String, json: JValue)

object JsonBucketingConfig {
  import org.json4s.scalaz.JsonScalaz._

  implicit def parse: JSONR[JsonBucketingConfig] = new JSONR[JsonBucketingConfig] {
    override def read(json: JValue): Result[JsonBucketingConfig] = {
      //val field: JsonScalaz.Result[JValue] = json.findField(_._1  == "bucketConfig").fold(None.asInstanceOf[Option[MahaBucketConfig]].successNel[JsonScalaz.Error])(_)
      //val
      //) {
      //  case (_, JArray(buckets)) =>
      // case _ => UncategorizedError("bucketConfig", "must be an array", List.empty).asInstanceOf[JsonScalaz.Error].failureNel[Option[MahaBucketConfig]]
      //}


      /*
      {
        "factoryClass": "",
        "config": {}
      }
        */

      val name: Result[String] = fieldExtended[String]("factoryClass")(json)
      val config: Result[JValue] = fieldExtended[JValue]("config")(json)
      (name |@| config){
        (a,b) => JsonBucketingConfig(a, b)
      }
    }
  }
}

case class JsonUTCTimeProviderConfig(className: String, json: JValue)

object JsonUTCTimeProviderConfig {
  import org.json4s.scalaz.JsonScalaz._

  implicit def parse: JSONR[JsonUTCTimeProviderConfig] = new JSONR[JsonUTCTimeProviderConfig] {
    override def read(json: JValue): Result[JsonUTCTimeProviderConfig] = {
      val name: Result[String] = fieldExtended[String]("factoryClass")(json)
      val config: Result[JValue] = fieldExtended[JValue]("config")(json)
      (name |@| config){
        (a,b) => JsonUTCTimeProviderConfig(a, b)
      }
    }
  }
}

case class JsonQueryGeneratorConfig(className: String, json: JValue)

object JsonQueryGeneratorConfig {
  import org.json4s.scalaz.JsonScalaz._

  implicit def parse: JSONR[JsonQueryGeneratorConfig] = new JSONR[JsonQueryGeneratorConfig] {
    override def read(json: JValue): Result[JsonQueryGeneratorConfig] = {
      val name: Result[String] = fieldExtended[String]("factoryClass")(json)
      val config: Result[JValue] = fieldExtended[JValue]("config")(json)
      (name |@| config){
        (a,b) => JsonQueryGeneratorConfig(a, b)
      }
    }
  }
}

case class JsonParallelServiceExecutorConfig(className: String, json: JValue)

object JsonParallelServiceExecutorConfig {
  import org.json4s.scalaz.JsonScalaz._
  implicit def parse: JSONR[JsonParallelServiceExecutorConfig] = new JSONR[JsonParallelServiceExecutorConfig] {
    override def read(json: JValue): Result[JsonParallelServiceExecutorConfig] = {
      val name: Result[String] = fieldExtended[String]("factoryClass")(json)
      val config: Result[JValue] = fieldExtended[JValue]("config")(json)
      (name |@| config){
        (a,b) => JsonParallelServiceExecutorConfig(a, b)
      }
    }
  }
}

case class JsonQueryExecutorConfig(className: String, json: JValue)

object JsonQueryExecutorConfig {
  import org.json4s.scalaz.JsonScalaz._

  implicit def parse: JSONR[JsonQueryExecutorConfig] = new JSONR[JsonQueryExecutorConfig] {
    override def read(json: JValue): Result[JsonQueryExecutorConfig] = {
      val name: Result[String] = fieldExtended[String]("factoryClass")(json)
      val config: Result[JValue] = fieldExtended[JValue]("config")(json)
      (name |@| config){
        (a,b) => JsonQueryExecutorConfig(a, b)
      }
    }
  }
}

case class JsonCuratorConfig(className: String, json: JValue)

object JsonCuratorConfig {
  import org.json4s.scalaz.JsonScalaz._

  implicit def parse: JSONR[JsonCuratorConfig] = new JSONR[JsonCuratorConfig] {
    override def read(json: JValue): Result[JsonCuratorConfig] = {
      val name: Result[String] = fieldExtended[String]("factoryClass")(json)
      val config: Result[JValue] = fieldExtended[JValue]("config")(json)
      (name |@| config){
        (a,b) => JsonCuratorConfig(a, b)
      }
    }
  }
}

case class JsonRegistryConfig(factRegistrationdFactoryClass: String
                              , dimensionRegistrationFactoryClass: String
                              , executors:Set[String]
                              , generators: Set[String]
                              , bucketConfigName: String
                              , utcTimeProviderName: String
                              , parallelServiceExecutorName: String
                              , dimEstimatorFactoryClass: String
                              , dimEstimatorFactoryConfig: JValue
                              , factEstimatorFactoryClass: String
                              , factEstimatorFactoryConfig : JValue
                              , defaultPublicFactRevisionMap: Map[String, Int]
                              , defaultPublicDimRevisionMap: Map[String, Int]
                             )

object JsonRegistryConfig {
  import org.json4s.scalaz.JsonScalaz._

  implicit def parse: JSONR[JsonRegistryConfig] = new JSONR[JsonRegistryConfig] {
    override def read(json: JValue): Result[JsonRegistryConfig] = {
      //val name: Result[String] = fieldExtended[String]("name")(json)
      val factRegistrationClass: Result[String] = fieldExtended[String]("factRegistrationClass")(json)
      val dimensionRegistrationClass: Result[String] = fieldExtended[String]("dimensionRegistrationClass")(json)
      val executors: Result[Set[String]] = fieldExtended[List[String]]("executors")(json).map(_.map(_.toLowerCase).toSet)
      val generators: Result[Set[String]] = fieldExtended[List[String]]("generators")(json).map(_.map(_.toLowerCase).toSet)
      val bucketingConfigName: Result[String] = fieldExtended[String]("bucketingConfigName")(json).map(_.toLowerCase)
      val utcTimeProviderName: Result[String] = fieldExtended[String]("utcTimeProviderName")(json).map(_.toLowerCase)
      val parallelServiceExecutorName: Result[String] = fieldExtended[String]("parallelServiceExecutorName")(json).map(_.toLowerCase)

      val dimEstimatorFactoryClassResult: Result[String] = fieldExtended[String]("dimEstimatorFactoryClass")(json)
      val dimEstimatorFactoryConfigResult: Result[JValue] = fieldExtended[JValue]("dimEstimatorFactoryConfig")(json)
      val factEstimatorFactoryClassResult: Result[String] = fieldExtended[String]("factEstimatorFactoryClass")(json)
      val factEstimatorFactoryConfigResult: Result[JValue] = fieldExtended[JValue]("factEstimatorFactoryConfig")(json)
      val defaultPublicFactRevisionMapResult: Result[Map[String, Int]] = fieldExtended[Map[String, Int]]("defaultPublicFactRevisionMap")(json)
      val defaultPublicDimRevisionMapResult: Result[Map[String, Int]] = fieldExtended[Map[String, Int]]("defaultPublicDimRevisionMap")(json)


      val builderConfig = (dimEstimatorFactoryClassResult |@| dimEstimatorFactoryConfigResult |@| factEstimatorFactoryClassResult |@| factEstimatorFactoryConfigResult |@|  defaultPublicFactRevisionMapResult |@|  defaultPublicDimRevisionMapResult) {
        (a, b, c, d, f, g) => (a, b, c, d, f, g)
      }


      (factRegistrationClass |@| dimensionRegistrationClass |@| executors |@| generators |@| bucketingConfigName |@| utcTimeProviderName |@| parallelServiceExecutorName
      |@| builderConfig
      ){
        case (a, b, c, d, e, f, g, (h, i, j, k, m, n)) => JsonRegistryConfig(a, b, c, d, e, f, g, h, i, j, k, m, n)
      }
    }
  }
}

case class JsonMahaServiceConfig(registryMap: Map[String, JsonRegistryConfig]
                                 , executorMap: Map[String, JsonQueryExecutorConfig]
                                 , generatorMap: Map[String, JsonQueryGeneratorConfig]
                                 , bucketingConfigMap: Map[String, JsonBucketingConfig]
                                 , utcTimeProviderMap: Map[String, JsonUTCTimeProviderConfig]
                                 , parallelServiceExecutorConfigMap: Map[String, JsonParallelServiceExecutorConfig]
                                 , jsonMahaRequestLogConfig: JsonMahaRequestLogConfig
                                 , curatorMap: Map[String, JsonCuratorConfig])

object JsonMahaServiceConfig {
  import org.json4s.scalaz.JsonScalaz._

  implicit def mahaBucketConfigJSONR: JSONR[JsonMahaServiceConfig] = new JSONR[JsonMahaServiceConfig] {
    override def read(json: JValue): Result[JsonMahaServiceConfig] = {

      val registryMap: Result[Map[String, JsonRegistryConfig]] = fieldExtended[Map[String, JsonRegistryConfig]]("registryMap")(json).map(_.map(m=> m._1.toLowerCase -> m._2))
      val executorMap: Result[Map[String, JsonQueryExecutorConfig]] = fieldExtended[Map[String, JsonQueryExecutorConfig]]("executorMap")(json).map(_.map(m=> m._1.toLowerCase -> m._2))
      val generatorMap: Result[Map[String, JsonQueryGeneratorConfig]] = fieldExtended[Map[String, JsonQueryGeneratorConfig]]("generatorMap")(json).map(_.map(m=> m._1.toLowerCase -> m._2))
      val bucketingConfigMap: Result[Map[String, JsonBucketingConfig]] = fieldExtended[Map[String, JsonBucketingConfig]]("bucketingConfigMap")(json).map(_.map(m=> m._1.toLowerCase -> m._2))
      val utcTimeProviderMap: Result[Map[String, JsonUTCTimeProviderConfig]] = fieldExtended[Map[String, JsonUTCTimeProviderConfig]]("utcTimeProviderMap")(json).map(_.map(m=> m._1.toLowerCase -> m._2))
      val parallelServiceExecutorConfigMap : Result[Map[String, JsonParallelServiceExecutorConfig]] = fieldExtended[Map[String, JsonParallelServiceExecutorConfig]]("parallelServiceExecutorConfigMap")(json).map(_.map(m=> m._1.toLowerCase -> m._2))
      val jsonMahaRequestLogConfigResult: Result[JsonMahaRequestLogConfig] = fieldExtended[JsonMahaRequestLogConfig]("mahaRequestLoggingConfig")(json)
      val curatorMap: Result[Map[String, JsonCuratorConfig]] = fieldExtended[Map[String, JsonCuratorConfig]]("curatorMap")(json).map(_.map(m=> m._1.toLowerCase -> m._2))

      (registryMap |@| executorMap |@| generatorMap |@| bucketingConfigMap |@| utcTimeProviderMap |@| parallelServiceExecutorConfigMap |@| jsonMahaRequestLogConfigResult |@| curatorMap) {
        (a, b, c, d, e, f, g, h) => JsonMahaServiceConfig(a, b, c, d, e, f, g, h)
      }

    }
  }
}
