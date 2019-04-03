// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import com.yahoo.maha.core.request.fieldExtended
import com.yahoo.maha.log.{KafkaMahaRequestLogWriter, MahaRequestLogWriter, MultiColoMahaRequestLogWriter, NoopMahaRequestLogWriter}
import com.yahoo.maha.service.MahaServiceConfig
import com.yahoo.maha.service.MahaServiceConfig._
import com.yahoo.maha.service.config.JsonKafkaRequestLoggingConfig
import org.json4s.JValue
import org.json4s.scalaz.JsonScalaz._
import scalaz.Scalaz


/**
 * Created by pranavbhole on 23/08/17.
 */
class KafkaMahaRequestLogWriterFactory extends MahaRequestLogWriterFactory {
  override def fromJson(config: JValue, isLoggingEnabled: Boolean): MahaConfigResult[MahaRequestLogWriter] = {
    val kafkaRequestLoggingConfigResult: Result[JsonKafkaRequestLoggingConfig] = JsonKafkaRequestLoggingConfig.parse.read(config)
     for {
       kafkaRequestLoggingConfig <- kafkaRequestLoggingConfigResult
     } yield {
       new KafkaMahaRequestLogWriter(kafkaRequestLoggingConfig.config, isLoggingEnabled)
     }
  }
  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class NoopMahaRequestLogWriterFactory extends MahaRequestLogWriterFactory {
  import Scalaz._
  override def fromJson(config: JValue, isLoggingEnabled: Boolean): MahaConfigResult[MahaRequestLogWriter] = new NoopMahaRequestLogWriter().successNel

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class MultiColoMahaRequestLogWriterFactory extends MahaRequestLogWriterFactory {

  override def fromJson(config: JValue, isLoggingEnabled: Boolean): MahaConfigResult[MahaRequestLogWriter] = {

    val multiColoConfigListResult: MahaServiceConfig.MahaConfigResult[List[JsonKafkaRequestLoggingConfig]] = fieldExtended[List[JsonKafkaRequestLoggingConfig]]("multiColoConfigList")(config)
    for {
      multiColoConfigList <- multiColoConfigListResult
    } yield {
      MultiColoMahaRequestLogWriter(multiColoConfigList.map(cnf =>  new KafkaMahaRequestLogWriter(cnf.config, isLoggingEnabled)))
    }
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}


