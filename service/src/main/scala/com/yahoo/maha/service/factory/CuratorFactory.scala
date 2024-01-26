// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import com.yahoo.maha.service.{MahaServiceConfig, MahaServiceConfigContext}
import com.yahoo.maha.service.curators._
import scalaz.syntax.validation._

class DefaultCuratorFactory extends CuratorFactory {

  override def fromJson(configJson: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[Curator] = {
    new DefaultCurator().asInstanceOf[Curator].successNel
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class RowCountCuratorFactory extends CuratorFactory {

  override def fromJson(configJson: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[Curator] = {
    new RowCountCurator().asInstanceOf[Curator].successNel
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class TimeShiftCuratorFactory extends CuratorFactory {

  override def fromJson(configJson: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[Curator] = {
    new TimeShiftCurator().asInstanceOf[Curator].successNel
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class DrillDownCuratorFactory extends CuratorFactory {

  override def fromJson(configJson: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[Curator] = {
    new DrilldownCurator().asInstanceOf[Curator].successNel
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}


class TotalMetricsCuratorFactory extends CuratorFactory {

  override def fromJson(configJson: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[Curator] = {
    new TotalMetricsCurator().asInstanceOf[Curator].successNel
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}
