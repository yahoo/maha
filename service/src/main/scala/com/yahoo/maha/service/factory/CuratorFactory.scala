// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import com.yahoo.maha.service.MahaServiceConfig
import com.yahoo.maha.service.curators.{Curator, DefaultCurator, DrilldownCurator, TimeShiftCurator}

import _root_.scalaz._
import scalaz.syntax.validation._

class DefaultCuratorFactory extends CuratorFactory {

  override def fromJson(configJson: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[Curator] = {
    new DefaultCurator().successNel
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class TimeShiftCuratorFactory extends CuratorFactory {

  override def fromJson(configJson: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[Curator] = {
    new TimeShiftCurator().successNel
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class DrilldownCuratorFactory extends CuratorFactory {

  override def fromJson(configJson: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[Curator] = {
    new DrilldownCurator().successNel
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}