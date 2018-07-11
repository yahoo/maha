package com.yahoo.maha.service.factory

import com.yahoo.maha.jdbc.List
import com.yahoo.maha.service.{MahaServiceConfig, MahaServiceConfigContext}
import com.yahoo.maha.service.curators.{Curator, FailingCurator}

/**
  * Created by hiral on 4/11/18.
  */

class FailingCuratorFactory extends CuratorFactory {

  import scalaz.syntax.validation._
  override def fromJson(configJson: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[Curator] = {
    new FailingCurator().successNel
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}
