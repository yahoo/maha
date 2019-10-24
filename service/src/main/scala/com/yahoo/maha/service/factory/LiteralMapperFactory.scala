// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import com.yahoo.maha.service.MahaServiceConfig.MahaConfigResult
import com.yahoo.maha.core.{DruidLiteralMapper, OracleLiteralMapper, PostgresLiteralMapper}
import org.json4s.JValue
import _root_.scalaz._
import com.yahoo.maha.service.MahaServiceConfigContext
import syntax.validation._

/**
 * Created by pranavbhole on 31/05/17.
 */
class DefaultOracleLiteralMapperFactory extends OracleLiteralMapperFactory {
  override def fromJson(config: JValue)(implicit context: MahaServiceConfigContext): MahaConfigResult[OracleLiteralMapper] = {
    new OracleLiteralMapper().successNel
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}
class DefaultDruidLiteralMapperFactory extends DruidLiteralMapperFactory {
  override def fromJson(config: JValue)(implicit context: MahaServiceConfigContext): MahaConfigResult[DruidLiteralMapper] = {
    new DruidLiteralMapper().successNel
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}
class DefaultPostgresLiteralMapperFactory extends PostgresLiteralMapperFactory {
  override def fromJson(config: JValue)(implicit context: MahaServiceConfigContext): MahaConfigResult[PostgresLiteralMapper] = {
    new PostgresLiteralMapper().successNel
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}