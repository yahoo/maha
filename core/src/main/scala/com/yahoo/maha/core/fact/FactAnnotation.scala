// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.fact

import com.yahoo.maha.core.{WithDruidEngine, ClassNameHashCode, WithOracleEngine, WithHiveEngine}

/**
 * Created by hiral on 10/7/15.
 */
sealed trait FactAnnotation

sealed trait FactAnnotationInstance extends FactAnnotation with ClassNameHashCode

sealed trait PartitioningScheme extends FactAnnotationInstance

case class HiveLocationAnnotation(path: String) extends FactAnnotationInstance with WithHiveEngine
case class HiveStorageFormatAnnotation(format: String) extends FactAnnotationInstance with WithHiveEngine

case class HivePartitioningScheme(schemeName:String) extends PartitioningScheme  with FactAnnotationInstance with WithHiveEngine

sealed trait FactStaticHint extends FactAnnotation
sealed trait FactDimDrivenHint extends FactAnnotation

case class OracleFactStaticHint(hint: String) extends FactStaticHint with FactAnnotationInstance with WithOracleEngine
case class OracleFactDimDrivenHint(hint: String) extends FactDimDrivenHint with FactAnnotationInstance with WithOracleEngine

case class OraclePartitioningScheme(schemeName:String) extends PartitioningScheme  with FactAnnotationInstance with WithOracleEngine
case class DruidQueryPriority(priority: Int) extends FactAnnotationInstance with WithDruidEngine
case object DruidGroupByStrategyV2 extends FactAnnotationInstance with WithDruidEngine


