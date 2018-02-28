// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.fact

import com.yahoo.maha.core._
import com.yahoo.maha.core.query.{FactualQueryContext, Query}

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
sealed trait FactConditionalHint extends FactAnnotation

case class OracleFactStaticHint(hint: String) extends FactStaticHint with FactAnnotationInstance with WithOracleEngine
case class OracleFactDimDrivenHint(hint: String) extends FactDimDrivenHint with FactAnnotationInstance with WithOracleEngine

case class OraclePartitioningScheme(schemeName:String) extends PartitioningScheme  with FactAnnotationInstance with WithOracleEngine

case class PrestoPartitioningScheme(schemeName:String) extends PartitioningScheme  with FactAnnotationInstance with WithPrestoEngine

case class DruidQueryPriority(priority: Int) extends FactAnnotationInstance with WithDruidEngine
case object DruidGroupByStrategyV1 extends FactAnnotationInstance with WithDruidEngine
case object DruidGroupByStrategyV2 extends FactAnnotationInstance with WithDruidEngine

sealed trait QueryCondition {
  def eval(query: Query): Boolean
}
sealed trait FactualQueryCondition extends QueryCondition {
  final def eval(query: Query) : Boolean = {
    if(query.queryContext.isInstanceOf[FactualQueryContext]) {
      evalContext(query.queryContext.asInstanceOf[FactualQueryContext])
    } else false
  }

  protected def evalContext(queryContext: FactualQueryContext) : Boolean
}
case class IsIndexOptimized(value: Boolean) extends FactualQueryCondition {
  protected def evalContext(queryContext: FactualQueryContext) : Boolean = {
    queryContext.factBestCandidate.isIndexOptimized
  }
}
case class IsGrainOptimized(value: Boolean) extends FactualQueryCondition {
  protected def evalContext(queryContext: FactualQueryContext) : Boolean = {
    queryContext.factBestCandidate.isGrainOptimized
  }
}
case class ForceDimDriven(value: Boolean) extends FactualQueryCondition {
  protected def evalContext(queryContext: FactualQueryContext) : Boolean = {
    queryContext.requestModel.forceDimDriven
  }
}
case class IsDimDriven(value: Boolean) extends FactualQueryCondition {
  protected def evalContext(queryContext: FactualQueryContext) : Boolean = {
    queryContext.requestModel.isDimDriven
  }
}
case class FactCondition(conditions: Set[QueryCondition]) {
  def eval(queryConditions: Set[QueryCondition]): Boolean = {
    (conditions.size <= queryConditions.size) && conditions.forall(queryConditions)
  }
}
object FactCondition {
  def apply(isIndexOptimized: Option[Boolean]
            , isGrainOptimized: Option[Boolean]
            , forceDimDriven: Option[Boolean]
            , isDimDriven: Option[Boolean]): FactCondition = {
    val list=List(isIndexOptimized, isGrainOptimized, forceDimDriven, isDimDriven)
    require(list.exists(_.isDefined), "At least one condition must be defined!")

    var conds: Set[QueryCondition] = Set.empty

    isIndexOptimized.foreach( v => conds+=IsIndexOptimized(v))
    isGrainOptimized.foreach( v => conds+=IsGrainOptimized(v))
    forceDimDriven.foreach( v => conds+=ForceDimDriven(v))
    isDimDriven.foreach( v => conds+=IsDimDriven(v))

    FactCondition(conds)
  }
}
case class OracleFactConditionalHint(cond: FactCondition, hint: String) extends FactAnnotation with WithOracleEngine
