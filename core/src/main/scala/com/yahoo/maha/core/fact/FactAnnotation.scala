// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.fact

import com.yahoo.maha.core._
import com.yahoo.maha.core.query.{FactualQueryContext, QueryContext}

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
sealed trait FactConditionalHint extends FactAnnotation {
  def conditions: Set[QueryCondition]
  def eval(queryConditions: Set[QueryCondition]): Boolean
  def hint: String
}

object FactConditionalHint {
  implicit val ordering: Ordering[FactConditionalHint] = Ordering.by(ch => ch.hint)
}

case class OracleFactStaticHint(hint: String) extends FactStaticHint with FactAnnotationInstance with WithOracleEngine
case class OracleFactDimDrivenHint(hint: String) extends FactDimDrivenHint with FactAnnotationInstance with WithOracleEngine

case class OraclePartitioningScheme(schemeName:String) extends PartitioningScheme  with FactAnnotationInstance with WithOracleEngine
case class PostgresFactStaticHint(hint: String) extends FactStaticHint with FactAnnotationInstance with WithPostgresEngine
case class PostgresFactDimDrivenHint(hint: String) extends FactDimDrivenHint with FactAnnotationInstance with WithPostgresEngine
case class PostgresPartitioningScheme(schemeName:String) extends PartitioningScheme  with FactAnnotationInstance with WithPostgresEngine

case class PrestoPartitioningScheme(schemeName:String) extends PartitioningScheme  with FactAnnotationInstance with WithPrestoEngine

case class BigqueryPartitioningScheme(schemeName: String) extends PartitioningScheme  with FactAnnotationInstance with WithBigqueryEngine

case class DruidQueryPriority(priority: Int) extends FactAnnotationInstance with WithDruidEngine
case object DruidGroupByStrategyV1 extends FactAnnotationInstance with WithDruidEngine
case object DruidGroupByStrategyV2 extends FactAnnotationInstance with WithDruidEngine
case class DruidGroupByIsSingleThreaded(isSingleThreaded: Boolean) extends FactAnnotationInstance with WithDruidEngine

sealed trait QueryCondition {
  def eval(query: QueryContext): Boolean
}
sealed trait FactualQueryCondition extends QueryCondition {
  final def eval(queryContext: QueryContext) : Boolean = {
    if(queryContext.isInstanceOf[FactualQueryContext]) {
      evalContext(queryContext.asInstanceOf[FactualQueryContext])
    } else false
  }

  protected def evalContext(queryContext: FactualQueryContext) : Boolean
}
case class IsIndexOptimized(value: Boolean) extends FactualQueryCondition {
  protected def evalContext(queryContext: FactualQueryContext) : Boolean = {
    queryContext.factBestCandidate.isIndexOptimized == value
  }
}
case class IsGrainOptimized(value: Boolean) extends FactualQueryCondition {
  protected def evalContext(queryContext: FactualQueryContext) : Boolean = {
    queryContext.factBestCandidate.isGrainOptimized == value
  }
}
case class ForceDimDriven(value: Boolean) extends FactualQueryCondition {
  protected def evalContext(queryContext: FactualQueryContext) : Boolean = {
    queryContext.requestModel.forceDimDriven == value
  }
}
case class IsDimDriven(value: Boolean) extends FactualQueryCondition {
  protected def evalContext(queryContext: FactualQueryContext) : Boolean = {
    queryContext.requestModel.isDimDriven == value
  }
}
case class MinRowsEstimate(value: Long) extends FactualQueryCondition {
  protected def evalContext(queryContext: FactualQueryContext): Boolean = {
    queryContext.factBestCandidate.factRows.rows >= value
  }
}
case class FactCondition(conditions: Set[QueryCondition]) {
  def eval(queryConditions: Set[QueryCondition]): Boolean = {
    (conditions.size <= queryConditions.size) && conditions.forall(queryConditions)
  }
}
object FactCondition {
  def apply(isIndexOptimized: Option[Boolean]
            , isGrainOptimized: Option[Boolean] = None
            , forceDimDriven: Option[Boolean] = None
            , isDimDriven: Option[Boolean] = None
            , minRowsEstimate: Option[Long] = None
           ): FactCondition = {
    val list=List(isIndexOptimized, isGrainOptimized, forceDimDriven, isDimDriven, minRowsEstimate)
    require(list.exists(_.isDefined), "At least one condition must be defined!")

    var conds: Set[QueryCondition] = Set.empty

    isIndexOptimized.foreach( v => conds+=IsIndexOptimized(v))
    isGrainOptimized.foreach( v => conds+=IsGrainOptimized(v))
    forceDimDriven.foreach( v => conds+=ForceDimDriven(v))
    isDimDriven.foreach( v => conds+=IsDimDriven(v))
    minRowsEstimate.foreach( v => conds+=MinRowsEstimate(v))

    FactCondition(conds)
  }
}
case class OracleFactConditionalHint(cond: FactCondition, hint: String) extends FactConditionalHint with WithOracleEngine {
  override def conditions: Set[QueryCondition] = cond.conditions
  override def eval(queryConditions: Set[QueryCondition]): Boolean = cond.eval(queryConditions)
}
case class PostgresFactConditionalHint(cond: FactCondition, hint: String) extends FactConditionalHint with WithPostgresEngine {
  override def conditions: Set[QueryCondition] = cond.conditions
  override def eval(queryConditions: Set[QueryCondition]): Boolean = cond.eval(queryConditions)
}
