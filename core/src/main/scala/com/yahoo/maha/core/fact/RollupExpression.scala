// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.fact

import com.yahoo.maha.core._
import org.json4s.JsonAST.{JArray, JObject}
import org.json4s.scalaz.JsonScalaz._

/**
 * Created by hiral on 10/7/15.
 */
sealed trait RollupExpression {
  val hasDerivedExpression = false
  lazy val sourceColumns: Set[String] = Set.empty
  lazy val sourcePrimitiveColumns: Set[String] = Set.empty

  private val jUtils = JsonUtils

  def asJSON: JObject =
    makeObj(
      List(
        ("expressionName" -> toJSON(this.getClass.getSimpleName))
        ,("hasDerivedExpression" -> toJSON(hasDerivedExpression))
        ,("sourcePrimitiveColumns"   ->  jUtils.asJSON(sourcePrimitiveColumns))
      )
    )
}

case object SumRollup extends RollupExpression
case object MaxRollup extends RollupExpression
case object MinRollup extends RollupExpression
case object AverageRollup extends RollupExpression
case object CountRollup extends RollupExpression
case object NoopRollup extends RollupExpression

sealed trait CustomRollup extends RollupExpression

/**
 * Please do not use this for simple rollup expressions
 */
case class HiveCustomRollup(expression: HiveDerivedExpression) extends CustomRollup with WithHiveEngine {
  override val hasDerivedExpression: Boolean = true
  override lazy val sourceColumns: Set[String] = expression.sourceColumns
  override lazy val sourcePrimitiveColumns: Set[String] = expression.sourcePrimitiveColumns
}
case class PrestoCustomRollup(expression: PrestoDerivedExpression) extends CustomRollup with WithPrestoEngine {
  override val hasDerivedExpression: Boolean = true
  override lazy val sourceColumns: Set[String] = expression.sourceColumns
  override lazy val sourcePrimitiveColumns: Set[String] = expression.sourcePrimitiveColumns
}
case class OracleCustomRollup(expression: OracleDerivedExpression) extends CustomRollup with WithOracleEngine {
  override val hasDerivedExpression: Boolean = true
  override lazy val sourceColumns: Set[String] = expression.sourceColumns
  override lazy val sourcePrimitiveColumns: Set[String] = expression.sourcePrimitiveColumns
}
case class PostgresCustomRollup(expression: PostgresDerivedExpression) extends CustomRollup with WithPostgresEngine {
  override val hasDerivedExpression: Boolean = true
  override lazy val sourceColumns: Set[String] = expression.sourceColumns
  override lazy val sourcePrimitiveColumns: Set[String] = expression.sourcePrimitiveColumns
}
case class BigqueryCustomRollup(expression: BigqueryDerivedExpression) extends CustomRollup with WithBigqueryEngine {
  override val hasDerivedExpression: Boolean = true
  override lazy val sourceColumns: Set[String] = expression.sourceColumns
  override lazy val sourcePrimitiveColumns: Set[String] = expression.sourcePrimitiveColumns
}
case class DruidCustomRollup(expression: DruidDerivedExpression) extends CustomRollup with WithDruidEngine {
  override val hasDerivedExpression: Boolean = true
  override lazy val sourceColumns: Set[String] = expression.sourceColumns
  override lazy val sourcePrimitiveColumns: Set[String] = expression.sourcePrimitiveColumns
}
case class DruidFilteredRollup(filter: Filter, factCol: DruidExpression.FieldAccess,
                               delegateAggregatorRollupExpression: RollupExpression) extends CustomRollup with WithDruidEngine {
  override val hasDerivedExpression: Boolean = true
  override lazy val sourceColumns: Set[String] = Set(filter.field, factCol.name) ++ delegateAggregatorRollupExpression.sourceColumns
}
case class DruidFilteredListRollup(filter: List[Filter], factCol: DruidExpression.FieldAccess,
                               delegateAggregatorRollupExpression: RollupExpression) extends CustomRollup with WithDruidEngine {
  override val hasDerivedExpression: Boolean = true
  override lazy val sourceColumns: Set[String] = filter.map(fil => fil.field).toSet ++ delegateAggregatorRollupExpression.sourceColumns ++ Set(factCol.name)
}

case class DruidHyperUniqueRollup(column: String) extends CustomRollup with WithDruidEngine {
  override val hasDerivedExpression: Boolean = true
  override lazy val sourceColumns: Set[String] = Set(column)
}
case object DruidThetaSketchRollup extends CustomRollup with WithDruidEngine

