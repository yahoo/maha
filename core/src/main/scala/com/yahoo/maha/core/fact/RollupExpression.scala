// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.fact

import com.yahoo.maha.core._

/**
 * Created by hiral on 10/7/15.
 */
sealed trait RollupExpression

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
case class HiveCustomRollup(expression: HiveDerivedExpression) extends CustomRollup with WithHiveEngine
case class OracleCustomRollup(expression: OracleDerivedExpression) extends CustomRollup with WithOracleEngine
case class DruidCustomRollup(expression: DruidDerivedExpression) extends CustomRollup with WithDruidEngine
case class DruidFilteredRollup(filter: Filter, factCol: DruidExpression.FieldAccess,
                               delegateAggregatorRollupExpression: RollupExpression) extends CustomRollup with WithDruidEngine
case class DruidFilteredListRollup(filter: List[Filter], factCol: DruidExpression.FieldAccess,
                               delegateAggregatorRollupExpression: RollupExpression) extends CustomRollup with WithDruidEngine
case object DruidThetaSketchRollup extends CustomRollup with WithDruidEngine

