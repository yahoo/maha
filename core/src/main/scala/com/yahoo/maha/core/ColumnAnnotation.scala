// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import org.json4s.JsonAST.{JNull, JObject, JValue}
import org.json4s.scalaz.JsonScalaz._
/**
 * Created by hiral on 10/2/15.
 */

sealed trait ColumnAnnotation {
  def asJSON: JObject = makeObj(
    List(
      ("annotation" -> toJSON(this.getClass.getSimpleName))
    )
  )
}

sealed trait SingletonColumn

trait ClassNameHashCode {
  override final val hashCode: Int = this.getClass.toString.hashCode
  override def equals(other: Any) : Boolean =  this.hashCode == other.hashCode()
}

sealed trait ColumnAnnotationInstance extends ColumnAnnotation with ClassNameHashCode {
  def instance: ColumnAnnotation
}

case class HiveShardingExpression(expression: HiveDerivedExpression) extends ColumnAnnotationInstance with WithHiveEngine {
  def instance: ColumnAnnotation = HiveShardingExpression.instance

  val jUtils = JsonUtils

  override def asJSON(): JObject =
    makeObj(
      List(
        ("annotation" -> toJSON(this.getClass.getSimpleName))
        ,("expression" -> jUtils.asJSON(expression))
      )
    )
}
case object HiveShardingExpression {
  val instance: ColumnAnnotation = HiveShardingExpression(null)
}

case class PrestoShardingExpression(expression: PrestoDerivedExpression) extends ColumnAnnotationInstance with WithPrestoEngine {
  def instance: ColumnAnnotation = PrestoShardingExpression.instance

  val jUtils = JsonUtils

  override def asJSON(): JObject =
    makeObj(
      List(
        ("annotation" -> toJSON(this.getClass.getSimpleName))
        ,("expression" -> jUtils.asJSON(expression))
      )
    )
}
case object PrestoShardingExpression {
  val instance: ColumnAnnotation = PrestoShardingExpression(null)
}

case class BigqueryShardingExpression(expression: BigqueryDerivedExpression) extends ColumnAnnotationInstance with WithBigqueryEngine {
  def instance: ColumnAnnotation = BigqueryShardingExpression.instance

  val jUtils = JsonUtils

  override def asJSON(): JObject =
    makeObj(
      List(
        ("annotation" -> toJSON(this.getClass.getSimpleName))
        ,("expression" -> jUtils.asJSON(expression))
      )
    )
}

case object BigqueryShardingExpression {
  val instance: ColumnAnnotation = BigqueryShardingExpression(null)
}

case object PrimaryKey extends ColumnAnnotation
case object EscapingRequired extends ColumnAnnotation
case object HiveSnapshotTimestamp extends ColumnAnnotation with SingletonColumn with WithHiveEngine
case object OracleSnapshotTimestamp extends ColumnAnnotation with SingletonColumn with WithOracleEngine
case object PostgresSnapshotTimestamp extends ColumnAnnotation with SingletonColumn with WithPostgresEngine
case object BigquerySnapshotTimestamp extends ColumnAnnotation with SingletonColumn with WithBigqueryEngine
case object IsAggregation extends ColumnAnnotation
case object CaseInsensitive extends ColumnAnnotation
case class ForeignKey(publicDimName: String) extends ColumnAnnotationInstance {
  def instance: ColumnAnnotation = ForeignKey.instance

  override def asJSON(): JObject =
    makeObj(
      List(
        ("annotation" -> toJSON(this.getClass.getSimpleName))
        ,("publicDimName" -> toJSON(publicDimName))
      )
    )
}
case object ForeignKey {
  val instance: ColumnAnnotation = ForeignKey("instance")
}
case class DayColumn(fmt: String) extends ColumnAnnotationInstance {
  def instance: ColumnAnnotation = DayColumn.instance

  override def asJSON(): JObject =
    makeObj(
      List(
        ("annotation" -> toJSON(this.getClass.getSimpleName))
        ,("fmt" -> toJSON(fmt))
      )
    )
}
case object DayColumn {
  val instance: ColumnAnnotation = DayColumn("instance")
}
