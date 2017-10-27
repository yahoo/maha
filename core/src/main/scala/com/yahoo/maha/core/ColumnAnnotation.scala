// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

/**
 * Created by hiral on 10/2/15.
 */

sealed trait ColumnAnnotation

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
}
case object HiveShardingExpression {
  val instance: ColumnAnnotation = HiveShardingExpression(null)
}

case object PrimaryKey extends ColumnAnnotation
case object EscapingRequired extends ColumnAnnotation
case object HiveSnapshotTimestamp extends ColumnAnnotation with SingletonColumn with WithHiveEngine
case object OracleSnapshotTimestamp extends ColumnAnnotation with SingletonColumn with WithOracleEngine
case object IsAggregation extends ColumnAnnotation
case object CaseInsensitive extends ColumnAnnotation
case class ForeignKey(publicDimName: String) extends ColumnAnnotationInstance {
  def instance: ColumnAnnotation = ForeignKey.instance
}
case object ForeignKey {
  val instance: ColumnAnnotation = ForeignKey("instance")
}
case class DayColumn(fmt: String) extends ColumnAnnotationInstance {
  def instance: ColumnAnnotation = DayColumn.instance
}
case object DayColumn {
  val instance: ColumnAnnotation = DayColumn("instance")
}
