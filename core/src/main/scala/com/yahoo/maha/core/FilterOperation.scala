// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

/**
 * Created by hiral on 10/2/15.
 */


import com.google.common.collect.Lists
import com.yahoo.maha.core.DruidDerivedFunction._
import com.yahoo.maha.core.DruidPostResultFunction.{START_OF_THE_MONTH, START_OF_THE_WEEK}
import com.yahoo.maha.core.MetaType.MetaType
import com.yahoo.maha.core.dimension.{DruidFuncDimCol, DruidPostResultFuncDimCol}
import com.yahoo.maha.core.request.{Parameter, TimeZoneValue, fieldExtended}
import grizzled.slf4j.Logging
import org.apache.druid.java.util.common.granularity.PeriodGranularity
import org.apache.druid.js.JavaScriptConfig
import org.apache.druid.query.dimension.{DefaultDimensionSpec, DimensionSpec}
import org.apache.druid.query.extraction.{RegexDimExtractionFn, SubstringDimExtractionFn, TimeDimExtractionFn, TimeFormatExtractionFn}
import org.apache.druid.query.filter.JavaScriptDimFilter
import org.apache.druid.query.ordering.StringComparators.{LexicographicComparator, NumericComparator}
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import org.joda.time.{DateTime, DateTimeZone, Days, Period}

import scala.collection.{Iterable, mutable}
import scalaz.syntax.applicative._
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.scalaz.JsonScalaz
import org.json4s.scalaz.JsonScalaz._
import org.json4s.jackson.JsonMethods._

import scala.collection.immutable.SortedSet

sealed trait FilterOperation
case object InFilterOperation extends FilterOperation { override def toString = "In" }
case object NotInFilterOperation extends FilterOperation { override def toString = "Not In" }
case object BetweenFilterOperation extends FilterOperation { override def toString = "Between" }
case object DateTimeBetweenFilterOperation extends FilterOperation { override def toString = "DateTimeBetween" }
case object EqualityFilterOperation extends FilterOperation { override def toString = "=" }
case object LikeFilterOperation extends FilterOperation { override def toString = "Like" }
case object NotLikeFilterOperation extends FilterOperation { override def toString = "Not Like" }
case object NotEqualToFilterOperation extends FilterOperation { override def toString = "<>" }
case object IsNullFilterOperation extends FilterOperation { override def toString = "IsNull" }
case object IsNotNullFilterOperation extends FilterOperation { override def toString = "IsNotNull" }
case object OuterFilterOperation extends FilterOperation { override def toString = "Outer" }
case object OrFilterOperation extends FilterOperation { override def toString = "Or" }
case object AndFilterOperation extends FilterOperation { override def toString = "And" }
case object GreaterThanFilterOperation extends FilterOperation { override def toString = ">" }
case object LessThanFilterOperation extends FilterOperation { override def toString = "<" }
case object FieldEqualityFilterOperation extends FilterOperation { override def toString = "==" }
case object NoopFilterOperation extends FilterOperation

object FilterOperation {
  val In : Set[FilterOperation] = Set(InFilterOperation)
  val NotIn : Set[FilterOperation] = Set(NotInFilterOperation)
  val Between : Set[FilterOperation] = Set(BetweenFilterOperation)
  val Equality : Set[FilterOperation] = Set(EqualityFilterOperation)
  val FieldEquality: Set[FilterOperation] = Set(FieldEqualityFilterOperation)
  val EqualityFieldEquality: Set[FilterOperation] = Set(EqualityFilterOperation, FieldEqualityFilterOperation)
  val InEqualityFieldEquality: Set[FilterOperation] = Set(InFilterOperation, EqualityFilterOperation, FieldEqualityFilterOperation)
  val InEqualityLikeFieldEquality: Set[FilterOperation] = Set(InFilterOperation, EqualityFilterOperation, FieldEqualityFilterOperation)
  val InBetweenEqualityFieldEquality: Set[FilterOperation] = Set(InFilterOperation, BetweenFilterOperation, EqualityFilterOperation, LikeFilterOperation, FieldEqualityFilterOperation)
  val InEquality: Set[FilterOperation] = Set(InFilterOperation, EqualityFilterOperation)
  val InEqualityNotEquals: Set[FilterOperation] = Set(InFilterOperation, EqualityFilterOperation, NotEqualToFilterOperation)
  val InNotInEquality: Set[FilterOperation] = Set(InFilterOperation, NotInFilterOperation, EqualityFilterOperation)
  val InEqualityLike : Set[FilterOperation] = Set(InFilterOperation, EqualityFilterOperation, LikeFilterOperation)
  val InEqualityLikeNotLike : Set[FilterOperation] = Set(InFilterOperation, EqualityFilterOperation, LikeFilterOperation, NotLikeFilterOperation)
  val InNotInEqualityNotEquals: Set[FilterOperation] = Set(InFilterOperation, NotInFilterOperation, EqualityFilterOperation, NotEqualToFilterOperation)
  val InNotInEqualityLike: Set[FilterOperation] = Set(InFilterOperation, EqualityFilterOperation, NotInFilterOperation, LikeFilterOperation)
  val InBetweenEquality: Set[FilterOperation] = Set(InFilterOperation, BetweenFilterOperation,EqualityFilterOperation)
  val InBetweenDateTimeBetweenEquality: Set[FilterOperation] = Set(InFilterOperation, BetweenFilterOperation,EqualityFilterOperation, DateTimeBetweenFilterOperation)
  val BetweenEquality: Set[FilterOperation] = Set(BetweenFilterOperation,EqualityFilterOperation)
  val InEqualityIsNotNull: Set[FilterOperation] = Set(InFilterOperation, EqualityFilterOperation, IsNotNullFilterOperation)
  val InEqualityIsNotNullNotIn: Set[FilterOperation] = Set(InFilterOperation, EqualityFilterOperation, IsNotNullFilterOperation,NotInFilterOperation)
  val InNotInEqualityNullNotNull: Set[FilterOperation] = Set(InFilterOperation, NotInFilterOperation, EqualityFilterOperation,
                                                             IsNullFilterOperation, IsNotNullFilterOperation)
  val InBetweenEqualityNullNotNull: Set[FilterOperation] = Set(InFilterOperation, BetweenFilterOperation,EqualityFilterOperation,
                                                               IsNullFilterOperation, IsNotNullFilterOperation)
  val InNotInEqualityLikeNullNotNull: Set[FilterOperation] = Set(InFilterOperation, EqualityFilterOperation, NotInFilterOperation,
                                                   LikeFilterOperation, IsNullFilterOperation, IsNotNullFilterOperation)
  val InNotInEqualityNotEqualsNullNotNull: Set[FilterOperation] = Set(InFilterOperation, EqualityFilterOperation,NotEqualToFilterOperation, NotInFilterOperation, IsNullFilterOperation, IsNotNullFilterOperation)

  val InNotInEqualityNotEqualsLikeNullNotNull: Set[FilterOperation] = Set(InFilterOperation, EqualityFilterOperation,NotEqualToFilterOperation, NotInFilterOperation,
                                                   LikeFilterOperation, IsNullFilterOperation, IsNotNullFilterOperation)

  val InNotInEqualityNotEqualsLikeNotLikeNullNotNull: Set[FilterOperation] = Set(InFilterOperation, EqualityFilterOperation,NotEqualToFilterOperation, NotInFilterOperation,
                                                  LikeFilterOperation, NotLikeFilterOperation, IsNullFilterOperation, IsNotNullFilterOperation)

  val InNotInEqualityNotEqualsLikeNullNotNullBetween: Set[FilterOperation] = Set(InFilterOperation, EqualityFilterOperation,NotEqualToFilterOperation, NotInFilterOperation,
    LikeFilterOperation, IsNullFilterOperation, IsNotNullFilterOperation, BetweenFilterOperation)

  val InNotInBetweenEqualityNotEqualsGreaterLesser: Set[FilterOperation] = Set(InFilterOperation, NotInFilterOperation, BetweenFilterOperation, EqualityFilterOperation, NotEqualToFilterOperation, GreaterThanFilterOperation, LessThanFilterOperation)
}

sealed trait Filter {
  def field: String
  def operator: FilterOperation
  def asValues: String
  def isPushDown: Boolean = false
  def canBeHighCardinalityFilter: Boolean = false
}
import scala.reflect.ClassTag
abstract class BaseEquality[T](implicit tag: ClassTag[T]) {
  def canEqual(a: Any): Boolean = tag.runtimeClass.isInstance(a)
  def compareT(a: T, b: T) : Int
  def compare(a: Any, b: Any): Int = {
    if(canEqual(a) && canEqual(b)) {
      compareT(a.asInstanceOf[T], b.asInstanceOf[T])
    } else {
      a.getClass.getSimpleName.compare(b.getClass.getSimpleName)
    }
  }
}
object BaseEquality {
  def from[T](cmp: (T, T) => Int)(implicit tag: ClassTag[T]) : BaseEquality[T] = {
    new BaseEquality[T]() {
      override def compareT(a: T, b: T): Int = cmp(a, b)
    }
  }
}

sealed trait ForcedFilter extends Filter {
  def isForceFilter: Boolean = false
  def isOverridable: Boolean = false
}

sealed trait MultiFieldForcedFilter extends ForcedFilter {
  def compareTo: String
}

case class PushDownFilter(f: Filter) extends Filter {
  def field: String = f.field
  def operator: FilterOperation = f.operator
  override def isPushDown: Boolean = true
  def asValues : String = f.toString
}

case class OuterFilter(filters: List[Filter]) extends Filter {
  override def operator: FilterOperation = OuterFilterOperation
  override def field: String = "outer"
  val asValues: String = filters.map(_.asValues).mkString(",")
}

case class BetweenFilter(field: String, from: String, to: String) extends Filter {
  override def operator = BetweenFilterOperation
  val asValues: String = s"$from-$to"
}

trait DateTimeFilter extends Filter {
  def format: String

  //This is always in UTC format
  val dateTimeFormatter: DateTimeFormatter = DateType.formatter(format)

  def convertToUTCFromLocalZone(zone: DateTimeZone): DateTimeFilter
}

case class DateTimeBetweenFilter(field: String, from: String, to: String, format: String) extends DateTimeFilter {
  override def operator = DateTimeBetweenFilterOperation

  val asValues: String = s"$from-$to"
  val fromDateTime: DateTime = try {
    dateTimeFormatter.parseDateTime(from)
  } catch {
    case e: Exception =>
      DateTimeBetweenFilterHelper.logError(s"Unchecked input to function from=$from", e)
      throw e
  }
  val toDateTime: DateTime = try {
    dateTimeFormatter.parseDateTime(to)
  } catch {
    case e: Exception =>
      DateTimeBetweenFilterHelper.logError(s"Unchecked input to function to=$to", e)
      throw e
  }

  require(fromDateTime.isBefore(toDateTime) || fromDateTime.isEqual(toDateTime), s"From datetime must be before or equal to To datetime : from=$from, to=$to")

  val daysBetween: Int = Days.daysBetween(fromDateTime, toDateTime).getDays
  val daysFromNow: Int = try {
    val currentDate = DateTime.now(DateTimeZone.UTC)
    Days.daysBetween(fromDateTime, currentDate).getDays
  } catch {
    case e: Exception =>
      1
  }

  private def getDateTimeWithZone(dt: DateTime, z: DateTimeZone): DateTime = {
    val yy = dt.getYear
    val mm = dt.getMonthOfYear
    val dd = dt.getDayOfMonth
    val hh = dt.getHourOfDay
    val mins = dt.getMinuteOfHour
    val secs = dt.getSecondOfMinute
    val millis = dt.getMillisOfSecond
    new DateTime(yy, mm, dd, hh, mins, secs, millis, z)
  }

  def convertToUTCFromLocalZone(zone: DateTimeZone) : DateTimeBetweenFilter = {
    val f = if(zone != DateTimeZone.UTC ) {
      getDateTimeWithZone(fromDateTime, zone)
    } else fromDateTime
    val t = if(zone != DateTimeZone.UTC) {
      getDateTimeWithZone(toDateTime, zone)
    } else toDateTime
    DateTimeBetweenFilterHelper.iso8601(field, DateTimeBetweenFilterHelper.isoFormat.print(f)
      , DateTimeBetweenFilterHelper.isoFormat.print(t))
  }
}

object DateTimeBetweenFilterHelper extends Logging {
  val isoFormat: DateTimeFormatter = ISODateTimeFormat.dateTime().withZoneUTC()
  val iso8601FormatString = "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"

  def iso8601FormattedString(dt: DateTime) : String = {
    isoFormat.print(dt)
  }

  def logError(msg: String, t: Throwable): Unit = {
    error(msg, t)
  }

  def iso8601(field: String, from: String, to: String): DateTimeBetweenFilter = {
    DateTimeBetweenFilter(field, from, to, iso8601FormatString)
  }

  def yearMonthDayHourMinuteSecondMilli(field: String, from: String, to: String): DateTimeBetweenFilter = {
    DateTimeBetweenFilter(field, from, to, "yyyy-MM-dd'T'HH:mm:ss.SSS")
  }

  def yearMonthDayHourMinuteSecond(field: String, from: String, to: String): DateTimeBetweenFilter = {
    DateTimeBetweenFilter(field, from, to, "yyyy-MM-dd'T'HH:mm:ss")
  }

  def yearMonthDayHourMinute(field: String, from: String, to: String): DateTimeBetweenFilter = {
    DateTimeBetweenFilter(field, from, to, "yyyy-MM-dd'T'HH:mm")
  }

  def yearMonthDayHour(field: String, from: String, to: String): DateTimeBetweenFilter = {
    DateTimeBetweenFilter(field, from, to, "yyyy-MM-dd'T'HH")
  }

  def yearMonthDay(field: String, from: String, to: String): DateTimeBetweenFilter = {
    DateTimeBetweenFilter(field, from, to, "yyyy-MM-dd")
  }
}

case class EqualityFilter(field: String, value: String
                          , override val isForceFilter: Boolean = false
                          , override val isOverridable: Boolean = false
                         ) extends ForcedFilter {
  override def operator = EqualityFilterOperation
  val asValues: String = value
  override def canBeHighCardinalityFilter: Boolean = true
}

case class GreaterThanFilter(field: String, value: String
                          , override val isForceFilter: Boolean = false
                          , override val isOverridable: Boolean = false
                         ) extends ForcedFilter {
  override def operator = GreaterThanFilterOperation
  val asValues: String = value
  override def canBeHighCardinalityFilter: Boolean = true
}

case class LessThanFilter(field: String, value: String
                             , override val isForceFilter: Boolean = false
                             , override val isOverridable: Boolean = false
                            ) extends ForcedFilter {
  override def operator = LessThanFilterOperation
  val asValues: String = value
  override def canBeHighCardinalityFilter: Boolean = true
}

case class JavaScriptFilter(field: String, function: String
                          , override val isForceFilter: Boolean = false
                          , override val isOverridable: Boolean = false
                         ) extends ForcedFilter {
  override def operator = NoopFilterOperation
  val asValues: String = function
  override def canBeHighCardinalityFilter: Boolean = true
}

case class FieldEqualityFilter(field: String, compareTo: String
                                  , override val isForceFilter: Boolean = false
                                  , override val isOverridable: Boolean = false
                                 ) extends MultiFieldForcedFilter {
  override def operator = FieldEqualityFilterOperation
  val asValues: String = field + "," + compareTo
  override def canBeHighCardinalityFilter: Boolean = true
}

sealed trait ValuesFilter extends ForcedFilter {
  def values: List[String]
  def renameField(newField: String): ValuesFilter
  val asValues: String = values.mkString(",")
}
case class InFilter(field: String, values: List[String]
                    , override val isForceFilter: Boolean = false
                    , override val isOverridable: Boolean = false) extends ValuesFilter {
  override def operator = InFilterOperation
  override def renameField(newField: String): ValuesFilter = this.copy(field = newField)
  override def canBeHighCardinalityFilter: Boolean = true
}
case class NotInFilter(field: String, values: List[String]
                       , override val isForceFilter: Boolean = false
                       , override val isOverridable: Boolean = false) extends ValuesFilter {
  override def operator = NotInFilterOperation
  override def renameField(newField: String): ValuesFilter = this.copy(field = newField)
  override def canBeHighCardinalityFilter: Boolean = true
}
case class LikeFilter(field: String, value: String
                      , override val isForceFilter: Boolean = false
                      , override val isOverridable: Boolean = false) extends ForcedFilter {
  override def operator = LikeFilterOperation
  val asValues: String = value
  override def canBeHighCardinalityFilter: Boolean = true
}

case class NotLikeFilter(field: String, value: String
                      , override val isForceFilter: Boolean = false
                      , override val isOverridable: Boolean = false) extends ForcedFilter {
  override def operator = NotLikeFilterOperation
  val asValues: String = value
  override def canBeHighCardinalityFilter: Boolean = true
}

case class NotEqualToFilter(field: String, value: String
                            , override val isForceFilter: Boolean = false
                            , override val isOverridable: Boolean = false) extends ForcedFilter {
  override def operator = NotEqualToFilterOperation
  val asValues: String = value
  override def canBeHighCardinalityFilter: Boolean = true
}
case class IsNullFilter(field: String
                        , override val isForceFilter: Boolean = false
                        , override val isOverridable: Boolean = false) extends ForcedFilter {
  override def operator = IsNullFilterOperation
  val asValues: String = org.apache.commons.lang3.StringUtils.EMPTY
}
case class IsNotNullFilter(field: String
                           , override val isForceFilter: Boolean = false
                           , override val isOverridable: Boolean = false) extends ForcedFilter {
  override def operator = IsNotNullFilterOperation
  val asValues: String = org.apache.commons.lang3.StringUtils.EMPTY
}

sealed trait CombiningFilter {
  def isEmpty : Boolean
}

case class OrFilter(filters: List[Filter]) extends ForcedFilter with CombiningFilter {
  override def operator: FilterOperation = OrFilterOperation
  override def field: String = "or"
  override def isEmpty : Boolean = filters.isEmpty
  val asValues: String = filters.map(_.asValues).mkString("(",") OR (",")")
}

case class AndFilter(filters: List[Filter]) extends ForcedFilter with CombiningFilter {
  override def operator: FilterOperation = AndFilterOperation
  override def field: String = "and"
  override def isEmpty : Boolean = filters.isEmpty
  val asValues: String = filters.map(_.asValues).mkString("(",") AND (",")")
}

case class RenderedAndFilter(filters: Iterable[String]) extends CombiningFilter {
  def isEmpty : Boolean = filters.isEmpty
  override def toString: String = filters.mkString("(",") AND (",")")
}

case class RenderedOrFilter(filters: Iterable[String]) extends CombiningFilter {
  def isEmpty : Boolean = filters.isEmpty
  override def toString: String = filters.mkString("(",") OR (",")")
}

sealed trait FilterRenderer[T, O] {
  def render(aliasToRenderedSqlMap: Map[String, (String, String)],
             filter: T,
             literalMapper: SqlLiteralMapper,
             column: Column,
             engine: Engine,
             grainOption: Option[Grain]): O
}

sealed trait BetweenFilterRenderer[O] extends FilterRenderer[BetweenFilter, O]

sealed trait DateTimeFilterRenderer[O] extends FilterRenderer[DateTimeBetweenFilter, O]

sealed trait InFilterRenderer[O] extends FilterRenderer[InFilter, O]

sealed trait NotInFilterRenderer[O] extends FilterRenderer[NotInFilter, O]

sealed trait EqualityFilterRenderer[O] extends FilterRenderer[EqualityFilter, O]

sealed trait GreaterThanFilterRenderer[O] extends FilterRenderer[GreaterThanFilter, O]

sealed trait LessThanFilterRenderer[O] extends FilterRenderer[LessThanFilter, O]

sealed trait LikeFilterRenderer[O] extends FilterRenderer[LikeFilter, O]

sealed trait NotLikeFilterRenderer[O] extends FilterRenderer[NotLikeFilter, O]

sealed trait NotEqualToFilterRenderer[O] extends FilterRenderer[NotEqualToFilter, O]

sealed trait IsNullFilterRenderer[O] extends FilterRenderer[IsNullFilter, O]

sealed trait IsNotNullFilterRenderer[O] extends FilterRenderer[IsNotNullFilter, O]

sealed trait JavaScriptFilterRenderer[O] extends FilterRenderer[JavaScriptFilter, O]

sealed trait FieldEqualityFilterRenderer[O] extends FilterRenderer[FieldEqualityFilter, O]


sealed trait SqlResult {
  def filter: String
  def escaped: Boolean
}

case class DefaultResult(filter: String, escaped: Boolean = false) extends SqlResult

/**
  * Categorizes an OrFilter by the type of its contained filtered Columns.
  * OrFilter can only directly compare columns of the same type.
  * @param orFilter   - Current OrFilter to categorize.
  * @param filterType - Category of the stored Filters for the current OrFilter.
  */
case class OrFilterMeta(orFilter: OrFilter, filterType: MetaType)

/**
  * All types of Filter Meta available:
  * MetricType  - Column is found in a PublicFact and represents a value that gets aggregated.
  * FactType    - Column is found in a PublicFact and are considered Dimensions but are not part of a joined in Dimension table.
  * DimType     - Column is found in a Dimension table joined in to the primary Fact.
  */
object MetaType extends Enumeration {
  type MetaType = Value
  val MetricType, FactType, DimType = Value
}

object SqlBetweenFilterRenderer extends BetweenFilterRenderer[SqlResult] {
  def render(aliasToRenderedSqlMap: Map[String, (String, String)],
             betweenFilter: BetweenFilter,
             literalMapper: SqlLiteralMapper,
             column: Column,
             engine: Engine,
             grainOption: Option[Grain]) : SqlResult = {
    val name = aliasToRenderedSqlMap(betweenFilter.field)._2
    val renderedFrom = literalMapper.toLiteral(column, betweenFilter.from, grainOption)
    val renderedTo = literalMapper.toLiteral(column, betweenFilter.to, grainOption)
    engine match {
      case OracleEngine =>
        column.dataType match {
          case DateType(format) =>
            if(grainOption.isDefined) {
               grainOption.get match {
                 case HourlyGrain  =>
                   DefaultResult(s"""$name >= $renderedFrom AND $name <= $renderedTo""")
                 case _=>
                   DefaultResult(s"""$name >= trunc($renderedFrom) AND $name <= trunc($renderedTo)""")
               }
            } else {
              DefaultResult(s"""$name >= trunc($renderedFrom) AND $name <= trunc($renderedTo)""")
            }
          case i: IntType if column.annotations.contains(DayColumn.instance) =>
            column.annotations.find(_.isInstanceOf[DayColumn]).fold(throw new IllegalStateException("Failed to find DayColumn when expected")){
              ca:ColumnAnnotation =>
                val dayColumn = ca.asInstanceOf[DayColumn]
                val fmt = dayColumn.fmt
                grainOption match {
                  case Some(HourlyGrain) =>
                    DefaultResult(s"""$name >= $renderedFrom AND $name <= $renderedTo""")
                  case _ =>
                    DefaultResult(s"""$name >= to_number(to_char(trunc($renderedFrom), '$fmt')) AND $name <= to_number(to_char(trunc($renderedTo), '$fmt'))""")
                }
            }
          case i: StrType if column.annotations.contains(DayColumn.instance) =>
            column.annotations.find(_.isInstanceOf[DayColumn]).fold(throw new IllegalStateException("Failed to find DayColumn when expected")){
              ca:ColumnAnnotation =>
                val dayColumn = ca.asInstanceOf[DayColumn]
                val fmt = dayColumn.fmt
                grainOption match {
                  case Some(HourlyGrain) =>
                    DefaultResult(s"""$name >= $renderedFrom AND $name <= $renderedTo""")
                  case _ =>
                    DefaultResult(s"""$name >= to_char(trunc($renderedFrom), '$fmt') AND $name <= to_char(trunc($renderedTo), '$fmt')""")
                }
            }
          case _ =>
            DefaultResult(s"""$name >= $renderedFrom AND $name <= $renderedTo""")
        }
      case PostgresEngine =>
        column.dataType match {
          case DateType(format) =>
            if(grainOption.isDefined) {
              grainOption.get match {
                case HourlyGrain  =>
                  DefaultResult(s"""$name >= $renderedFrom AND $name <= $renderedTo""")
                case _=>
                  DefaultResult(s"""$name >= DATE_TRUNC('DAY', $renderedFrom) AND $name <= DATE_TRUNC('DAY', $renderedTo)""")
              }
            } else {
              DefaultResult(s"""$name >= DATE_TRUNC('DAY', $renderedFrom) AND $name <= DATE_TRUNC('DAY', $renderedTo)""")
            }
          case i: IntType if column.annotations.contains(DayColumn.instance) =>
            column.annotations.find(_.isInstanceOf[DayColumn]).fold(throw new IllegalStateException("Failed to find DayColumn when expected")){
              ca:ColumnAnnotation =>
                val dayColumn = ca.asInstanceOf[DayColumn]
                val fmt = dayColumn.fmt
                grainOption match {
                  case Some(HourlyGrain) =>
                    DefaultResult(s"""$name >= $renderedFrom AND $name <= $renderedTo""")
                  case _ =>
                    DefaultResult(s"""$name >= to_char(DATE_TRUNC('DAY', $renderedFrom), '$fmt')::INTEGER AND $name <= to_char(DATE_TRUNC('DAY', $renderedTo), '$fmt')::INTEGER""")
                }
            }
          case i: StrType if column.annotations.contains(DayColumn.instance) =>
            column.annotations.find(_.isInstanceOf[DayColumn]).fold(throw new IllegalStateException("Failed to find DayColumn when expected")){
              ca:ColumnAnnotation =>
                val dayColumn = ca.asInstanceOf[DayColumn]
                val fmt = dayColumn.fmt
                grainOption match {
                  case Some(HourlyGrain) =>
                    DefaultResult(s"""$name >= $renderedFrom AND $name <= $renderedTo""")
                  case _ =>
                    DefaultResult(s"""$name >= to_char(DATE_TRUNC('DAY', $renderedFrom), '$fmt') AND $name <= to_char(DATE_TRUNC('DAY', $renderedTo), '$fmt')""")
                }
            }
          case _ =>
            DefaultResult(s"""$name >= $renderedFrom AND $name <= $renderedTo""")
        }
      case HiveEngine =>
        DefaultResult(s"""$name >= $renderedFrom AND $name <= $renderedTo""")
      case PrestoEngine =>
        DefaultResult(s"""$name >= $renderedFrom AND $name <= $renderedTo""")
      case BigqueryEngine =>
        DefaultResult(s"""$name >= $renderedFrom AND $name <= $renderedTo""")
      case _ =>
        throw new IllegalArgumentException(s"Unsupported engine for BetweenFilterRenderer $engine")
    }
  }
}

object SqlDateTimeBetweenFilterRenderer extends DateTimeFilterRenderer[SqlResult] {
  def render(aliasToRenderedSqlMap: Map[String, (String, String)],
             datetimeFilter: DateTimeBetweenFilter,
             literalMapper: SqlLiteralMapper,
             column: Column,
             engine: Engine,
             grainOption: Option[Grain]) : SqlResult = {
    val name = aliasToRenderedSqlMap(datetimeFilter.field)._2
    require(column.dataType.isInstanceOf[DateType] || column.dataType.isInstanceOf[TimestampType]
      , s"DateTimeFilter must be on a column with data type of DateType or TimestampType : $name : ${column.dataType}")
    val fromDateTime = DateType.formatter(datetimeFilter.format).parseDateTime(datetimeFilter.from)
    val toDateTime = DateType.formatter(datetimeFilter.format).parseDateTime(datetimeFilter.to)
    engine match {
      case OracleEngine =>
        column.dataType match {
          case DateType(format) =>
            val ISO8601DateFormat = ISODateTimeFormat.yearMonthDay().withZoneUTC()
            val fromISO = ISO8601DateFormat.print(fromDateTime)
            val toISO = ISO8601DateFormat.print(toDateTime)
            val from = s"""'$fromISO'"""
            val to = s"""'$toISO'"""
            val renderedFrom = s"to_date($from, 'YYYY-MM-DD')"
            val renderedTo = s"to_date($to, 'YYYY-MM-DD')"
            DefaultResult(s"""$name >= $renderedFrom AND $name <= $renderedTo""")
          case TimestampType(format) =>
            val ISO8601DateFormat = ISODateTimeFormat.dateTime().withZoneUTC()
            val fromISO = ISO8601DateFormat.print(fromDateTime)
            val toISO = ISO8601DateFormat.print(toDateTime)
            val from = s"""'$fromISO'"""
            val to = s"""'$toISO'"""
            val renderedFrom = s"TO_UTC_TIMESTAMP_TZ($from)"
            val renderedTo = s"TO_UTC_TIMESTAMP_TZ($to)"
            DefaultResult(s"""$name >= $renderedFrom AND $name <= $renderedTo""")
          case a =>
            throw new IllegalArgumentException(s"Unsupported data type : $a")
        }
      case PostgresEngine =>
        column.dataType match {
          case DateType(format) =>
            val ISO8601DateFormat = ISODateTimeFormat.yearMonthDay().withZoneUTC()
            val fromISO = ISO8601DateFormat.print(fromDateTime)
            val toISO = ISO8601DateFormat.print(toDateTime)
            val from = s"""'$fromISO'"""
            val to = s"""'$toISO'"""
            val renderedFrom = s"to_date($from, 'YYYY-MM-DD')"
            val renderedTo = s"to_date($to, 'YYYY-MM-DD')"
            DefaultResult(s"""$name >= $renderedFrom AND $name <= $renderedTo""")
          case TimestampType(format) =>
            val ISO8601DateFormat = ISODateTimeFormat.dateTime().withZoneUTC()
            val fromISO = ISO8601DateFormat.print(fromDateTime)
            val toISO = ISO8601DateFormat.print(toDateTime)
            val from = s"""'$fromISO'::timestamptz"""
            val to = s"""'$toISO'::timestamptz"""
            val renderedFrom = from
            val renderedTo = to
            DefaultResult(s"""$name >= $renderedFrom AND $name <= $renderedTo""")
          case a =>
            throw new IllegalArgumentException(s"Unsupported data type : $a")
        }
      case HiveEngine =>
        column.dataType match {
          case DateType(format) =>
            //yyyy-MM-dd'T'HH:mm:ss.SSSZZ
            val ISO8601DateFormat = ISODateTimeFormat.yearMonthDay().withZoneUTC()
            val fromISO = ISO8601DateFormat.print(fromDateTime)
            val toISO = ISO8601DateFormat.print(toDateTime)
            val from = s"""'$fromISO'"""
            val to = s"""'$toISO'"""
            val renderedFrom = s"cast($from as date)"
            val renderedTo = s"cast($to as date)"
            DefaultResult(s"""$name >= $renderedFrom AND $name <= $renderedTo""")
          case TimestampType(format) =>
            //yyyy-MM-dd'T'HH:mm:ss.SSSZZ
            val ISO8601DateFormat = ISODateTimeFormat.dateHourMinuteSecondMillis().withZoneUTC()
            val fromISO = ISO8601DateFormat.print(fromDateTime)
            val toISO = ISO8601DateFormat.print(toDateTime)
            val from = s"""'$fromISO'"""
            val to = s"""'$toISO'"""
            val renderedFrom = s"cast($from as timestamp)"
            val renderedTo = s"cast($to as timestamp)"
            DefaultResult(s"""$name >= $renderedFrom AND $name <= $renderedTo""")
          case a =>
            throw new IllegalArgumentException(s"Unsupported data type : $a")
        }
      case PrestoEngine =>
        column.dataType match {
          case DateType(format) =>
            //yyyy-MM-dd'T'HH:mm:ss.SSSZZ
            val ISO8601DateFormat = ISODateTimeFormat.yearMonthDay().withZoneUTC()
            val fromISO = ISO8601DateFormat.print(fromDateTime)
            val toISO = ISO8601DateFormat.print(toDateTime)
            val from = s"""'$fromISO'"""
            val to = s"""'$toISO'"""
            val renderedFrom = s"date($from)"
            val renderedTo = s"date($to)"
            DefaultResult(s"""$name >= $renderedFrom AND $name <= $renderedTo""")
          case TimestampType(format) =>
            //yyyy-MM-dd'T'HH:mm:ss.SSSZZ
            val ISO8601DateFormat = ISODateTimeFormat.dateTime().withZoneUTC()
            val fromISO = ISO8601DateFormat.print(fromDateTime)
            val toISO = ISO8601DateFormat.print(toDateTime)
            val from = s"""'$fromISO'"""
            val to = s"""'$toISO'"""
            val renderedFrom = s"from_iso8601_timestamp($from)"
            val renderedTo = s"from_iso8601_timestamp($to)"
            DefaultResult(s"""$name >= $renderedFrom AND $name <= $renderedTo""")
          case a =>
            throw new IllegalArgumentException(s"Unsupported data type : $a")
        }
      case BigqueryEngine =>
        column.dataType match {
          case DateType(format) =>
            val ISO8601DateFormat = ISODateTimeFormat.yearMonthDay().withZoneUTC()
            val fromISO = ISO8601DateFormat.print(fromDateTime)
            val toISO = ISO8601DateFormat.print(toDateTime)
            val from = s"""'$fromISO'"""
            val to = s"""'$toISO'"""
            val renderedFrom = s"DATE($from)"
            val renderedTo = s"DATE($to)"
            DefaultResult(s"""$name >= $renderedFrom AND $name <= $renderedTo""")
          case TimestampType(format) =>
            val ISO8601DateFormat = ISODateTimeFormat.dateHourMinuteSecondMillis().withZoneUTC()
            val fromISO = ISO8601DateFormat.print(fromDateTime)
            val toISO = ISO8601DateFormat.print(toDateTime)
            val from = s"""'$fromISO'"""
            val to = s"""'$toISO'"""
            val renderedFrom = s"TIMESTAMP($from)"
            val renderedTo = s"TIMESTAMP($to)"
            DefaultResult(s"""$name >= $renderedFrom AND $name <= $renderedTo""")
          case a =>
            throw new IllegalArgumentException(s"Unsupported data type : $a")
        }
      case _ =>
        throw new IllegalArgumentException(s"Unsupported engine for DateTimeFilterRenderer $engine")
    }
  }
}

object SqlInFilterRenderer extends InFilterRenderer[SqlResult] {
  def render(aliasToRenderedSqlMap: Map[String, (String, String)],
             filter: InFilter,
             literalMapper: SqlLiteralMapper,
             column: Column,
             engine: Engine,
             grainOption: Option[Grain]) : SqlResult = {
    val name = aliasToRenderedSqlMap(filter.field)._2
    val renderedValues = filter.values.map(literalMapper.toLiteral(column, _, grainOption))
    engine match {
      case OracleEngine =>
        column.dataType match {
          case StrType(_, _, _) if column.caseInSensitive =>
            DefaultResult(s"""lower($name) IN (${renderedValues.map(l => s"lower($l)").mkString(",")})""")
          case _ =>
            DefaultResult(s"""$name IN (${renderedValues.mkString(",")})""")
        }
      case PostgresEngine =>
        column.dataType match {
          case StrType(_, _, _) if column.caseInSensitive =>
            DefaultResult(s"""lower($name) IN (${renderedValues.map(l => s"lower($l)").mkString(",")})""")
          case _ =>
            DefaultResult(s"""$name IN (${renderedValues.mkString(",")})""")
        }
      case HiveEngine | PrestoEngine | BigqueryEngine =>
        column.dataType match {
          case StrType(_, _, _) if column.caseInSensitive =>
            DefaultResult(s"""lower($name) IN (${renderedValues.map(l => s"lower($l)").mkString(",")})""")
          case _ =>
            DefaultResult(s"""$name IN (${renderedValues.mkString(",")})""")
        }
      case _ =>
        throw new IllegalArgumentException(s"Unsupported engine for InFilterRenderer $engine")
    }
  }
}

object SqlNotInFilterRenderer extends NotInFilterRenderer[SqlResult] {
  def render(aliasToRenderedSqlMap: Map[String, (String, String)],
             filter: NotInFilter,
             literalMapper: SqlLiteralMapper,
             column: Column,
             engine: Engine,
             grainOption: Option[Grain]) : SqlResult = {
    val name = aliasToRenderedSqlMap(filter.field)._2
    val renderedValues = filter.values.map(literalMapper.toLiteral(column, _, grainOption))
    engine match {
      case OracleEngine =>
        column.dataType match {
          case StrType(_, _, _) if column.caseInSensitive =>
            DefaultResult(s"""lower($name) NOT IN (${renderedValues.map(l => s"lower($l)").mkString(",")})""")
          case _ =>
            DefaultResult(s"""$name NOT IN (${renderedValues.mkString(",")})""")
        }
      case PostgresEngine | BigqueryEngine =>
        column.dataType match {
          case StrType(_, _, _) if column.caseInSensitive =>
            DefaultResult(s"""lower($name) NOT IN (${renderedValues.map(l => s"lower($l)").mkString(",")})""")
          case _ =>
            DefaultResult(s"""$name NOT IN (${renderedValues.mkString(",")})""")
        }
      case HiveEngine | PrestoEngine =>
        DefaultResult(s"""$name NOT IN (${renderedValues.mkString(",")})""")
      case _ =>
        throw new IllegalArgumentException(s"Unsupported engine for NotInFilterRenderer $engine")
    }
  }
}

object SqlEqualityFilterRenderer extends EqualityFilterRenderer[SqlResult] {
  def render(aliasToRenderedSqlMap: Map[String, (String, String)],
             filter: EqualityFilter,
             literalMapper: SqlLiteralMapper,
             column: Column,
             engine: Engine,
             grainOption : Option[Grain]) : SqlResult = {
    val name = aliasToRenderedSqlMap(filter.field)._2
    val renderedValue = literalMapper.toLiteral(column, filter.value, grainOption)
    engine match {
      case OracleEngine =>
        column.dataType match {
          case StrType(_, _, _) if column.caseInSensitive =>
            DefaultResult(s"""lower($name) = lower($renderedValue)""")
          case _ =>
            DefaultResult(s"""$name = $renderedValue""")
        }
      case PostgresEngine =>
        column.dataType match {
          case StrType(_, _, _) if column.caseInSensitive =>
            DefaultResult(s"""lower($name) = lower($renderedValue)""")
          case _ =>
            DefaultResult(s"""$name = $renderedValue""")
        }
      case HiveEngine =>
        column.dataType match {
          case StrType(_, _, _) if column.caseInSensitive =>
            DefaultResult(s"""lower($name) = lower($renderedValue)""")
          case _ =>
            DefaultResult(s"""$name = $renderedValue""")
        }
      case PrestoEngine | BigqueryEngine =>
        column.dataType match {
          case StrType(_, _, _) if column.caseInSensitive =>
            DefaultResult(s"""lower($name) = lower($renderedValue)""")
          case _ =>
            DefaultResult(s"""$name = $renderedValue""")
        }
      case _ =>
        throw new IllegalArgumentException(s"Unsupported engine for EqualityFilterRenderer $engine")
    }
  }
}

object SqlFieldEqualityFilterRenderer extends FieldEqualityFilterRenderer[SqlResult] {

  def checkTypesCreateResultOrThrow(name: String,
                                    compareTo: String,
                                    column: Column,
                                    otherColumn: Column): SqlResult = {
    if(column.dataType.jsonDataType != otherColumn.dataType.jsonDataType) {
      throw new IllegalArgumentException(s"Both dataTypes should be the same, but are ${column.dataType} and ${otherColumn.dataType}")
    }

    (column.dataType, otherColumn.dataType) match {
      case (StrType(_,_,_), StrType(_,_,_)) if column.caseInSensitive && otherColumn.caseInSensitive =>
        DefaultResult(s"""lower($name) = lower($compareTo)""")
      case (StrType(_,_,_), StrType(_,_,_)) if column.caseInSensitive =>
        DefaultResult(s"""lower($name) = $compareTo""")
      case (StrType(_,_,_), StrType(_,_,_)) if otherColumn.caseInSensitive =>
        DefaultResult(s"""$name = lower($compareTo)""")
      case _ =>
        DefaultResult(s"""$name = $compareTo""")
    }
  }

  def render(aliasToRenderedSqlMap: Map[String, (String, String)],
             filter: FieldEqualityFilter,
             literalMapper: SqlLiteralMapper,
             column: Column,
             engine: Engine,
             grainOption : Option[Grain]) : SqlResult = {
    val renderedName: String = aliasToRenderedSqlMap(filter.field)._2
    val renderedCompareTo: String = aliasToRenderedSqlMap(filter.compareTo)._2
    val compareTo: String = aliasToRenderedSqlMap(filter.compareTo)._1
    val otherColumn: Column = column.columnContext.getColumnByName(compareTo).get
    engine match {
      case OracleEngine | HiveEngine | PrestoEngine | PostgresEngine | BigqueryEngine =>
        checkTypesCreateResultOrThrow(renderedName, renderedCompareTo, column, otherColumn)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported engine for FieldEqualityFilterRenderer $engine")
    }
  }
}

object SqlFilterRenderFactory {
  def renderComparisonFilterWithOperator(name: String,
                                         filter: Filter,
                                         literalMapper: LiteralMapper,
                                         column: Column,
                                         engine: Engine,
                                         grainOption: Option[Grain],
                                         operator: String,
                                         validEngineSet: Set[Engine]
                                        ): SqlResult = {
    val renderedValue = literalMapper.toLiteral(column, filter.asInstanceOf[ForcedFilter].asValues, grainOption)
    if (validEngineSet.contains(engine)) {
      column.dataType match {
        case StrType(_, _, _) if column.caseInSensitive =>
          DefaultResult(s"""lower($name) $operator lower($renderedValue)""")
        case _ =>
          DefaultResult(s"""$name $operator $renderedValue""")
      }
    } else
      throw new IllegalArgumentException(s"Unsupported engine for Operator: ${filter.operator} $engine")
  }
}

object SqlGreaterThanFilterRenderer extends GreaterThanFilterRenderer[SqlResult] {
  def render(aliasToRenderedSqlMap: Map[String, (String, String)],
             filter: GreaterThanFilter,
             literalMapper: SqlLiteralMapper,
             column: Column,
             engine: Engine,
             grainOption : Option[Grain]) : SqlResult = {
    val name: String = aliasToRenderedSqlMap(filter.field)._2
    SqlFilterRenderFactory.renderComparisonFilterWithOperator(name, filter, literalMapper, column, engine, grainOption, ">", Set(OracleEngine, HiveEngine, PrestoEngine, PostgresEngine, BigqueryEngine))
  }
}

object SqlLessThanFilterRenderer extends LessThanFilterRenderer[SqlResult] {
  def render(aliasToRenderedSqlMap: Map[String, (String, String)],
             filter: LessThanFilter,
             literalMapper: SqlLiteralMapper,
             column: Column,
             engine: Engine,
             grainOption : Option[Grain]) : SqlResult = {
    val name: String = aliasToRenderedSqlMap(filter.field)._2
    SqlFilterRenderFactory.renderComparisonFilterWithOperator(name, filter, literalMapper, column, engine, grainOption, "<", Set(OracleEngine, HiveEngine, PrestoEngine, PostgresEngine, BigqueryEngine))
  }
}

object SqlLikeFilterRenderer extends LikeFilterRenderer[SqlResult] {

  private[this] val specialCharsR = """[%_\\]""".r

  def escapeEscapeChars(value: String) : String = {
    value.replaceAllLiterally("""\""", """\\""")
  }

  def escapeSpecialChars(value: String) : String = {
    escapeEscapeChars(value).replaceAllLiterally("""%""","""\%""").replaceAllLiterally("""_""", """\_""")
  }

  def render(aliasToRenderedSqlMap: Map[String, (String, String)],
             filter: LikeFilter,
             literalMapper: SqlLiteralMapper,
             column: Column,
             engine: Engine,
             grainOption: Option[Grain]) : SqlResult = {
    val name = aliasToRenderedSqlMap(filter.field)._2
    val escaped = specialCharsR.findFirstIn(filter.value).isDefined
    val escapeValue = {
      if(escaped) {
        escapeSpecialChars(filter.value)
      } else {
        filter.value
      }
    }
    val renderedValue = literalMapper.toLiteral(column, s"%$escapeValue%", grainOption)
    engine match {
      case OracleEngine =>
        column.dataType match {
          case StrType(_, _, _) if column.caseInSensitive =>
            if(escaped) {
              DefaultResult( s"""lower($name) LIKE lower($renderedValue) ESCAPE '\\'""", escaped = escaped)
            } else {
              DefaultResult( s"""lower($name) LIKE lower($renderedValue)""", escaped = escaped)
            }
          case _ =>
            if(escaped) {
              DefaultResult( s"""$name LIKE $renderedValue ESCAPE '\\'""", escaped = escaped)
            } else {
              DefaultResult( s"""$name LIKE $renderedValue""", escaped = escaped)
            }
        }
      case PostgresEngine =>
        column.dataType match {
          case StrType(_, _, _) if column.caseInSensitive =>
            if(escaped) {
              DefaultResult( s"""lower($name) LIKE lower($renderedValue) ESCAPE '\\'""", escaped = escaped)
            } else {
              DefaultResult( s"""lower($name) LIKE lower($renderedValue)""", escaped = escaped)
            }
          case _ =>
            if(escaped) {
              DefaultResult( s"""$name LIKE $renderedValue ESCAPE '\\'""", escaped = escaped)
            } else {
              DefaultResult( s"""$name LIKE $renderedValue""", escaped = escaped)
            }
        }
      case HiveEngine | PrestoEngine =>
        column.dataType match {
          case StrType(_, _, _) if column.caseInSensitive =>
              DefaultResult( s"""lower($name) LIKE lower($renderedValue)""", escaped = escaped)
          case _ =>
            DefaultResult(s"""$name LIKE $renderedValue""")
        }
      case BigqueryEngine =>
        val escapedEscapeValue = escapeEscapeChars(escapeValue)
        val bqRenderedValue = literalMapper.toLiteral(column, s"%$escapedEscapeValue%", grainOption)
        column.dataType match {
          case StrType(_, _, _) if column.caseInSensitive =>
            DefaultResult( s"""lower($name) LIKE lower($bqRenderedValue)""", escaped = escaped)
          case _ =>
            DefaultResult(s"""$name LIKE $bqRenderedValue""")
        }
      case _ =>
        throw new IllegalArgumentException(s"Unsupported engine for LikeFilterRenderer $engine")
    }
  }
}

object SqlNotLikeFilterRenderer extends NotLikeFilterRenderer[SqlResult] {

  private[this] val specialCharsR = """[%_\\]""".r

  def escapeEscapeChars(value: String) : String = {
    value.replaceAllLiterally("""\""", """\\""")
  }

  def escapeSpecialChars(value: String) : String = {
    escapeEscapeChars(value).replaceAllLiterally("""%""","""\%""").replaceAllLiterally("""_""", """\_""")
  }

  def render(aliasToRenderedSqlMap: Map[String, (String, String)],
             filter: NotLikeFilter,
             literalMapper: SqlLiteralMapper,
             column: Column,
             engine: Engine,
             grainOption: Option[Grain]) : SqlResult = {
    val name = aliasToRenderedSqlMap(filter.field)._2
    val escaped = specialCharsR.findFirstIn(filter.value).isDefined
    val escapeValue = {
      if(escaped) {
        escapeSpecialChars(filter.value)
      } else {
        filter.value
      }
    }
    val renderedValue = literalMapper.toLiteral(column, s"%$escapeValue%", grainOption)
    engine match {
      case OracleEngine =>
        column.dataType match {
          case StrType(_, _, _) if column.caseInSensitive =>
            if(escaped) {
              DefaultResult( s"""lower($name) NOT LIKE lower($renderedValue) ESCAPE '\\'""", escaped = escaped)
            } else {
              DefaultResult( s"""lower($name) NOT LIKE lower($renderedValue)""", escaped = escaped)
            }
          case _ =>
            if(escaped) {
              DefaultResult( s"""$name NOT LIKE $renderedValue ESCAPE '\\'""", escaped = escaped)
            } else {
              DefaultResult( s"""$name NOT LIKE $renderedValue""", escaped = escaped)
            }
        }
      case PostgresEngine =>
        column.dataType match {
          case StrType(_, _, _) if column.caseInSensitive =>
            if(escaped) {
              DefaultResult( s"""lower($name) NOT LIKE lower($renderedValue) ESCAPE '\\'""", escaped = escaped)
            } else {
              DefaultResult( s"""lower($name) NOT LIKE lower($renderedValue)""", escaped = escaped)
            }
          case _ =>
            if(escaped) {
              DefaultResult( s"""$name NOT LIKE $renderedValue ESCAPE '\\'""", escaped = escaped)
            } else {
              DefaultResult( s"""$name NOT LIKE $renderedValue""", escaped = escaped)
            }
        }
      case HiveEngine | PrestoEngine =>
        column.dataType match {
          case StrType(_, _, _) if column.caseInSensitive =>
            DefaultResult( s"""lower($name) NOT LIKE lower($renderedValue)""", escaped = escaped)
          case _ =>
            DefaultResult(s"""$name NOT LIKE $renderedValue""")
        }
      case BigqueryEngine =>
        val escapedEscapeValue = escapeEscapeChars(escapeValue)
        val bqRenderedValue = literalMapper.toLiteral(column, s"%$escapedEscapeValue%", grainOption)
        column.dataType match {
          case StrType(_, _, _) if column.caseInSensitive =>
            DefaultResult( s"""lower($name) NOT LIKE lower($bqRenderedValue)""", escaped = escaped)
          case _ =>
            DefaultResult(s"""$name NOT LIKE $bqRenderedValue""")
        }
      case _ =>
        throw new IllegalArgumentException(s"Unsupported engine for NotLikeFilterRenderer $engine")
    }
  }
}

object SqlNotEqualToFilterRenderer extends NotEqualToFilterRenderer[SqlResult] {
  def render(aliasToRenderedSqlMap: Map[String, (String, String)],
             filter: NotEqualToFilter,
             literalMapper: SqlLiteralMapper,
             column: Column,
             engine: Engine,
             grainOption: Option[Grain]) : SqlResult = {
    val name = aliasToRenderedSqlMap(filter.field)._2
    val renderedValue = literalMapper.toLiteral(column, filter.value, grainOption)
    engine match {
      case OracleEngine =>
        column.dataType match {
          case StrType(_, _, _) if column.caseInSensitive =>
            DefaultResult(s"""lower($name) <> lower($renderedValue)""")
          case _ =>
            DefaultResult(s"""$name <> $renderedValue""")
        }
      case PostgresEngine | BigqueryEngine =>
        column.dataType match {
          case StrType(_, _, _) if column.caseInSensitive =>
            DefaultResult(s"""lower($name) <> lower($renderedValue)""")
          case _ =>
            DefaultResult(s"""$name <> $renderedValue""")
        }
      case HiveEngine  | PrestoEngine =>
        DefaultResult(s"""$name <> $renderedValue""")
      case _ =>
        throw new IllegalArgumentException(s"Unsupported engine for NotEqualToFilterRenderer $engine")
    }
  }
}
object SqlIsNullFilterRenderer extends IsNullFilterRenderer[SqlResult] {
  def render(aliasToRenderedSqlMap: Map[String, (String, String)],
             filter: IsNullFilter,
             literalMapper: SqlLiteralMapper,
             column: Column,
             engine: Engine,
             grainOption: Option[Grain]) : SqlResult = {
    val name = aliasToRenderedSqlMap(filter.field)._2
    engine match {
      case OracleEngine | HiveEngine | PrestoEngine | PostgresEngine | BigqueryEngine =>
        DefaultResult(s"""$name IS NULL""")
      case _ =>
        throw new IllegalArgumentException(s"Unsupported engine for IsNullFilterRenderer $engine")
    }
  }
}
object SqlIsNotNullFilterRenderer extends IsNotNullFilterRenderer[SqlResult] {
  def render(aliasToRenderedSqlMap: Map[String, (String, String)],
             filter: IsNotNullFilter,
             literalMapper: SqlLiteralMapper,
             column: Column,
             engine: Engine,
             grainOption: Option[Grain]) : SqlResult = {
    val name = aliasToRenderedSqlMap(filter.field)._2
    engine match {
      case OracleEngine | HiveEngine | PrestoEngine | PostgresEngine | BigqueryEngine =>
        DefaultResult(s"""$name IS NOT NULL""")
      case _ =>
        throw new IllegalArgumentException(s"Unsupported engine for IsNotNullFilterRenderer $engine")
    }
  }
}

object FilterDruid {
  import org.apache.druid.query.filter.{DimFilter, NotDimFilter, OrDimFilter, SearchQueryDimFilter, SelectorDimFilter, BoundDimFilter, ColumnComparisonDimFilter}
  import org.apache.druid.query.groupby.having._
  import org.apache.druid.query.search.InsensitiveContainsSearchQuerySpec
  import org.joda.time.DateTime

  import collection.JavaConverters._


  val druidLiteralMapper = new DruidLiteralMapper

  def extractFromAndToDate(f: Filter, g: Grain) : (DateTime, DateTime) = {
    f match {
      case dtf: DateTimeBetweenFilter =>
        val f = g.fromFullFormattedString(g.toFullFormattedString(dtf.fromDateTime))
        val t = g.fromFullFormattedString(g.toFullFormattedString(dtf.toDateTime))
        (f, t)
      case BetweenFilter(_, from, to) =>
        val f = g.fromFormattedString(from)
        val t = g.fromFormattedString(to)
        (f, t)
      case any => throw new UnsupportedOperationException(s"Only between or datetime between filter supported : $any")
    }
  }

  def getMaxDate(f: Filter, g: Grain) : DateTime = {
    f match {
      case dtf: DateTimeBetweenFilter =>
        g.fromFormattedString(g.toFormattedString(dtf.toDateTime))
      case BetweenFilter(_, from, to) =>
        g.fromFormattedString(to)
      case InFilter(_, values, _, _) =>
        g.fromFormattedString(values.max)
      case EqualityFilter(_, from, _, _) =>
        g.fromFormattedString(from)
      case any => throw new UnsupportedOperationException(s"Only Between/In/Equality filter supported : $any")
    }
  }

  private[this] def processBetweenFilterForDate(fromDate: DateTime, toDate: DateTime, grain: Grain, columnAlias: String, column: Column, columnsByNameMap: Map[String, Column], timezone: DateTimeZone) : DimFilter = {
    val values: List[String] = {
      val dates = new mutable.HashSet[String]()
      var currentDate = fromDate
      while(currentDate.isBefore(toDate) || currentDate.isEqual(toDate)) {
        dates += grain.toFormattedString(currentDate)
        currentDate = grain.incrementOne(currentDate)
      }
      dates.toList
    }
    val selectorList : List[DimFilter] = column match {
      case DruidFuncDimCol(name, dt, cc, df, a, ann, foo) =>
        df match {
          case DRUID_TIME_FORMAT(fmt,zone) =>
            val exFn = new TimeFormatExtractionFn(fmt, zone, null, null, false)
            values.map {
              v => new SelectorDimFilter(DRUID_TIME_FORMAT.sourceDimColName, druidLiteralMapper.toLiteral(column, v, Option(grain)), exFn)
            }
          case DRUID_TIME_FORMAT_WITH_PERIOD_GRANULARITY(fmt, period, zone) =>
            val periodGranularity = new PeriodGranularity(new Period(period), null, zone)
            val exFn = new TimeFormatExtractionFn(fmt, zone, null, periodGranularity, false)
            values.map {
              v => new SelectorDimFilter(DRUID_TIME_FORMAT_WITH_PERIOD_GRANULARITY.sourceDimColName, druidLiteralMapper.toLiteral(column, v, Option(grain)), exFn)
            }
          case TIME_FORMAT_WITH_REQUEST_CONTEXT(fmt) =>
            val exFn = new TimeFormatExtractionFn(fmt, timezone, null, null, false)
            values.map {
              v => new SelectorDimFilter(columnAlias, druidLiteralMapper.toLiteral(column, v, Option(grain)), null)
            }
          case formatter@DATETIME_FORMATTER(fieldName, index, length) =>
            val exFn = new SubstringDimExtractionFn(index, length)
            values.map {
              v => new SelectorDimFilter(formatter.dimColName, druidLiteralMapper.toLiteral(column, v, Grain.getGrainByField(column.name)), exFn)
            }
          case any => throw new UnsupportedOperationException(s"Unhandled druid func $any")
        }
      case _ =>
        values.map {
            v => new SelectorDimFilter(columnAlias, druidLiteralMapper.toLiteral(column, v, Option(grain)), null)
          }

    }
    new OrDimFilter(selectorList.asJava)
  }

  private[this] def processDateTimeBetweenFilterForDate(fromDate: DateTime, toDate: DateTime, grain: Grain, columnAlias: String, column: Column, columnsByNameMap: Map[String, Column], timezone: DateTimeZone) : DimFilter = {
    val values: List[String] = {
      val dates = new mutable.HashSet[String]()
      var currentDate = fromDate
      while(currentDate.isBefore(toDate) || currentDate.isEqual(toDate)) {
        dates += grain.toFullFormattedString(currentDate)
        currentDate = grain.incrementOne(currentDate)
      }
      dates.map(druidLiteralMapper.toLiteral(column, _, Option(grain))).to[SortedSet].toList
    }
    val selectorList : List[DimFilter] = column match {
      case DruidFuncDimCol(name, dt, cc, df, a, ann, foo) =>
        df match {
          case DRUID_TIME_FORMAT(fmt,zone) =>
            val exFn = new TimeFormatExtractionFn(fmt, zone, null, null, false)
            values.map {
              v => new SelectorDimFilter(DRUID_TIME_FORMAT.sourceDimColName, v, exFn)
            }
          case DRUID_TIME_FORMAT_WITH_PERIOD_GRANULARITY(fmt, period, zone) =>
            val periodGranularity = new PeriodGranularity(new Period(period), null, zone)
            val exFn = new TimeFormatExtractionFn(fmt, zone, null, periodGranularity, false)
            values.map {
              v => new SelectorDimFilter(DRUID_TIME_FORMAT_WITH_PERIOD_GRANULARITY.sourceDimColName, v, exFn)
            }
          case TIME_FORMAT_WITH_REQUEST_CONTEXT(fmt) =>
            val exFn = new TimeFormatExtractionFn(fmt, timezone, null, null, false)
            values.map {
              v => new SelectorDimFilter(columnAlias, v, null)
            }
          case formatter@DATETIME_FORMATTER(fieldName, index, length) =>
            val exFn = new SubstringDimExtractionFn(index, length)
            values.map {
              v => new SelectorDimFilter(formatter.dimColName, v, exFn)
            }.toList
          case any => throw new UnsupportedOperationException(s"Unhandled druid func $any")
        }
      case _ =>
        values.map {
          v => new SelectorDimFilter(columnAlias, druidLiteralMapper.toLiteral(column, v, Option(grain)), null)
        }

    }
    new OrDimFilter(selectorList.asJava)
  }

  def isExpensiveDateDimFilter(model: RequestModel,
                               aliasToNameMapFull: Map[String, String],
                               columnsByNameMap: Map[String, Column]) : Boolean = {

    val name: String = aliasToNameMapFull(model.localTimeDayFilter.field)
    val column = columnsByNameMap(name)

    column match {
      case DruidFuncDimCol(_, _, _, df, _, _, _) =>
        df match {
          case TIME_FORMAT_WITH_REQUEST_CONTEXT(fmt) => true
          case _ => false
        }
      case _ => false
    }

  }

  def renderDateDimFilters(model: RequestModel
                           , aliasToNameMapFull: Map[String, String]
                           , columnsByNameMap: Map[String, Column]
                           , grain: Grain
                           , forOuterQuery: Boolean = false) : Seq[DimFilter] = {
    val dimFilters = new mutable.ArrayBuffer[DimFilter]
    val timezone = DateTimeZone.forID(model.additionalParameters.getOrElse(Parameter.TimeZone, TimeZoneValue.apply(DateTimeZone.UTC.getID))
      .asInstanceOf[TimeZoneValue].value)
    val name: String = aliasToNameMapFull(model.localTimeDayFilter.field)
    val column = columnsByNameMap(name)

    model.localTimeDayFilter.operator match {
      case InFilterOperation | EqualityFilterOperation =>
        dimFilters += renderFilterDim(model.localTimeDayFilter, aliasToNameMapFull, columnsByNameMap, Option(DailyGrain))
        model.localTimeHourFilter.foreach {
          filter =>
            if(aliasToNameMapFull.contains(HourlyGrain.HOUR_FILTER_FIELD)) {
              dimFilters += renderFilterDim(filter, aliasToNameMapFull, columnsByNameMap, Option(HourlyGrain))
            }
        }
        model.localTimeMinuteFilter.foreach {
          filter =>
            if(aliasToNameMapFull.contains(MinuteGrain.MINUTE_FILTER_FIELD)) {
              dimFilters += renderFilterDim(filter, aliasToNameMapFull, columnsByNameMap, Option(HourlyGrain))
            }
        }
      case DateTimeBetweenFilterOperation if column.dataType.isInstanceOf[TimestampType] =>
        val columnAlias = if(forOuterQuery) model.localTimeDayFilter.field else column.alias.getOrElse(name)
        val (f, t) = extractFromAndToDate(model.localTimeDayFilter, grain)
        dimFilters += processDateTimeBetweenFilterForDate(f, t, grain, columnAlias, column, columnsByNameMap, timezone)
      case BetweenFilterOperation | DateTimeBetweenFilterOperation =>
        val (dayFrom, dayTo) = {
          val columnAlias = if(forOuterQuery) model.localTimeDayFilter.field else column.alias.getOrElse(name)
          val (f, t) = extractFromAndToDate(model.localTimeDayFilter, DailyGrain)
          dimFilters += processBetweenFilterForDate(f, t, DailyGrain, columnAlias, column, columnsByNameMap, timezone)
          (f, t)
        }

        if(aliasToNameMapFull.contains(HourlyGrain.HOUR_FILTER_FIELD)) {
          val fromAndToWithHour = model.localTimeHourFilter.map {
            filter =>
              val name: String = aliasToNameMapFull(filter.field)
              val column = columnsByNameMap(name)
              val columnAlias = column.alias.getOrElse(name)
              val (f, t) = extractFromAndToDate(filter, HourlyGrain)
              val (fWithHour, tWithHour) = (dayFrom.withHourOfDay(f.getHourOfDay), dayTo.withHourOfDay(t.getHourOfDay))
              dimFilters += processBetweenFilterForDate(fWithHour, tWithHour, HourlyGrain, columnAlias, column, columnsByNameMap, timezone)
              (fWithHour, tWithHour)
          }

          if (aliasToNameMapFull.contains(MinuteGrain.MINUTE_FILTER_FIELD)) {
            for {
              (dayWithHourFrom, dayWithHourTo) <- fromAndToWithHour
              filter <- model.localTimeMinuteFilter
            } {
              val name: String = aliasToNameMapFull(filter.field)
              val column = columnsByNameMap(name)
              val columnAlias = column.alias.getOrElse(name)
              val (f, t) = extractFromAndToDate(filter, MinuteGrain)
              val (fWithHourAndMinute, tWithHourAndMinute) = (dayWithHourFrom.withMinuteOfHour(f.getMinuteOfHour), dayWithHourTo.withMinuteOfHour(t.getMinuteOfHour))
              dimFilters += processBetweenFilterForDate(fWithHourAndMinute, tWithHourAndMinute, MinuteGrain, columnAlias, column, columnsByNameMap, timezone)
            }
          }
        }

      case any => throw new UnsupportedOperationException(s"Unsupported date operation : ${model.localTimeDayFilter}")

    }
    dimFilters
  }

  def renderOrDimFilters(filters: List[Filter],
                         aliasToNameMapFull: Map[String, String],
                         columnsByNameMap: Map[String, Column],
                         grainOption: Option[Grain],
                         forOuterQuery: Boolean = false): DimFilter = {
    val selectorList : List[DimFilter] = filters.map {
      filter => {
        renderFilterDim(filter, aliasToNameMapFull, columnsByNameMap, grainOption, forOuterQuery)
      }
    }
    new OrDimFilter(selectorList.asJava)
  }

  def renderOrFactFilters(filters: List[Filter],
                          aliasToNameMapFull: Map[String, String],
                          columnsByNameMap: Map[String, Column]) : HavingSpec = {
    val list: List[HavingSpec] = filters.map {
      filter => {
        renderFilterFact(filter, aliasToNameMapFull, columnsByNameMap)
      }
    }
    new OrHavingSpec(list.asJava)
  }

  def renderFilterDim(filter: Filter,
                      aliasToNameMapFull: Map[String, String],
                      columnsByNameMap: Map[String, Column],
                      grainOption: Option[Grain],
                      forOuterQuery: Boolean = false) : DimFilter = {

    val name = if (aliasToNameMapFull.contains(filter.field)) aliasToNameMapFull(filter.field) else filter.field
    val column = columnsByNameMap(name)
    val columnAlias = if (forOuterQuery) filter.field else column.alias.getOrElse(name)
    val grain = grainOption.getOrElse(DailyGrain)

    filter match {
      case PushDownFilter(f) =>
        renderFilterDim(f, aliasToNameMapFull, columnsByNameMap, grainOption)
      case f @ BetweenFilter(field, from, to) =>
        column.dataType match {
          case DateType(_) | TimestampType(_) =>
            throw new UnsupportedOperationException(s"Date or Timestamp rendering not supported by this method, use renderDateDimFilters")
          case it: IntType =>
            val fieldString = aliasToNameMapFull(field)
            val filter = new BoundDimFilter(fieldString, from, to, false, false, false, null, new NumericComparator)
            filter
          case st : StrType =>
            val fieldString = aliasToNameMapFull(field)
            val filter = new BoundDimFilter(fieldString, from, to, false, false, false, null, new LexicographicComparator)
            filter
          case _ =>
            //currently
            throw new UnsupportedOperationException(s"Between filter not supported on Druid dimension fields : $f")
        }
      case dtf: DateTimeBetweenFilter =>
        throw new UnsupportedOperationException(s"DateTimeBetween filter not supported on Druid dimension fields : ${dtf.field}")
      case f @ InFilter(alias, values, _, _) =>
        val selectorList : List[DimFilter] = values.map {
          v => getDruidFilter(grainOption, column, columnAlias, v, columnsByNameMap)
        }
        new OrDimFilter(selectorList.asJava)
      case f @ NotInFilter(alias, values, _, _) =>
        val selectorList : List[DimFilter] = values.map {
          v => getDruidFilter(grainOption, column, columnAlias, v, columnsByNameMap)
        }
        new NotDimFilter(new OrDimFilter(selectorList.asJava))
      case f @ EqualityFilter(alias, value, _, _) => {
        getDruidFilter(grainOption, column, columnAlias, value, columnsByNameMap)
      }
      case f @ FieldEqualityFilter(field, compareTo, _, _) => {
        val firstAlias = aliasToNameMapFull(field)
        val otherAlias = aliasToNameMapFull(compareTo)
        val defaultDimSpecs: List[DimensionSpec] = (List(firstAlias, otherAlias)).map(
          (fieldToCompare: String) => {
            val col: Column = columnsByNameMap(fieldToCompare)
            val alias: String = col.alias.getOrElse(fieldToCompare)
            new DefaultDimensionSpec(alias, alias)
          })
        new ColumnComparisonDimFilter(defaultDimSpecs.asJava)
      }
      case f @ NotEqualToFilter(alias, value, _, _) =>
        val selector = getDruidFilter(grainOption, column, columnAlias, value, columnsByNameMap)
        new NotDimFilter(selector)
      case f @ LikeFilter(alias, value, _, _) =>
        val spec = new InsensitiveContainsSearchQuerySpec(druidLiteralMapper.toLiteral(column, value, grainOption))
        new SearchQueryDimFilter(columnAlias, spec, null)
      case f @ NotLikeFilter(alias, value, _, _) =>
        val spec = new InsensitiveContainsSearchQuerySpec(druidLiteralMapper.toLiteral(column, value, grainOption))
        new NotDimFilter(new SearchQueryDimFilter(columnAlias, spec, null))
      case f @ JavaScriptFilter(alias, func, _, _) => {
        new JavaScriptDimFilter(alias, func, null, JavaScriptConfig.getEnabledInstance)
      }
      case f @ IsNullFilter(alias, _, _) =>
        getDruidFilter(grainOption, column, columnAlias, null, columnsByNameMap)
      case f @ IsNotNullFilter(alias, _, _) =>
        val selector = getDruidFilter(grainOption, column, columnAlias, null, columnsByNameMap)
        new NotDimFilter(selector)
      case f =>
        throw new UnsupportedOperationException(s"Unhandled filter operation $f")
    }
  }

  def getDruidFilter(grainOption: Option[Grain], column: Column, columnAlias: String, value: String, columnsByNameMap: Map[String, Column]): DimFilter = {

    def getSourceDimColFormat(sourceDimCol: Column) : String = {
      val sourceDimColFormat = sourceDimCol.dataType match {
        case DateType(sourceFormat) =>
          sourceFormat.getOrElse(grainOption.get.formatString)
        case StrType(_, _, _) =>
          grainOption.get.formatString
        case any =>
          throw new UnsupportedOperationException(s"Found unhandled dataType : $any")
      }
      sourceDimColFormat
    }

    column match {
      case DruidFuncDimCol(name, dt, cc, df, a, ann, foo) =>
        df match {
          case DRUID_TIME_FORMAT(fmt,zone) =>
            val exFn = new TimeFormatExtractionFn(fmt, zone, null, null, false)
            new SelectorDimFilter(DRUID_TIME_FORMAT.sourceDimColName, druidLiteralMapper.toLiteral(column, value, grainOption), exFn)
          case DRUID_TIME_FORMAT_WITH_PERIOD_GRANULARITY(fmt, period, zone) =>
            val periodGranularity = new PeriodGranularity(new Period(period), null, zone)
            val exFn = new TimeFormatExtractionFn(fmt, zone, null, periodGranularity, false)
            new SelectorDimFilter(DRUID_TIME_FORMAT_WITH_PERIOD_GRANULARITY.sourceDimColName, druidLiteralMapper.toLiteral(column, value, grainOption), exFn)
          case formatter@DATETIME_FORMATTER(fieldName, index, length) =>
            val exFn = new SubstringDimExtractionFn(index, length)
            new SelectorDimFilter(formatter.dimColName, druidLiteralMapper.toLiteral(column, value, Grain.getGrainByField(column.name)), exFn)
          case regex@REGEX(fieldName, expr, index, replaceMissingValue, replaceMissingValueWith) =>
            val exFn = new RegexDimExtractionFn(expr, index, replaceMissingValue, replaceMissingValueWith)
            new SelectorDimFilter(regex.dimColName, druidLiteralMapper.toLiteral(column, value, Grain.getGrainByField(column.name)), exFn)
          case decoder@DECODE_DIM(fieldName, args @ _*) =>
            val sourceDimCol = columnsByNameMap(decoder.dimColName)
            val sourceDimMappedValues = if (sourceDimCol.dataType.hasStaticMapping) {
              sourceDimCol.dataType.reverseStaticMapping(value).toList
            } else {
              Set(value).toList
            }
            val colReverseMapping = decoder.map.groupBy(_._2).mapValues(_.keys)
            val decodeLists = sourceDimMappedValues.map {
              v => colReverseMapping.contains(v) match {
                case true =>
                  val curValueList = if (sourceDimCol.dataType.hasStaticMapping || sourceDimCol.dataType == dt) List(v) else Nil
                  colReverseMapping(v).toList ++ curValueList
                case false =>
                  List(v)
              }
            }
            val mappedValues = decodeLists.flatten.distinct
            mappedValues.length match {
              case 1 =>
                new SelectorDimFilter(decoder.dimColName, druidLiteralMapper.toLiteral(sourceDimCol, mappedValues(0), grainOption), null)
              case _ =>
                val selectorList: List[DimFilter] = mappedValues.map {
                  v => new SelectorDimFilter(decoder.dimColName, druidLiteralMapper.toLiteral(sourceDimCol, v, grainOption), null)
                }
                new OrDimFilter(selectorList.asJava)
            }
          case _ =>
            new SelectorDimFilter(columnAlias, druidLiteralMapper.toLiteral(column, value, grainOption), null)
        }
      case DruidPostResultFuncDimCol(name, dt, cc, prf, _, _, _) => {
        prf match {
          case sotw@START_OF_THE_WEEK(exp) => {
            val sourceDimCol = columnsByNameMap(sotw.colName)
            val sourceDimColFormat: String = getSourceDimColFormat(sourceDimCol)

            val yearAndWeekFormattedValue = sotw.toFormattedString(value)

            val exFn = new TimeDimExtractionFn(sourceDimColFormat, sotw.yearandWeekOfTheYearFormatForDruid, false)
            new SelectorDimFilter(sourceDimCol.alias.getOrElse(sourceDimCol.name), yearAndWeekFormattedValue, exFn)
          }
          case sotm@START_OF_THE_MONTH(exp) => {
            val sourceDimCol = columnsByNameMap(sotm.colName)
            val sourceDimColFormat: String = getSourceDimColFormat(sourceDimCol)
            val exFn = new TimeDimExtractionFn(sourceDimColFormat, sotm.startOfTheMonthFormat, false)
            new SelectorDimFilter(sourceDimCol.alias.getOrElse(sourceDimCol.name), value, exFn)
          }
          case any => throw new UnsupportedOperationException(s"Unhandled druid post result func $any")
        }
      }
      case _ =>
        new SelectorDimFilter(columnAlias, druidLiteralMapper.toLiteral(column, value, grainOption), null)
    }
  }

  def renderFilterFact(filter: Filter,
                       aliasToNameMapFull: Map[String, String],
                       columnsByNameMap: Map[String, Column]) : HavingSpec = {

    import collection.JavaConverters._

    filter match {
      case PushDownFilter(f) =>
        renderFilterFact(f, aliasToNameMapFull, columnsByNameMap)
      case f @ BetweenFilter(alias, from, to) =>
        val name = aliasToNameMapFull(alias)
        val column = columnsByNameMap(name)
        // > from and < to or equalTo from or equalTo to
        val greatThanFrom = new GreaterThanHavingSpec(alias, druidLiteralMapper.toNumber(column, from))
        val lessThanTo = new LessThanHavingSpec(alias, druidLiteralMapper.toNumber(column, to))
        val equalToFrom = new EqualToHavingSpec(alias, druidLiteralMapper.toNumber(column, from))
        val equalToTo = new EqualToHavingSpec(alias, druidLiteralMapper.toNumber(column, to))
        val greaterThanFromAndLessThanTo = new AndHavingSpec(Lists.newArrayList(greatThanFrom, lessThanTo))
        new OrHavingSpec(Lists.newArrayList(greaterThanFromAndLessThanTo, equalToFrom, equalToTo))
      case f @ InFilter(alias, values, _, _) =>
        val name = aliasToNameMapFull(alias)
        val column = columnsByNameMap(name)
        val equalToList : List[HavingSpec] = values.map {
          v => new EqualToHavingSpec(alias, druidLiteralMapper.toNumber(column, v))
        }
        new OrHavingSpec(equalToList.asJava)
      case f @ NotInFilter(alias, values, _, _) =>
        val name = aliasToNameMapFull(alias)
        val column = columnsByNameMap(name)
        val equalToList : List[HavingSpec] = values.map {
          v => new EqualToHavingSpec(alias, druidLiteralMapper.toNumber(column, v))
        }
        new NotHavingSpec(new OrHavingSpec(equalToList.asJava))
      case f @ EqualityFilter(alias, value, _, _) =>
        val name = aliasToNameMapFull(alias)
        val column = columnsByNameMap(name)
        new EqualToHavingSpec(alias, druidLiteralMapper.toNumber(column, value))
      case f @ GreaterThanFilter(alias, value, _, _) =>
        val name = aliasToNameMapFull(alias)
        val column = columnsByNameMap(name)
        new GreaterThanHavingSpec(alias, druidLiteralMapper.toNumber(column, value))
      case f @ LessThanFilter(alias, value, _, _) =>
        val name = aliasToNameMapFull(alias)
        val column = columnsByNameMap(name)
        new LessThanHavingSpec(alias, druidLiteralMapper.toNumber(column, value))
      case f @ LikeFilter(_, _, _, _) =>
        throw new UnsupportedOperationException(s"Like filter not supported on Druid fact fields : $f")
      case f @ FieldEqualityFilter(field, compareTo, _, _) =>
        throw new UnsupportedOperationException(s"Column Comparison is not supported on Druid fact fields : $f")
      case f =>
        throw new UnsupportedOperationException(s"Unhandled filter operation $f")
    }
  }
}

object FilterSql {

  def renderFilter(filter: Filter,
                   aliasToNameMapFull: Map[String, String],
                   nameToAliasAndRenderedSqlMap: Map[String, (String, String)],
                   columnsByNameMap: Map[String, Column],
                   engine: Engine,
                   literalMapper: SqlLiteralMapper,
                   grainOption: Option[Grain] = None): SqlResult = {

    val aliasToRenderedSqlMap: mutable.HashMap[String, (String, String)] = new mutable.HashMap[String, (String, String)]()
    val name = aliasToNameMapFull(filter.field)
    val column = columnsByNameMap(name)
    val nameOrAlias = column.alias.getOrElse(name)
    val expandedExpression = if (nameToAliasAndRenderedSqlMap.contains(filter.field)) Option(nameToAliasAndRenderedSqlMap(filter.field)._2) else None
    val exp = expandedExpression match {
      case None =>
        column match {
          case column if column.isInstanceOf[DerivedColumn] =>
            val derCol = column.asInstanceOf[DerivedColumn]
            derCol.derivedExpression.render(name).toString
          case _ => nameOrAlias
        }
      case Some(e) => e
    }

    aliasToRenderedSqlMap(filter.field) = (name, exp)
    filter match {
      case PushDownFilter(f) =>
        renderFilter(f, aliasToNameMapFull, nameToAliasAndRenderedSqlMap, columnsByNameMap, engine, literalMapper, None)
      case FieldEqualityFilter(f,g, _, _) =>
        val otherColumnName = aliasToNameMapFull(g)
        val otherColumn = columnsByNameMap(otherColumnName)
        val otherNameOrAlias = otherColumn.alias.getOrElse(otherColumnName)
        val expandedOtherExpression = if (nameToAliasAndRenderedSqlMap.contains(g)) Option(nameToAliasAndRenderedSqlMap(g)._2) else None
        val exp2 = expandedOtherExpression match {
          case None =>
            column match {
              case column if otherColumn.isInstanceOf[DerivedColumn] =>
                val derCol = column.asInstanceOf[DerivedColumn]
                derCol.derivedExpression.render(otherColumnName).toString
              case _ => otherNameOrAlias
            }
          case Some(e) => e
        }
        aliasToRenderedSqlMap(g) = (otherColumnName, exp2)
        renderFilterWithAlias(filter, aliasToRenderedSqlMap.toMap, column, engine, literalMapper, grainOption = grainOption )
      case _ =>
        renderFilterWithAlias(filter, aliasToRenderedSqlMap.toMap, column, engine, literalMapper, grainOption = grainOption )
    }
  }

  def renderOuterFilter(filter: Filter,
                   columnsByNameMap: Map[String, Column],
                   engine: Engine,
                   literalMapper: SqlLiteralMapper,
                   grainOption: Option[Grain] = None): SqlResult = {


    val outerColName = s""" "${filter.field}"  """
    val column = columnsByNameMap({filter.field})
    renderFilterWithAlias(filter, Map(filter.field -> (filter.field, outerColName)), column, engine, literalMapper, grainOption = grainOption)
  }

  def renderFilterWithAlias(filter: Filter,
                   aliasToRenderedSqlMap: Map[String, (String, String)],
                   column: Column,
                   engine: Engine,
                   literalMapper: SqlLiteralMapper,
                   grainOption: Option[Grain] = None): SqlResult = {

    filter match {
      case f@BetweenFilter(alias, from, to) =>
        SqlBetweenFilterRenderer.render(
          aliasToRenderedSqlMap,
          f,
          literalMapper,
          column,
          engine,
          grainOption)
      case f: DateTimeBetweenFilter =>
        SqlDateTimeBetweenFilterRenderer.render(
          aliasToRenderedSqlMap,
          f,
          literalMapper,
          column,
          engine,
          grainOption)
      case f@InFilter(alias, values, _, _) =>
        SqlInFilterRenderer.render(
          aliasToRenderedSqlMap,
          f,
          literalMapper,
          column,
          engine,
          grainOption
        )
      case f@NotInFilter(alias, values, _, _) =>
        SqlNotInFilterRenderer.render(
          aliasToRenderedSqlMap,
          f,
          literalMapper,
          column,
          engine,
          grainOption
        )
      case f@EqualityFilter(alias, value, _, _) =>
        SqlEqualityFilterRenderer.render(
          aliasToRenderedSqlMap,
          f,
          literalMapper,
          column,
          engine,
          grainOption
        )
      case f@FieldEqualityFilter(alias, compareTo, _, _) =>
        SqlFieldEqualityFilterRenderer.render(
          aliasToRenderedSqlMap,
          f,
          literalMapper,
          column,
          engine,
          grainOption
        )
      case f@GreaterThanFilter(alias, value, _, _) =>
        SqlGreaterThanFilterRenderer.render(
          aliasToRenderedSqlMap,
          f,
          literalMapper,
          column,
          engine,
          grainOption
        )
      case f@LessThanFilter(alias, value, _, _) =>
        SqlLessThanFilterRenderer.render(
          aliasToRenderedSqlMap,
          f,
          literalMapper,
          column,
          engine,
          grainOption
        )
      case f@NotEqualToFilter(alias, value, _, _) =>
        SqlNotEqualToFilterRenderer.render(
          aliasToRenderedSqlMap,
          f,
          literalMapper,
          column,
          engine,
          grainOption
        )
      case f@LikeFilter(alias, value, _, _) =>
        SqlLikeFilterRenderer.render(
          aliasToRenderedSqlMap,
          f,
          literalMapper,
          column,
          engine,
          grainOption
        )
      case f@NotLikeFilter(alias, value, _, _) =>
        SqlNotLikeFilterRenderer.render(
          aliasToRenderedSqlMap,
          f,
          literalMapper,
          column,
          engine,
          grainOption
        )
      case f@IsNullFilter(alias, _, _) =>
        SqlIsNullFilterRenderer.render(
          aliasToRenderedSqlMap,
          f,
          literalMapper,
          column,
          engine,
          grainOption
        )
      case f@IsNotNullFilter(alias, _, _) =>
        SqlIsNotNullFilterRenderer.render(
          aliasToRenderedSqlMap,
          f,
          literalMapper,
          column,
          engine,
          grainOption
        )
      case f@OrFilter(filters) => {
        val sqlResults: mutable.ListBuffer[SqlResult] = mutable.ListBuffer.empty[SqlResult]
        for(filterItem <- filters) {
          val filterColumn = column.columnContext.getColumnByName(aliasToRenderedSqlMap(filterItem.field)._1).get
          sqlResults.append(renderFilterWithAlias(filterItem, aliasToRenderedSqlMap, filterColumn, engine, literalMapper, grainOption))
        }
        val newSqlResultString: String = sqlResults.map(_.filter).mkString("(",") OR (",")")
        DefaultResult(newSqlResultString)
      }
      case f@AndFilter(filters) => {
        val sqlResults: mutable.ListBuffer[SqlResult] = mutable.ListBuffer.empty[SqlResult]
        for(filterItem <- filters) {
          val filterColumn = column.columnContext.getColumnByName(aliasToRenderedSqlMap(filterItem.field)._1).get
          sqlResults.append(renderFilterWithAlias(filterItem, aliasToRenderedSqlMap, filterColumn, engine, literalMapper, grainOption))
        }
        val newSqlResultString: String = sqlResults.map(_.filter).mkString("(",") AND (",")")
        DefaultResult(newSqlResultString)
      }
      case f =>
        throw new UnsupportedOperationException(s"Unhandled filter operation $f")
    }
  }

}

object Filter extends Logging {
  import JsonUtils._

  import _root_.scalaz.Validation
  import Validation.FlatMap._

  def compare(a: Filter, b:Filter): Int = Filter.baseEquality.compare(a,b)
  val baseEquality: BaseEquality[Filter] = BaseEquality.from[Filter]{
    (a,b) =>
      val op = a.operator.toString.compare(b.operator.toString)
      if(op == 0) {
        val f = a.field.compare(b.field)
        if(f == 0)
          a.asValues.compare(b.asValues)
        else f
      }
      else op
  }

  val baseFilterOrdering: Ordering[Filter] = {
    new Ordering[Filter] {
      override def compare(x: Filter, y: Filter): Int = Filter.compare(x,y)
    }
  }

  //Source for ordering on Set insertion.
  implicit def orderingByAlias[A <: Filter]: Ordering[A] = {
    Ordering.fromLessThan {
      (a, b) =>
        if(a.isPushDown == b.isPushDown) {
          compare(a, b) < 0
        } else {
          a.isPushDown > b.isPushDown
        }
    }
  }

  implicit def filterJSONW : JSONW[Filter] = new JSONW[Filter] {
    override def write(filter: Filter): JValue = filter match {
      case OuterFilter(filters) =>
        makeObj(
           ("operator" -> toJSON(filter.operator.toString))
            :: ("outerFilters" -> { toJSON(filters)
           })
            :: Nil)
      case BetweenFilter(field, from, to) =>
        makeObj(
          ("field" -> toJSON(field))
          :: ("operator" -> toJSON(filter.operator.toString))
          :: ("from" -> toJSON(from))
          :: ("to" -> toJSON(to))
          :: Nil)
      case DateTimeBetweenFilter(field, from, to, fmt) =>
        makeObj(
          ("field" -> toJSON(field))
            :: ("operator" -> toJSON(filter.operator.toString))
            :: ("from" -> toJSON(from))
            :: ("to" -> toJSON(to))
            :: ("format" -> toJSON(fmt))
            :: Nil)
      case InFilter(field, values, _, _) =>
        makeObj(
          ("field" -> toJSON(field))
          :: ("operator" -> toJSON(filter.operator.toString))
          :: ("values" -> toJSON(values))
          :: Nil)
      case NotInFilter(field, values, _, _) =>
        makeObj(
          ("field" -> toJSON(field))
            :: ("operator" -> toJSON(filter.operator.toString))
            :: ("values" -> toJSON(values))
            :: Nil)
      case EqualityFilter(field, value, _, _) =>
        makeObj(
          ("field" -> toJSON(field))
          :: ("operator" -> toJSON(filter.operator.toString))
          :: ("value" -> toJSON(value))
          :: Nil)
      case FieldEqualityFilter(field, compareTo, _, _) =>
        makeObj(
          ("field" -> toJSON(field))
            :: ("operator" -> toJSON(filter.operator.toString))
            :: ("compareTo" -> toJSON(compareTo))
            :: Nil)
      case GreaterThanFilter(field, value, _, _) =>
        makeObj(
          ("field" -> toJSON(field))
            :: ("operator" -> toJSON(filter.operator.toString))
            :: ("value" -> toJSON(value))
            :: Nil)
      case LessThanFilter(field, value, _, _) =>
        makeObj(
          ("field" -> toJSON(field))
            :: ("operator" -> toJSON(filter.operator.toString))
            :: ("value" -> toJSON(value))
            :: Nil)
      case LikeFilter(field, value, _, _) =>
        makeObj(
          ("field" -> toJSON(field))
            :: ("operator" -> toJSON(filter.operator.toString))
            :: ("value" -> toJSON(value))
            :: Nil)
      case NotLikeFilter(field, value, _, _) =>
        makeObj(
          ("field" -> toJSON(field))
            :: ("operator" -> toJSON(filter.operator.toString))
            :: ("value" -> toJSON(value))
            :: Nil)
      case NotEqualToFilter(field, value, _, _) =>
        makeObj(
          ("field" -> toJSON(field))
            :: ("operator" -> toJSON(filter.operator.toString))
            :: ("value" -> toJSON(value))
            :: Nil)
      case IsNullFilter(field, _, _) =>
        makeObj(
          ("field" -> toJSON(field))
            :: ("operator" -> toJSON(filter.operator.toString))
            :: Nil)
      case IsNotNullFilter(field, _, _) =>
        makeObj(
          ("field" -> toJSON(field))
            :: ("operator" -> toJSON(filter.operator.toString))
            :: Nil)
      case OrFilter(filters) =>
        makeObj(
          ("operator" -> toJSON(filter.operator.toString))
           :: ("filterExpressions" -> { toJSON(filters) })
            :: Nil)
      case AndFilter(filters) =>
        makeObj(
          ("operator" -> toJSON(filter.operator.toString))
           :: ("filterExpressions" -> { toJSON(filters) })
            :: Nil)
      case PushDownFilter(wrappedFilter) =>
        write(wrappedFilter)
      case unsupported =>
        warn(s"Unsupported filter for json serialize JSONW[Filter] : $unsupported")
        JNothing

    }
  }

  def nonEmptyString(string: String, fieldName : String, jsonFieldName: String) : JsonScalaz.Result[Boolean] = {
    import _root_.scalaz.syntax.validation._
    if (string.isEmpty) {
      Fail.apply(jsonFieldName, s"$fieldName filter cannot have empty string")
    } else {
      true.successNel
    }
  }

  def nonEmptyList(string: List[Any], fieldName : String, jsonFieldName: String) : JsonScalaz.Result[Boolean] = {
    import _root_.scalaz.syntax.validation._
    if (string.isEmpty) {
      Fail.apply(jsonFieldName, s"$fieldName filter cannot have empty list")
    } else {
      true.successNel
    }
  }

  def outerFilter(filter: Filter) : JsonScalaz.Result[Boolean] = {
    import _root_.scalaz.syntax.validation._
    if (filter.isPushDown) {
      Fail.apply("Outer", "filter expression cannot have push down filter")
    } else {
      true.successNel
    }
  }

  def orFilter(orFilter: OrFilter) : JsonScalaz.Result[Boolean] = {
    import _root_.scalaz.syntax.validation._
    if(orFilter.filters.isEmpty) {
      Fail.apply(orFilter.field, s"filter cannot have empty list")
    } else {
      true.successNel
    }
  }

  def andFilter(andFilter: AndFilter) : JsonScalaz.Result[Boolean] = {
    import _root_.scalaz.syntax.validation._
    if(andFilter.filters.isEmpty) {
      Fail.apply(andFilter.field, s"filter cannot have empty list")
    } else {
      true.successNel
    }
  }

  implicit def filterSetJSONR: JSONR[List[Filter]] = new JSONR[List[Filter]] {
    override def read(json: JValue): JsonScalaz.Result[List[Filter]] = {
      import _root_.scalaz.syntax.validation._
      implicit val formats = DefaultFormats

      val readValues = Serialization.read[List[JValue]](compact(render(json)))
      val readAndVerifyFilters: Set[JsonScalaz.Result[Filter]] = readValues.map(value=>filterJSONR.read(value)).toSet
      val returnSetCondition: JsonScalaz.Result[Boolean] =
        if (readAndVerifyFilters.forall(result=>result.isSuccess)) true.successNel else Fail.apply(readAndVerifyFilters.toString(),  s"Filter set is not correct.")

      returnSetCondition.map(_ => readAndVerifyFilters.map(result => result.getOrElse(null)).toList)
    }
  }


  implicit def filterJSONR: JSONR[Filter] = new JSONR[Filter] {
    override def read(json: JValue): JsonScalaz.Result[Filter] = {
      val operatorResult = field[String]("operator")(json)
      import _root_.scalaz.Success

      operatorResult.flatMap { operator =>
          operator.toLowerCase match {
            case "outer" =>
              val fil = OuterFilter.applyJSON(fieldExtended[List[Filter]]("outerFilters"))(json)
              fil.flatMap {
                f =>
                  outerFilter(f).map( _ => f)
              }
            case "or" =>
              val fil = OrFilter.applyJSON(fieldExtended[List[Filter]]("filterExpressions"))(json)
              fil.flatMap {
                f =>
                  orFilter(f).map( _ => f)
              }
            case "and" =>
              val fil = AndFilter.applyJSON(fieldExtended[List[Filter]]("filterExpressions"))(json)
              fil.flatMap {
                f =>
                  andFilter(f).map( _ => f)
              }
            case "between" =>
              val filter = BetweenFilter.applyJSON(field[String]("field"), stringField("from"), stringField("to"))(json)
              filter.flatMap {
                f =>
                  (nonEmptyString(f.from, f.field, "from") |@| nonEmptyString(f.to, f.field, "to"))((a,b) => f)
              }
            case "datetimebetween" =>
              val filter = (stringField("field")(json) |@| stringField("from")(json) |@| stringField("to")(json) |@| stringField("format")(json))((a,b,c,d) => {
                DateTimeBetweenFilter(a, b, c, d)
              })
              filter.flatMap {
                f =>
                  (nonEmptyString(f.from, f.field, "from") |@| nonEmptyString(f.to, f.field, "to") |@| nonEmptyString(f.format, f.field, "format"))((a,b,c) => f)
              }
            case "in" =>
              val filter = InFilter.applyJSON(field[String]("field"), stringListField("values"), booleanFalse, booleanFalse)(json)
              filter.flatMap {
                f =>
                    import _root_.scalaz.Scalaz._
                    val listCheckResult: JsonScalaz.Result[List[Boolean]] =
                      f.values.map(nonEmptyString(_, f.field, "values")).sequence[JsonScalaz.Result, Boolean]
                    (nonEmptyList(f.values, f.field, "values") |@| listCheckResult)((a, b) => f)
              }
            case "not in" =>
              val filter = NotInFilter.applyJSON(field[String]("field"), stringListField("values"), booleanFalse, booleanFalse)(json)
              filter.flatMap {
                f =>
                  import _root_.scalaz.Scalaz._
                  val listCheckResult : JsonScalaz.Result[List[Boolean]] =
                    f.values.map(nonEmptyString(_, f.field, "values")).sequence[JsonScalaz.Result, Boolean]
                  (nonEmptyList(f.values, f.field, "values") |@| listCheckResult)((a,b) => f)

              }
            case "=" | "equals" | "equal" =>
              val filter = EqualityFilter.applyJSON(field[String]("field"), stringField("value"), booleanFalse, booleanFalse)(json)
              filter.flatMap {
                f => nonEmptyString(f.value, f.field, "value").map(_ => f)
              }
            case "==" =>
              val filter = FieldEqualityFilter.applyJSON(field[String]("field"), stringField("compareTo"), booleanFalse, booleanFalse)(json)
              filter.flatMap {
                f => nonEmptyString(f.compareTo, f.field, "compareTo").map(_ => f)
              }
            case ">" =>
              val filter = GreaterThanFilter.applyJSON(field[String]("field"), stringField("value"), booleanFalse, booleanFalse)(json)
              filter.flatMap {
                f => nonEmptyString(f.value, f.field, "value").map(_ => f)
              }
            case "<" =>
              val filter = LessThanFilter.applyJSON(field[String]("field"), stringField("value"), booleanFalse, booleanFalse)(json)
              filter.flatMap {
                f => nonEmptyString(f.value, f.field, "value").map(_ => f)
              }
            case "like" =>
              val filter = LikeFilter.applyJSON(field[String]("field"), stringField("value"), booleanFalse, booleanFalse)(json)
              filter.flatMap {
                f => nonEmptyString(f.value, f.field, "value").map(_ => f)
              }
            case "not like" =>
              val filter = NotLikeFilter.applyJSON(field[String]("field"), stringField("value"), booleanFalse, booleanFalse)(json)
              filter.flatMap {
                f => nonEmptyString(f.value, f.field, "value").map(_ => f)
              }
            case "<>" | "not equal to" =>
              val filter = NotEqualToFilter.applyJSON(field[String]("field"), stringField("value"), booleanFalse, booleanFalse)(json)
              filter.flatMap {
                f => nonEmptyString(f.value, f.field, "value").map(_ => f)
              }
            case "isnull" | "is null" =>
              field[String]("field")(json).map(field => IsNullFilter(field))
            case "isnotnull" | "is not null" =>
              field[String]("field")(json).map(field => IsNotNullFilter(field))
            case unknown =>
              Fail.apply("operator", s"unsupported operator : $unknown")
          }
      }
    }
  }

  /**
    * For returning all filter fields relevant to the current filter type.
    * Used for pre-validated filters, such as those coming
    * from the PublicFact (forced filters).
    * @param filter - Filter to return data from.
    * @return - Set dependent upon input filter type only.
    */
  def returnFieldSetWithoutValidation(filter: Filter) : Set[String] = {
    filter match {
      case _: OuterFilter => Set.empty
      case fieldEqualityFilter: MultiFieldForcedFilter => Set(fieldEqualityFilter.field, fieldEqualityFilter.compareTo)
      case _: OrFilter => Set.empty
      case _: AndFilter => Set.empty
      case betweenFilter: BetweenFilter => Set(betweenFilter.field)
      case dateTimeBetweenFilter: DateTimeBetweenFilter => Set(dateTimeBetweenFilter.field)
      case equalityFilter: EqualityFilter => Set(equalityFilter.field)
      case inFilter: InFilter => Set(inFilter.field)
      case notInFilter: NotInFilter => Set(notInFilter.field)
      case notEqualToFilter: NotEqualToFilter => Set(notEqualToFilter.field)
      case greaterThanFilter: GreaterThanFilter => Set(greaterThanFilter.field)
      case lessThanFilter: LessThanFilter => Set(lessThanFilter.field)
      case isNotNullFilter: IsNotNullFilter => Set(isNotNullFilter.field)
      case likeFilter: LikeFilter => Set(likeFilter.field)
      case notLikeFilter: NotLikeFilter => Set(notLikeFilter.field)
      case isNullFilter: IsNullFilter => Set(isNullFilter.field)
      case pushDownFilter: PushDownFilter => returnFieldSetWithoutValidation(pushDownFilter.f)
      case t: Filter => throw new IllegalArgumentException("The field set for the input filter is undefined. " + t.field + " with filter " + t.toString)
    }
  }

  /**
    * Given a list of filters, return all given fields.
    * @param allFilters - filters to render.
    * @return - Set of fields associated with the given filters.
    */
  def returnFieldSetOnMultipleFiltersWithoutValidation(allFilters: Set[Filter]): Set[String] = {
    allFilters.flatMap(filter => returnFieldSetWithoutValidation(filter))
  }

  def returnFullFieldSetForPkAliases(filter: Filter) : Set[String] = {
    filter match {
      case _: OuterFilter => Set.empty
      case orFilter: OrFilter => orFilter.filters.flatMap{ innerFilter: Filter => returnFullFieldSetForPkAliases(innerFilter) }.toSet
      case andFilter: AndFilter => andFilter.filters.flatMap{ innerFilter: Filter => returnFullFieldSetForPkAliases(innerFilter) }.toSet
      case fieldEqualityFilter: MultiFieldForcedFilter => Set(fieldEqualityFilter.field, fieldEqualityFilter.compareTo)
      case betweenFilter: BetweenFilter => Set(betweenFilter.field)
      case dateTimeBetweenFilter: DateTimeBetweenFilter => Set(dateTimeBetweenFilter.field)
      case equalityFilter: EqualityFilter => Set(equalityFilter.field)
      case inFilter: InFilter => Set(inFilter.field)
      case notInFilter: NotInFilter => Set(notInFilter.field)
      case notEqualToFilter: NotEqualToFilter => Set(notEqualToFilter.field)
      case greaterThanFilter: GreaterThanFilter => Set(greaterThanFilter.field)
      case lessThanFilter: LessThanFilter => Set(lessThanFilter.field)
      case isNotNullFilter: IsNotNullFilter => Set(isNotNullFilter.field)
      case likeFilter: LikeFilter => Set(likeFilter.field)
      case isNullFilter: IsNullFilter => Set(isNullFilter.field)
      case pushDownFilter: PushDownFilter => returnFullFieldSetForPkAliases(pushDownFilter.f)
      case t: Filter => throw new IllegalArgumentException("The field alias set for the input filter is undefined. " + t.field + " with filter " + t.toString)
    }
  }

  def returnFillFieldSetOnMultipleFiltersForPkAliases(allFilters: Set[Filter]): Set[String] = {
    allFilters.flatMap(filter => returnFullFieldSetForPkAliases(filter))
  }

  /**
    * Given an input filter, return a map of its field(s) to its filter operation.
    * @param filter - filter to return.
    * @return - Map of filter fields to FilterOperation.
    */
  def returnFieldAndOperationMapWithoutValidation(filter: Filter) : Map[String, FilterOperation] = {
    filter match {
      case _: OuterFilter => Map.empty
      case fieldEqualityFilter: FieldEqualityFilter => Map(fieldEqualityFilter.field -> fieldEqualityFilter.operator, fieldEqualityFilter.compareTo -> fieldEqualityFilter.operator)
      case _: OrFilter => Map.empty
      case _: AndFilter => Map.empty
      case betweenFilter: BetweenFilter => Map(betweenFilter.field -> betweenFilter.operator)
      case dateTimeBetweenFilter: DateTimeBetweenFilter => Map(dateTimeBetweenFilter.field -> dateTimeBetweenFilter.operator)
      case equalityFilter: EqualityFilter => Map(equalityFilter.field -> equalityFilter.operator)
      case inFilter: InFilter => Map(inFilter.field -> inFilter.operator)
      case notInFilter: NotInFilter => Map(notInFilter.field -> notInFilter.operator)
      case notEqualToFilter: NotEqualToFilter => Map(notEqualToFilter.field -> notEqualToFilter.operator)
      case greaterThanFilter: GreaterThanFilter => Map(greaterThanFilter.field -> greaterThanFilter.operator)
      case lessThanFilter: LessThanFilter => Map(lessThanFilter.field -> lessThanFilter.operator)
      case isNotNullFilter: IsNotNullFilter => Map(isNotNullFilter.field -> isNotNullFilter.operator)
      case likeFilter: LikeFilter => Map(likeFilter.field -> likeFilter.operator)
      case isNullFilter: IsNullFilter => Map(isNullFilter.field -> isNullFilter.operator)
      case pushDownFilter: PushDownFilter => returnFieldAndOperationMapWithoutValidation(pushDownFilter.f)
      case t: Filter => throw new IllegalArgumentException("The filter map for the input filter is undefined. " + t.field + " with filter " + t.toString)
    }
  }

  /**
    * Create a map from filter field(s) to FilterOperation.
    * @param allFilters - filters to convert.
    * @return - Map from filter Field to FilterOperation.
    */
  def returnFieldAndOperationMapOnMultipleFiltersWithoutValidation(allFilters: Set[Filter]) : Map[String, FilterOperation] = {
    allFilters.flatMap{
      filter => returnFieldAndOperationMapWithoutValidation(filter) }.toMap
  }

  /**
    * Compare two forced filters on their field values to ensure
    * non-overridable forced filters are kept.
    * @param filter1 - first filter to check
    * @param filter2 - second filter to check
    * @return - A comparison of fields.
    */
  def compareForcedFilters(filter1: ForcedFilter
                             , filter2: ForcedFilter): Boolean = {
    val firstFieldSet = returnFieldSetWithoutValidation(filter1)
    val secondFieldSet = returnFieldSetWithoutValidation(filter2)
    firstFieldSet.exists(field => secondFieldSet.contains(field))
  }
}


