// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import org.json4s.JsonAST.JObject
import org.json4s.scalaz.JsonScalaz._

import scala.collection.immutable.Nil

/**
 * Created by hiral on 10/2/15.
 */

sealed trait DataType {
  def asJson: JObject = makeObj("type" -> toJSON(jsonDataType) :: "constraint" -> toJSON(constraint) :: Nil)
  def jsonDataType: String
  def constraint: Option[String]
  def hasStaticMapping: Boolean
  def hasUniqueStaticMapping: Boolean
  def reverseStaticMapping: Map[String, Set[String]]
}

case class StaticMapping[T](tToStringMap: Map[T, String], default: String) {
  val stringToTMap: Map[String, Set[String]] = tToStringMap.groupBy(_._2).mapValues(_.map(_._1.toString).toSet)
  val hasUniqueMapping : Boolean = (!stringToTMap.contains(default)) && stringToTMap.keySet.size == tToStringMap.keySet.size
}

object StaticMapping {
  implicit def fromInt(map: (Map[Int, String], String)) : StaticMapping[Int] = {
    StaticMapping(map._1, map._2)
  }

  implicit def fromString(map: (Map[String, String], String)) : StaticMapping[String] = {
    StaticMapping(map._1, map._2)
  }
}

case class IntType private(length: Int, staticMapping: Option[StaticMapping[Int]],
                           default: Option[Int], min: Option[Int], max: Option[Int]) extends DataType {
  val hasStaticMapping = staticMapping.isDefined
  val hasUniqueStaticMapping = staticMapping.exists(_.hasUniqueMapping)
  val reverseStaticMapping = staticMapping.map(_.stringToTMap).getOrElse(Map.empty)
  val jsonDataType = if (hasStaticMapping) "Enum" else "Number"
  val constraint: Option[String] = if (hasStaticMapping) Option.apply(reverseStaticMapping.keys.mkString("|")) else if (length > 0) Option.apply(length.toString) else None
}

case class StrType private(length: Int, staticMapping: Option[StaticMapping[String]], default: Option[String]) extends DataType {
  val hasStaticMapping = staticMapping.isDefined
  val hasUniqueStaticMapping = staticMapping.exists(_.hasUniqueMapping)
  val reverseStaticMapping = staticMapping.map(_.stringToTMap).getOrElse(Map.empty)
  val jsonDataType = if (hasStaticMapping) "Enum" else "String"
  val constraint: Option[String] = if (hasStaticMapping) {
    Option.apply(reverseStaticMapping.keys.mkString("|"))
  } else {
    if (length > 0) Option.apply(length.toString) else None
  }
}
case class DecType private(length: Int, scale:Int, default: Option[BigDecimal],
                           min: Option[BigDecimal], max: Option[BigDecimal], dummy: Int = 0) extends DataType {
  val hasStaticMapping = false
  val hasUniqueStaticMapping = false
  val reverseStaticMapping : Map[String, Set[String]] = Map.empty
  val jsonDataType: String = "Number"
  val constraint: Option[String] = if (length > 0) Option.apply(length.toString) else None
}
case class DateType private(format: Option[String]) extends DataType {
  val hasStaticMapping = false
  val hasUniqueStaticMapping = false
  val reverseStaticMapping : Map[String, Set[String]] = Map.empty
  val jsonDataType: String = "Date"
  val constraint: Option[String] = format
}
case class TimestampType private(format: Option[String]) extends DataType {
  val hasStaticMapping = false
  val hasUniqueStaticMapping = false
  val reverseStaticMapping : Map[String, Set[String]] = Map.empty
  val jsonDataType: String = "Date"
  val constraint: Option[String] = format
}

case object IntType {
  private[this] val noLength = new IntType(0, None, None, None, None)
  
  def apply() : IntType = {
    noLength
  }

  def apply(length: Int) : IntType = {
    require(length > 0, "IntType(length) : invalid argument : length > 0")
    new IntType(length, None, None, None, None)
  }

  def apply(length: Int, default: Int) : IntType = {
    require(length >= 0, "IntType(length, default) : invalid argument : length >= 0")
    new IntType(length, None, Option(default), None, None)
  }

  def apply(length: Int, staticMapping: StaticMapping[Int]) : IntType = {
    require(length > 0, "IntType(length, staticMapping) : invalid argument : length > 0")
    require(staticMapping.tToStringMap.nonEmpty, "IntType(length, (Map[Int, String], String)) : static mapping cannot be empty")
    new IntType(length, Option(staticMapping), None, None, None)
  }

  def apply(length: Int, default: Int, min: Int, max: Int): IntType = {
    require(length > 0, "IntType(length, default, min, max) : invalid argument : length > 0")
    new IntType(length, None, Option(default), Option(min), Option(max))
  }

}

case object StrType {
  private[this] val noLength = new StrType(0, None, None)

  def apply() : StrType = {
    noLength
  }

  def apply(length: Int) : StrType = {
    require(length > 0, "StrType(length) : invalid argument : length > 0")
    new StrType(length, None, None)
  }

  def apply(length: Int, default: String) : StrType = {
    require(length >= 0, "StrType(length, default) : invalid argument : length >= 0")
    new StrType(length, None, Option(default))
  }

  def apply(length: Int, staticMapping: StaticMapping[String]) : StrType = {
    require(length > 0, "StrType(length, staticMapping) : invalid argument : length > 0")
    require(staticMapping.tToStringMap.nonEmpty, "StringType(length, (Map[String, String], String)) : static mapping cannot be empty")
    new StrType(length, Option(staticMapping), None)
  }

  def apply(length: Int, staticMapping: StaticMapping[String], default: String) : StrType = {
    require(length > 0, "StrType(length, staticMapping, default) : invalid argument : length > 0")
    require(staticMapping.tToStringMap.nonEmpty, "StringType(length, (Map[String, String], String)) : static mapping cannot be empty")
    new StrType(length, Option(staticMapping), Option(default))
  }
}

case object DecType {
  private[this] val noLength = new DecType(0, 0, None, None, None)

  def apply() : DecType = {
    noLength
  }

  def apply(length: Int) : DecType = {
    require(length > 0, "DecType(length) : invalid argument : length > 0")
    new DecType(length, 0, None, None, None)
  }

  def apply(length: Int, default: String) : DecType = {
    require(length >= 0, "DecType(length, default) : invalid argument : length >= 0")
    val bd = BigDecimal(default)
    //convert to string and then to double for validation
    bd.toString().toDouble
    new DecType(length, 0, Option(bd), None, None)
  }

  def apply(length: Int, scale: Int) : DecType = {
    require(length > 0, "DecType(length, scale) : invalid argument : length > 0")
    require(scale >= 0, "DecType(length, scale) : invalid argument : scale >= 0")
    require(length >= scale, "DecType(length, scale) : invalid argument : length >= scale")
    new DecType(length, scale, None, None, None)
  }

  def apply(length: Int, scale: Int, default: String) : DecType = {
    require(length >= 0, "DecType(length, scale, default) : invalid argument : length >= 0")
    require(scale >= 0, "DecType(length, scale, default) : invalid argument : scale >= 0")
    val bd = BigDecimal(default)
    //convert to string and then to double for validation
    bd.toString().toDouble
    new DecType(length, scale, Option(bd), None, None)
  }

  def apply(length: Int, default: String, min: String, max: String): DecType = {
    require(length >= 0, "DecType(length, default, min, max) : invalid argument : length >= 0")
    val minBD = BigDecimal(min)
    minBD.toString().toDouble
    val maxBD = BigDecimal(max)
    maxBD.toString().toDouble
    val defaultBD = BigDecimal(default)
    defaultBD.toString().toDouble
    new DecType(length, 0, Option(defaultBD), Option(minBD), Option(maxBD))
  }

  def apply(length: Int, scale: Int, default: String, min: String, max: String): DecType = {
    require(length > 0, "DecType(length, scale, default, min, max) : invalid argument : length > 0")
    val minBD = BigDecimal(min)
    minBD.toString().toDouble
    val maxBD = BigDecimal(max)
    maxBD.toString().toDouble
    val defaultBD = BigDecimal(default)
    defaultBD.toString().toDouble
    new DecType(length, scale, Option(defaultBD), Option(minBD), Option(maxBD))
  }
}

case object DateType {
  private[this] val noFormat = new DateType(None)
  private[this] val ORACLE_DATE_FORMAT = "YYYY-MM-DD"
  private[this] val HIVE_DATE_FORMAT = "YYYY-MM-dd"
  private[this] val DRUID_DATE_HOUR_FORMAT = "YYYYMMddHH"
  private[this] val HIVE_DATE_STRING_FORMAT = "YYYYMMDD"
  private[this] val HIVE_DATE_HOUR_STRING_FORMAT = "YYYYMMDDhh"
  private[this] val DATE_FORMAT = "yyyyMMdd"
  private[this] val DRUID_DATE_FORMAT = "YYYYMMdd"
  private[this] val DRUID_HOUR_FORMAT = "HH"
  private[this] val DRUID_MINUTE_FORMAT = "mm"
  private[this] val UTC_TIME_FORMAT = "YYYYMMDDhhmm"
  private[this] val HOUR_FORMAT = "YYYY-MM-DD HH24"
  private[this] val ORACLE_HOUR_FORMAT = "hh"
  private[this] val HIVE_HOUR = "YYYYMMDDHH"
  private[this] val UTC_TIME_HOUR = "yyyyMMddHH"

  def apply() : DateType = {
    noFormat
  }

  def apply(format: String) : DateType = {
    require(format != null && format.nonEmpty, "DateType(format) : invalid argument : format cannot be null or empty")
    val validFormats = Set(ORACLE_DATE_FORMAT, HIVE_DATE_FORMAT, DATE_FORMAT, DRUID_DATE_FORMAT, DRUID_HOUR_FORMAT, DRUID_MINUTE_FORMAT, UTC_TIME_FORMAT, HOUR_FORMAT, ORACLE_HOUR_FORMAT, HIVE_HOUR
    ,HIVE_DATE_STRING_FORMAT, HIVE_DATE_HOUR_STRING_FORMAT,DRUID_DATE_HOUR_FORMAT,UTC_TIME_HOUR)
    require(validFormats.contains(format), s"Invalid format for DateType($format)")
    new DateType(Option(format))
  }
}

case object TimestampType {

  private[this] val noFormat = new TimestampType(None)

  def apply() : TimestampType = {
    noFormat
  }

  def apply(format: String) : TimestampType = {
    require(format != null && format.nonEmpty, "TimestampType(format) : invalid argument : format cannot be null or empty")
    //TODO: add format validation
    new TimestampType(Option(format))
  }
  
}
