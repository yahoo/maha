package com.yahoo.maha.core.query

import java.math.MathContext

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.yahoo.maha.core.{IntType, _}
import grizzled.slf4j.Logging
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.json4s.DefaultFormats

import scala.collection.concurrent.TrieMap
import scala.math.BigDecimal.RoundingMode

trait ResultSetTransformer extends Logging {

  def extractBigDecimal(field: Any) : BigDecimal = {
    field match  {
      case f : BigInt => BigDecimal(f.asInstanceOf[BigInt])
      case f : Double => BigDecimal(f.asInstanceOf[Double])
      case f : Long => BigDecimal(f.asInstanceOf[Long])
      case f : Float => BigDecimal(f.asInstanceOf[Float].toDouble)
      case f : Int => BigDecimal(f.asInstanceOf[Int])
      case _ => BigDecimal(0.0)
    }
  }

  def transform(grain: Grain, resultAlias: String, column: Column, inputValue: Any) : Any
  def canTransform(resultAlias: String, column: Column): Boolean
}

object ResultSetTransformer {
  val DEFAULT_TRANSFORMS = List(new DateTransformer, new NumberTransformer)
}

object DateTransformer {
  private[this] val dateTimeFormatters = new TrieMap[String, DateTimeFormatter]()

  def getDateTimeFormatter(fmt: String) : DateTimeFormatter = {
    if (dateTimeFormatters.contains(fmt)) {
      dateTimeFormatters(fmt)
    } else {
      dateTimeFormatters.synchronized {
        if (!dateTimeFormatters.contains(fmt)) {
          val newFormatter = DateTimeFormat.forPattern(fmt).withZoneUTC()
          dateTimeFormatters += fmt -> newFormatter
          newFormatter
        } else {
          dateTimeFormatters(fmt)
        }
      }
    }
  }
}

case class DateTransformer() extends ResultSetTransformer {

  override def transform(grain: Grain, resultAlias: String, column: Column, inputValue: Any): Any = {

    val sourceFormatDate: String = inputValue.toString

    if (!canTransform(resultAlias, column)) return sourceFormatDate

    val formattedDate: String = column.dataType match {
      case DateType(fmtOption) if fmtOption.isDefined =>
        val fmt = fmtOption.get
        val formatter = DateTransformer.getDateTimeFormatter(fmt)
        Grain.getGrainByField(resultAlias).fold(sourceFormatDate){
          grain => grain.toFormattedString(formatter.parseDateTime(sourceFormatDate))
        }
      case _ =>
        sourceFormatDate
    }

    return formattedDate
  }

  def canTransform(resultAlias: String, column: Column): Boolean = {
    resultAlias.equalsIgnoreCase(DailyGrain.DAY_FILTER_FIELD)
  }
}


case class NumberTransformer() extends ResultSetTransformer {

  val DEFAULT_SCALE = 10
  implicit val formats = DefaultFormats

  val mathContextCache = CacheBuilder
    .newBuilder()
    .maximumSize(100)
    .concurrencyLevel(Runtime.getRuntime.availableProcessors())
    .build(new CacheLoader[java.lang.Integer, MathContext]() {
      override def load(key: java.lang.Integer): MathContext = {
        new MathContext(key, java.math.RoundingMode.HALF_EVEN)
      }
    })

  override def transform(grain: Grain, resultAlias: String, column: Column, resultValue: Any): Any = {

    val outputValue = column.dataType  match {
      case DecType(len: Int, scale, _, min, _, _) => {

        val resultDecimal = extractBigDecimal(resultValue)

        val result: BigDecimal = {
          if (scale > 0 && len > 0) {
            val mc = mathContextCache.get(len)
            resultDecimal.setScale(scale, RoundingMode.HALF_EVEN).round(mc)
          } else if(scale > 0) {
            resultDecimal.setScale(scale, RoundingMode.HALF_EVEN)
          } else if(len > 0) {
            val mc = mathContextCache.get(len)
            resultDecimal.setScale(DEFAULT_SCALE, RoundingMode.HALF_EVEN).round(mc)
          } else {
            resultDecimal.setScale(DEFAULT_SCALE, RoundingMode.HALF_EVEN)
          }
        }
        if (result % 1 == 0) { // No decimal places
          result.setScale(0)  // Do not want to show .00 in case of exact double value
        } else {
          result.doubleValue() // Double value prints without trailing zeros, unlike BigDecimal
        }
      }
      case IntType(_,_,_,_,_) =>
        if (resultValue.isInstanceOf[Double]) {
          BigDecimal(resultValue.asInstanceOf[Double]).toLong
        } else if (resultValue.isInstanceOf[String]) {
          try{
            BigInt(resultValue.asInstanceOf[String]).toLong
          } catch {
            case e:Exception =>
              resultValue
          }
        } else {
          resultValue
        }
      case _ =>
        resultValue
    }

    outputValue
  }

  def canTransform(resultAlias: String, column: Column): Boolean = {
    !resultAlias.equalsIgnoreCase(DailyGrain.DAY_FILTER_FIELD)
  }
}
