// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
/**
 * Created by hiral on 12/19/15.
 */

package com.yahoo.maha

import java.sql.{Date, ResultSet, Timestamp}
import java.text.SimpleDateFormat

import scala.util.Try

package object jdbc {

  type Seq[+A] = scala.collection.immutable.Seq[A]
  val Seq = scala.collection.immutable.Seq
  type List[+A] = scala.collection.immutable.List[A]
  val List = scala.collection.immutable.List
  type Vector[+A] = scala.collection.immutable.Vector[A]
  val Vector = scala.collection.immutable.Vector

  implicit class RowData(rs: ResultSet) {
    def apply(columnNumber: Int): Any = rs.getObject(columnNumber)
    def apply(columnName: String): Any = rs.getObject(columnName)
    def toIterator[E](rowMapper: ResultSet => E): Iterator[E] = new Iterator[E] {
      override def hasNext: Boolean = rs.next()
      override def next(): E = rowMapper(rs)
    }.toIterator
  }

  implicit class DateFormatter(date: java.sql.Date) {
    def print: String = new SimpleDateFormat("yyyy-MM-dd").format(date)
  }

  implicit class TimeFormatter(time: java.sql.Time) {
    def print: String = new SimpleDateFormat("HH:mm:ss.SSS").format(time)
  }

  implicit class TimestampFormatter(timestamp: java.sql.Timestamp) {
    def print: String = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(timestamp)
  }

  implicit class NullOption(v: Try[String]) {
    def toOpt: Option[String] = v.toOption.find(_ != "null")
  }

  implicit class PimpedRowData(row: ResultSet) {
    def str(name: String): String = Option(row(name).asInstanceOf[String]).map(_.trim).orNull
    def str(index: Int): String = Option(row(index).asInstanceOf[String]).map(_.trim).orNull
    def strOpt(name: String): Option[String] = Option(str(name))
    def strOpt(index: Int): Option[String] = Option(str(index))
    def int(name: String): Int = row(name).asInstanceOf[Int]
    def int(index: Int): Int = row(index).asInstanceOf[Int]
    def intOpt(name: String): Option[Int] = Try(int(name)).toOption
    def intOpt(index: Int): Option[Int] = Try(int(index)).toOption
    def long(name: String): Long = row(name).asInstanceOf[Long]
    def long(index: Int): Long = row(index).asInstanceOf[Long]
    def longOpt(name: String): Option[Long] = Try(long(name)).toOption
    def longOpt(index: Int): Option[Long] = Try(long(index)).toOption
    def float(name: String): Float = row(name).asInstanceOf[Float]
    def float(index: Int): Float = row(index).asInstanceOf[Float]
    def floatOpt(name: String): Option[Float] = Try(float(name)).toOption
    def floatOpt(index: Int): Option[Float] = Try(float(index)).toOption
    def double(name: String): Double = row(name).asInstanceOf[Double]
    def double(index: Int): Double = row(index).asInstanceOf[Double]
    def doubleOpt(index: Int): Option[Double] = Try(double(index)).toOption
    def doubleOpt(name: String): Option[Double] = Try(double(name)).toOption
    def bi(name: String): BigInt = row(name).asInstanceOf[BigInt]
    def bi(index: Int): BigInt = row(index).asInstanceOf[BigInt]
    def biOpt(name: String): Option[BigInt] = Try(bi(name)).toOption
    def biOpt(index: Int): Option[BigInt] = Try(bi(index)).toOption
    def bd(name: String): BigDecimal = BigDecimal(row(name).asInstanceOf[java.math.BigDecimal])
    def bd(index: Int): BigDecimal = BigDecimal(row(index).asInstanceOf[java.math.BigDecimal])
    def bdOpt(name: String): Option[BigDecimal] = Option(bd(name))
    def bdOpt(index: Int): Option[BigDecimal] = Option(bd(index))
    def date(name: String): Date = row(name).asInstanceOf[Date]
    def date(index:Int): Date = row(index).asInstanceOf[Date]
    def dateOpt(name: String): Option[Date] = Option(date(name))
    def dateOpt(index: Int): Option[Date] = Option(date(index))
    def dateStr(name: String): String = date(name).print
    def dateStr(index: Int): String = date(index).print
    def dateStrOpt(name: String): Option[String] = Option(date(name)).map(_.print)
    def dateStrOpt(index: Int): Option[String] = Option(date(index)).map(_.print)
    def dateTime(name: String): Timestamp = row(name).asInstanceOf[Timestamp]
    def dateTime(index: Int): Timestamp = row(index).asInstanceOf[Timestamp]
    def dateTimeStr(name: String): String = dateTime(name).print
    def dateTimeStr(index: Int): String = dateTime(index).print
    def dateTimeStrOpt(name: String): Option[String] = Option(dateTime(name)).map(_.print)
    def dateTimeStrOpt(index: Int): Option[String] = Option(dateTime(index)).map(_.print)
    def bool(name: String): Boolean = row(name).asInstanceOf[Boolean]
    def bool(index: Int): Boolean = row(index).asInstanceOf[Boolean]
    def boolOpt(name: String): Option[Boolean] = Try(bool(name)).toOption
    def boolOpt(index: Int): Option[Boolean] = Try(bool(index)).toOption
  }

  //
  // SqlInterpolation
  //

  case class SqlAndArgs(sql: String, args: Seq[Any]) {
    def +(that: SqlAndArgs): SqlAndArgs = {
      SqlAndArgs(sql + " " + that.sql, args ++ that.args)
    }

    def stripMargin: SqlAndArgs = SqlAndArgs(sql.stripMargin, args)
  }

  // '$arg does string interpolation rather than argument
  implicit class SqlInterpolationHelper(val sc: StringContext) {
    def q(args: Any*): SqlAndArgs = {

      var actualArgs: List[Any] = List()
      val parts = sc.parts.iterator.toList
      val inserts = args.iterator.toList

      val pi = parts.zip(inserts)
      val sql = pi.foldLeft("")((a:String, b:(String, Any)) => {
        if (b._1.endsWith("'")) {
          a + b._1.dropRight(1) + b._2
        }
        else {
          if (b._2.isInstanceOf[List[Any]]) {
            val list = b._2.asInstanceOf[List[Any]]
            actualArgs = list.reverse ++ actualArgs
            a + b._1 + ("?," * list.length).dropRight(1)
          }
          else {
            actualArgs = b._2 :: actualArgs
            a + b._1 + "?"
          }
        }
      })
      val extra = if (pi.length < parts.length) parts.reverse.head.toString else ""

      SqlAndArgs(sql + extra, actualArgs.reverse)
    }
  }
}
