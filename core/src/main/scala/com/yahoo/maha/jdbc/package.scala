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

  implicit class RowData(rs: ResultSet) {
    def apply(columnNumber: Int): Any = rs.getObject(columnNumber)
    def apply(columnName: String): Any = rs.getObject(columnName)
    def toIterator[E](rowMapper: ResultSet => E): Iterator[E] = new Iterator[E] {
      override def hasNext: Boolean = rs.next()
      override def next(): E = rowMapper(rs)
    }
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
}
