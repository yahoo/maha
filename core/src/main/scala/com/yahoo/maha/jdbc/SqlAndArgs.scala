// Copyright 2018, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.jdbc

/**
  * Created by hiral on 5/17/18.
  */
// SQL Interpolation
case class SqlAndArgs(sql: String, args: Seq[Any]) {
  def +(that: SqlAndArgs): SqlAndArgs = {
    SqlAndArgs(sql + " " + that.sql, args ++ that.args)
  }

  def stripMargin: SqlAndArgs = SqlAndArgs(sql.stripMargin, args)
}
