// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import scala.collection.mutable.LinkedHashSet
import scala.collection.mutable.ArrayBuffer

/**
 * Created by hiral on 11/2/15.
 */
class QueryBuilder(val initSize: Int, val orderBySize: Int) {
  /**
   * The list of items (each with their alias) in the outer SELECT
   */
  private[this] val outerSelectColumns: LinkedHashSet[String] = new LinkedHashSet()
  /**
   * The list of items for the fact + dimension group by view SELECT
   */
  private[this] val preOuterSelectColumns: LinkedHashSet[String] = new LinkedHashSet()
  /**
   * The list of items for the fact view SELECT
   */
  private[this] val factViewColumns: LinkedHashSet[String] = new LinkedHashSet[String]()
  /**
   * WHERE clause
   */
  private[this] var whereClause: String = ""
  /**
   * HAVING clause
   */
  private[this] var havingClause: String = ""
  /**
   * outer WHERE clause
   */
  private[this] var outerWhereClause: String = ""
  /**
   * The list of GROUP BY clauses for the inner fact SELECT
   */
  private[this] val groupByExpressions: LinkedHashSet[String] = LinkedHashSet.empty
  /**
   * The list of GROUP BY clauses for the inner fact SELECT
   */
  private[this] val outerGroupByExpressions: LinkedHashSet[String] = LinkedHashSet.empty
  /**
   * The list of ORDER BY expressions for the outer most query
   */
  private[this] lazy val orderByExpressions: LinkedHashSet[String] = LinkedHashSet.empty
  /**
   * The driving dimension join clauses
   */
  private[this] val dimensionJoins: LinkedHashSet[String] = LinkedHashSet.empty

  /**
   * Multi-dimension LOJ clause
   */
  private[this] val multiDimensionJoins: ArrayBuffer[String] = ArrayBuffer.empty
  /**
   * Hive only: the date-related partition predicates
   */
  private[this] lazy val partitionPredicates: ArrayBuffer[String] = ArrayBuffer.empty
  /**
   * Hive only: the column headers (for the columns on the outermost SELECT)
   */
  private[this] lazy val columnHeaders: LinkedHashSet[String] = LinkedHashSet.empty

  private[this] lazy val outerQueryEndClauses: LinkedHashSet[String] = new LinkedHashSet[String]

  private[this] var hasDimensionPagination: Boolean = false

  def getHasDimensionPagination: Boolean = hasDimensionPagination

  def setHasDimensionPagination(): Unit = {
    hasDimensionPagination = true
  }

  def addOuterColumn(fragment: String) {
    outerSelectColumns += fragment
  }

  def getOuterColumns: String = {
    outerSelectColumns.mkString(", ")
  }

  def addPreOuterColumn(col: String) = {
    preOuterSelectColumns +=col
  }

  def getPreOuterColumns : String = {
    preOuterSelectColumns.mkString(", ")
  }

  def addGroupBy(expr: String) {
     groupByExpressions add expr
  }

  def getGroupByExpressionsList: LinkedHashSet[String] = {
    groupByExpressions
  }

  def getGroupByExpression: String = {
    getGroupByExpressionsList.mkString(", ")
  }

  def getGroupByClause: String = {
    if (getGroupByExpressionsList.isEmpty) "" else s"GROUP BY $getGroupByExpression"
  }

  def getOuterGroupByClause: String = {
     if (outerGroupByExpressions.isEmpty) "" else s"GROUP BY ${outerGroupByExpressions.mkString(", ")}"
  }

  def addOuterGroupByExpressions(outerGroupBy: String) {
     this.outerGroupByExpressions.add(outerGroupBy)
  }

  def getOrderByExpressionsList: LinkedHashSet[String] = {
    orderByExpressions
  }

  def getOrderByExpression: String = {
    getOrderByExpressionsList.mkString(", ")
  }

  def getOrderByClause: String = {
    if (getOrderByExpressionsList.isEmpty) "" else s"ORDER BY $getOrderByExpression"
  }

  def addOrderBy(columnExpression: String) {
    orderByExpressions += columnExpression
  }

  def addFactViewColumn(fragment: String) {
    if(fragment.nonEmpty) {
      factViewColumns += fragment
    }
  }

  def getFactViewColumns: String = {
    factViewColumns.mkString(", ")
  }

  def getWhereClause: String = {
    whereClause
  }

  def containsFactViewColumns(s: String): Boolean = {
    factViewColumns.contains(s)
  }

  def setWhereClause(whereClause: String) {
    this.whereClause = whereClause
  }

  def getOuterWhereClause: String = {
    outerWhereClause
  }

  def setOuterWhereClause(outerWhereClause: String) {
    this.outerWhereClause = outerWhereClause
  }

  def addDimensionJoin(fragment: String) {
    dimensionJoins += fragment
  }

  def addMultiDimensionJoin(fragment: String) {
    multiDimensionJoins += fragment
  }

  def getJoinExpressions: String = {
    dimensionJoins.mkString("")
  }

  def getMultiDimensionJoinExpressions: String = {
    multiDimensionJoins.mkString("")
  }

  def getHavingClause: String = {
    havingClause
  }

  def setHavingClause(havingClause: String) {
    this.havingClause = havingClause
  }

  def addPartitionPredicate(predicate: String) {
    partitionPredicates += predicate
  }

  def getPartitionPredicates: String = {
    partitionPredicates.view.filter(_ != null).mkString(" OR ")
  }

  def addColumnHeader(columnName: String) {
    columnHeaders += columnName
  }

  def getColumnHeaders: String = {
    columnHeaders.mkString(",")
  }
  
  def addOuterQueryEndClause(clause: String): Unit = {
    outerQueryEndClauses += clause
  }
  
  def getOuterQueryEndClause: String = {
    outerQueryEndClauses.mkString(" ")
  }
}
