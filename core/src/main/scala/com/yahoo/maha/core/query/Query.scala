// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core._

/**
 * Created by jians on 10/20/15.
 */

trait Query {
  import scala.collection.JavaConverters._

  def queryContext: QueryContext
  def engine: Engine
  def aliasColumnMap: Map[String, Column]
  def additionalColumns: IndexedSeq[String]
  def ephemeralAliasColumnMap: Map[String, Column] = Map.empty
  def asString: String
  def tableName: String = queryContext.primaryTableName
  def aliasColumnMapJava : java.util.Map[String, Column] = aliasColumnMap.asJava
  def queryGenVersion: Option[Version]
}

case class OracleQuery(queryContext: QueryContext, 
                       asString: String,
                       parameters: QueryParameters, 
                       aliasColumnMap: Map[String, Column],
                       additionalColumns: IndexedSeq[String],
                       queryGenVersion: Option[Version] = None) extends Query with WithOracleEngine

case class PostgresQuery(queryContext: QueryContext,
                       asString: String,
                       parameters: QueryParameters,
                       aliasColumnMap: Map[String, Column],
                       additionalColumns: IndexedSeq[String],
                       queryGenVersion: Option[Version] = None) extends Query with WithPostgresEngine

case class HiveQuery(queryContext: QueryContext,
                     asString: String,
                     udfStatements: Option[Set[UDFRegistration]],
                     parameters: QueryParameters,
                     columnHeaders: IndexedSeq[String],
                     aliasColumnMap: Map[String, Column],
                     additionalColumns: IndexedSeq[String],
                     queryGenVersion: Option[Version] = None) extends Query with WithHiveEngine

case class PrestoQuery(queryContext: QueryContext,
                     asString: String,
                     udfStatements: Option[Set[UDFRegistration]],
                     parameters: QueryParameters,
                     columnHeaders: IndexedSeq[String],
                     aliasColumnMap: Map[String, Column],
                     additionalColumns: IndexedSeq[String],
                     queryGenVersion: Option[Version] = None) extends Query with WithPrestoEngine

case class BigqueryQuery(queryContext: QueryContext,
                         asString: String,
                         udfStatements: Option[Set[UDFRegistration]],
                         parameters: QueryParameters,
                         columnHeaders: IndexedSeq[String],
                         aliasColumnMap: Map[String, Column],
                         additionalColumns: IndexedSeq[String],
                         queryGenVersion: Option[Version] = None) extends Query with WithBigqueryEngine

object NoopQuery extends Query {
  override def queryContext: QueryContext = throw new UnsupportedOperationException("NoopQuery")

  override def asString: String = throw new UnsupportedOperationException("NoopQuery")

  override def additionalColumns: IndexedSeq[String] = throw new UnsupportedOperationException("NoopQuery")

  override def engine: Engine = throw new UnsupportedOperationException("NoopQuery")

  override def aliasColumnMap: Map[String, Column] = throw new UnsupportedOperationException("NoopQuery")

  override def queryGenVersion: Option[Version] = throw new UnsupportedOperationException("NoopQuery")
}
