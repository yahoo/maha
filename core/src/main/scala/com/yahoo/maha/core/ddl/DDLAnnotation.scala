// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.ddl

import com.yahoo.maha.core.{WithHiveEngine, WithOracleEngine, WithPrestoEngine, WithPostgresEngine, WithBigqueryEngine}

/**
 * Created by shengyao on 1/28/16.
 */
sealed trait DDLAnnotation

case class OracleDDLAnnotation(pks: Set[String] = Set.empty,
                               partCols: Set[String] = Set.empty) extends DDLAnnotation with WithOracleEngine

case class HiveDDLAnnotation(annotations: Map[String, String] = Map.empty,
                             columnOrdering: IndexedSeq[String] = IndexedSeq.empty) extends DDLAnnotation with WithHiveEngine

case class PrestoDDLAnnotation(annotations: Map[String, String] = Map.empty,
                             columnOrdering: IndexedSeq[String] = IndexedSeq.empty) extends DDLAnnotation with WithPrestoEngine

case class PostgresDDLAnnotation(pks: Set[String] = Set.empty,
                               partCols: Set[String] = Set.empty) extends DDLAnnotation with WithPostgresEngine

case class BigqueryDDLAnnotation(annotations: Map[String, String] = Map.empty,
                                 columnOrdering: IndexedSeq[String] = IndexedSeq.empty) extends DDLAnnotation with WithBigqueryEngine

