// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.dimension

import com.yahoo.maha.core.{ClassNameHashCode, WithOracleEngine, WithPostgresEngine}

/**
 * Created by hiral on 10/2/15.
 */

sealed trait DimensionAnnotation
sealed trait DimensionAnnotationInstance extends DimensionAnnotation with ClassNameHashCode

case class OraclePKCompositeIndex(indexName: String) extends DimensionAnnotationInstance with WithOracleEngine

case object OracleHashPartitioning extends DimensionAnnotationInstance with WithOracleEngine

case class DimensionOracleStaticHint(hint: String) extends DimensionAnnotationInstance with WithOracleEngine

case class PostgresPKCompositeIndex(indexName: String) extends DimensionAnnotationInstance with WithPostgresEngine

case object PostgresHashPartitioning extends DimensionAnnotationInstance with WithPostgresEngine

case class DimensionPostgresStaticHint(hint: String) extends DimensionAnnotationInstance with WithPostgresEngine
