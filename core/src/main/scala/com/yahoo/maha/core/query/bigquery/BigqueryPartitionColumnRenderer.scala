// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.bigquery

import com.yahoo.maha.core.query.{DimensionBundle, FactualQueryContext}
import com.yahoo.maha.core._
import scala.collection.mutable

object BigqueryPartitionColumnRenderer extends PartitionColumnRenderer {
  override def renderDim(
    requestModel: RequestModel,
    dimBundle: DimensionBundle,
    literalMapper: LiteralMapper,
    engine: Engine
  ): String = {
    val partitionFilterRendered: mutable.LinkedHashSet[String] = new mutable.LinkedHashSet[String]
    dimBundle.dim.partitionColumns.foreach {
      column =>
        val name = column.name
        val defaultValue = {
          column.dataType match {
            case c: StrType => c.default
            case _ => None
          }
        }
        val partitionPredicate = s"%DEFAULT_DIM_PARTITION_PREDICTATE%"

        val renderedValue = {
          if (defaultValue.isDefined) {
            literalMapper.toLiteral(column, defaultValue.get)
          } else {
            literalMapper.toLiteral(column, partitionPredicate)
          }
        }
        partitionFilterRendered.add(DefaultResult(s"""$name = $renderedValue """).filter)
    }
    if (partitionFilterRendered.isEmpty) ""
    else RenderedAndFilter(partitionFilterRendered.toSet).toString
  }

  override def renderFact(
    queryContext: FactualQueryContext,
    literalMapper: LiteralMapper,
    engine: Engine
  ): Option[String] = None
}
