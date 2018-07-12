// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import com.yahoo.maha.core.dimension.Dimension
import com.yahoo.maha.core.query.{DimensionBundle, FactualQueryContext}
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable

/**
 * Created by pranavbhole on 05/04/16.
 */
trait PartitionColumnRenderer {

  def renderDim(requestModel: RequestModel,
                 dimBundle: DimensionBundle,
                literalMapper: LiteralMapper,
                engine: Engine) : String
  
  def renderFact(
             queryContext: FactualQueryContext,
             literalMapper: LiteralMapper,
             engine: Engine) : Option[String]
}

/* Please note that this is DefaultPartitionColumnRenderer which is not schema specific
*/
object DefaultPartitionColumnRenderer extends PartitionColumnRenderer {

  def renderDim(requestModel: RequestModel,
                 dimBundle: DimensionBundle,
                literalMapper: LiteralMapper,
                engine: Engine) : String = {
    val partitionFilterRendered = new mutable.LinkedHashSet[String]
    dimBundle.dim.partitionColumns.foreach {
      column=>
        val name = column.name
        val defaultValue = {
          column.dataType match {
            case c:StrType=> c.default
            case _ => None
          }
        }
        val partitionPredicate = s"%DEFAULT_DIM_PARTITION_PREDICTATE%"

        val renderedValue = {
          if (defaultValue.isDefined) {
            literalMapper.toLiteral(column,defaultValue.get)
          } else {
            literalMapper.toLiteral(column,partitionPredicate)
          }
        }
        partitionFilterRendered.add(DefaultResult(s"""$name = $renderedValue """).filter)
    }
    AndFilter(partitionFilterRendered.toSet).toString
  }

  def renderFact(queryContext: FactualQueryContext,
                 literalMapper: LiteralMapper,
                 engine: Engine) : Option[String] = None
}

