package com.yahoo.maha.core.query.hive

import com.yahoo.maha.core.query.{DimensionBundle, FactualQueryContext}
import com.yahoo.maha.core.{DefaultResult, Engine, LiteralMapper, PartitionColumnRenderer, RenderedAndFilter, RequestModel, StrType}

import scala.collection.mutable

/*
    Created by pranavbhole on 6/18/19
*/
object TestPartitionRenderer extends PartitionColumnRenderer {
  override def renderDim(requestModel: RequestModel, dimBundle: DimensionBundle, literalMapper: LiteralMapper, engine: Engine): String = {
    val partitionFilterRendered : mutable.LinkedHashSet[String] = new mutable.LinkedHashSet[String]
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
    RenderedAndFilter(partitionFilterRendered.toSet).toString
  }

  override def renderFact(queryContext: FactualQueryContext, literalMapper: LiteralMapper, engine: Engine): Option[String] = Some("factPartCol = 123")
}
