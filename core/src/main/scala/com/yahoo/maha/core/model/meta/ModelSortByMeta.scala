package com.yahoo.maha.core.model.meta

import com.yahoo.maha.core.request.Order

/**
  *
  * - factSortByMap
  * - dimSortByMap
  */
case class ModelSortByMeta (
                           factSortByMap: Map[String, Order]
                           , dimSortByMap: Map[String, Order]
                           ) {
  override def toString: String =
    s"""${this.getClass.getSimpleName}:
       factSortByMap: $factSortByMap
       dimSortByMap: $dimSortByMap""".stripMargin
}
