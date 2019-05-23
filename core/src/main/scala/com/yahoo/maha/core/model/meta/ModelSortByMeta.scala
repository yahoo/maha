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

}
