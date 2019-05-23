package com.yahoo.maha.core.model.meta

import com.yahoo.maha.core.model.{ColumnInfo, SortByColumnInfo}

/**
  *
  * - requestCols
  * - requestSortByCols
  * - dimColumnAliases
  */
case class ModelColumnsMeta (
                            requestCols: IndexedSeq[ColumnInfo]
                            , requestSortByCols: IndexedSeq[SortByColumnInfo]
                            , dimColumnAliases: Set[String]
                            ) {

}
