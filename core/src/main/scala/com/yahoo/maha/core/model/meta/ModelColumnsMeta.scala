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
  override def toString: String =
    s"""${this.getClass.getSimpleName}:
       requestCols: $requestCols
       requestSortByCols: $requestSortByCols
       dimColumnAliases: $dimColumnAliases""".stripMargin
}
