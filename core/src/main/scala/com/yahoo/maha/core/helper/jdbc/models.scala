package com.yahoo.maha.core.helper.jdbc

import com.yahoo.maha.core.dimension.DimLevel

import scala.collection.immutable.SortedSet

case class ColumnMetadata(label: String
                          , isAutoIncrement: Boolean
                          , nullableStatus: String
                          , precision: Int
                          , scale: Int
                          , columnType: String
                          , columnTypeName: String
                          , columnClassName: String
                          , columnDisplaySize: Int
                         )

case class PrimaryKeyMetadata(fkTableName: String
                              , fkColName: String
                              , pkTableName: String
                              , pkColName: String
                             )

case class TableMetadata(tables: SortedSet[String]
                         , primaryKeyMetadataMap: Map[String, IndexedSeq[PrimaryKeyMetadata]]
                         , pkSet: Map[String, SortedSet[String]]
                         , fkMap: Map[String, Map[String, String]]
                         , forwardMap: Map[String, SortedSet[String]]
                         , backwardMap: Map[String, SortedSet[String]]
                        )

case class SchemaDump(tableMetadata: TableMetadata, tableLevels: Map[String, DimLevel])

case class LikeCriteria(schema: String, like: String)

case class DDLDumpConfig(viewsLike: IndexedSeq[LikeCriteria], triggersLike: IndexedSeq[LikeCriteria], procsLike: IndexedSeq[LikeCriteria])