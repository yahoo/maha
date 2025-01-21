package com.yahoo.maha.core.helper.jdbc

import com.yahoo.maha.core.dimension.DimLevel

import scala.collection.immutable.SortedSet
import scala.collection.mutable

case class ColumnMetadata(columnName: String
                          , typeName: String
                          , dataType: String
                          , columnSize: Int
                          , decimalDigits: Int
                          , isNullable: String
                          , isAutoIncrement: String)

case class PrimaryKeyMetadata(fkTableName: String
                              , fkColName: String
                              , pkTableName: String
                              , pkColName: String
                             )

case class TableMetadata(tables: SortedSet[String]
                         , primaryKeyMetadataMap: Map[String, IndexedSeq[PrimaryKeyMetadata]]
                         , pkSet: Map[String, SortedSet[String]]
                         , fkMap: Map[String, Map[String, String]]
                         , colMap: Map[String, IndexedSeq[ColumnMetadata]]
                        )

case class TableDependencyMap(forwardMap: Map[String, SortedSet[String]]
                              , backwardMap: Map[String, SortedSet[String]])

object TableDependencyMap {
  def from(tableMetadata: TableMetadata): TableDependencyMap = {
    //table -> all dependent tables
    val forwardMap = tableMetadata.fkMap.map { case (tn, fkMap) => tn -> fkMap.values.to[SortedSet] }

    //table -> all tables depending on table
    val backwardMap: mutable.Map[String, mutable.HashSet[String]] = new mutable.HashMap[String, mutable.HashSet[String]]().withDefault(d => new mutable.HashSet[String]())

    tableMetadata.fkMap.foreach {
      case (tn, fkMap) =>
        val bwMap: Map[String, mutable.HashSet[String]] = fkMap.values.toSet[String].map(tn => tn -> backwardMap(tn)).toMap
        bwMap.foreach(_._2.add(tn))
        backwardMap ++= bwMap
    }
    TableDependencyMap(forwardMap = forwardMap, backwardMap = backwardMap.mapValues(_.to[SortedSet]).toMap)
  }
}

case class SchemaDump(tableMetadata: TableMetadata, tableDependencyMap: TableDependencyMap, tableLevels: Map[String, DimLevel])

case class ResultSetColumnMetadata(label: String
                                   , isAutoIncrement: Boolean
                                   , nullableStatus: String
                                   , precision: Int
                                   , scale: Int
                                   , columnType: String
                                   , columnTypeName: String
                                   , columnClassName: String
                                   , columnDisplaySize: Int
                                  )

case class LikeCriteria(schema: String, like: String)

case class DDLDumpConfig(viewsLike: IndexedSeq[LikeCriteria], triggersLike: IndexedSeq[LikeCriteria], procsLike: IndexedSeq[LikeCriteria])
