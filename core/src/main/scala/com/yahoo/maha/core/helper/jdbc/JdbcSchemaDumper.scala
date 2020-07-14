package com.yahoo.maha.core.helper.jdbc

import java.sql.ResultSetMetaData

import com.yahoo.maha.core.dimension.{DimLevel, LevelOne}
import com.yahoo.maha.jdbc.JdbcConnection
import grizzled.slf4j.Logging
import org.apache.commons.lang3.StringUtils

import scala.collection.immutable.SortedSet
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object JdbcSchemaDumper extends Logging {
  //cat /tmp/t|grep static|sed -E 's/ *public [a-z ]* //g;s/  *//g;s/([^=]*)=([-0-9]*);/, \2 -> "\1"/g'
  val columnTypeMap: Map[Int, String] = Map(-7 -> "BIT"
    , -6 -> "TINYINT"
    , 5 -> "SMALLINT"
    , 4 -> "INTEGER"
    , -5 -> "BIGINT"
    , 6 -> "FLOAT"
    , 7 -> "REAL"
    , 8 -> "DOUBLE"
    , 2 -> "NUMERIC"
    , 3 -> "DECIMAL"
    , 1 -> "CHAR"
    , 12 -> "VARCHAR"
    , -1 -> "LONGVARCHAR"
    , 91 -> "DATE"
    , 92 -> "TIME"
    , 93 -> "TIMESTAMP"
    , -2 -> "BINARY"
    , -3 -> "VARBINARY"
    , -4 -> "LONGVARBINARY"
    , 0 -> "NULL"
    , 1111 -> "OTHER"
    , 2000 -> "JAVA_OBJECT"
    , 2001 -> "DISTINCT"
    , 2002 -> "STRUCT"
    , 2003 -> "ARRAY"
    , 2004 -> "BLOB"
    , 2005 -> "CLOB"
    , 2006 -> "REF"
    , 70 -> "DATALINK"
    , 16 -> "BOOLEAN"
    , -8 -> "ROWID"
    , -15 -> "NCHAR"
    , -9 -> "NVARCHAR"
    , -16 -> "LONGNVARCHAR"
    , 2011 -> "NCLOB"
    , 2009 -> "SQLXML"
    , 2012 -> "REF_CURSOR"
    , 2013 -> "TIME_WITH_TIMEZONE"
    , 2014 -> "TIMESTAMP_WITH_TIMEZONE")

  implicit class ExtractResultSetMetaData(resultSetMetaData: ResultSetMetaData) {
    def extractResultSetMetaData: Map[String, ColumnMetadata] = {
      if (resultSetMetaData == null)
        return Map.empty
      try {
        var count = 1
        val map = new mutable.HashMap[String, ColumnMetadata]
        while (count <= resultSetMetaData.getColumnCount) {
          val isAutoIncrement = resultSetMetaData.isAutoIncrement(count)
          val nullableStatus = resultSetMetaData.isNullable(count) match {
            case 0 => "columnNoNulls"
            case 1 => "columnNullable"
            case _ => "columnNullableUnknown"
          }
          val scale = resultSetMetaData.getScale(count)
          val precision = resultSetMetaData.getPrecision(count)
          val columnDisplaySize = resultSetMetaData.getColumnDisplaySize(count)
          val label = resultSetMetaData.getColumnLabel(count)
          val columnTypeInt = resultSetMetaData.getColumnType(count)
          val columnType = columnTypeMap.getOrElse(columnTypeInt, s"UNKNOWN type $columnTypeInt")
          val columnTypeName = resultSetMetaData.getColumnTypeName(count)
          val columnClassName = resultSetMetaData.getColumnClassName(count)
          map.put(label
            , ColumnMetadata(label = label
              , isAutoIncrement = isAutoIncrement
              , nullableStatus = nullableStatus
              , scale = scale
              , precision = precision
              , columnDisplaySize = columnDisplaySize
              , columnType = columnType
              , columnTypeName = columnTypeName
              , columnClassName = columnClassName
            ))
          count += 1
        }
        map.toMap
      } catch {
        case e: Exception =>
          error("Failed to extract jdbc schema metadata", e)
          Map.empty
      }
    }
  }

  def buildTableMetadata(jdbcConnection: JdbcConnection): Try[TableMetadata] = {
    jdbcConnection.withConnection { connection =>
      val databaseMetaData = connection.getMetaData
      val rs = databaseMetaData.getTables(null, null, null, Array[String]("TABLE"))
      val tables = new ArrayBuffer[String]()
      while (rs.next()) {
        val tn = rs.getString("TABLE_NAME")
        if (StringUtils.isNotBlank(tn)) {
          tables += tn
        }
      }

      val tablePkSet: mutable.Map[String, SortedSet[String]] = new mutable.HashMap[String, SortedSet[String]]()
      val tableFkMap: mutable.Map[String, Map[String, String]] = new mutable.HashMap[String, Map[String, String]]()
      val tablePrimaryKeyMetadataMap: mutable.Map[String, IndexedSeq[PrimaryKeyMetadata]] = new mutable.HashMap[String, IndexedSeq[PrimaryKeyMetadata]]
      val forwardMap: mutable.Map[String, SortedSet[String]] = new mutable.HashMap[String, SortedSet[String]]()
      val backwardMap: mutable.Map[String, mutable.HashSet[String]] = new mutable.HashMap[String, mutable.HashSet[String]]().withDefault(d => new mutable.HashSet[String]())

      tables.foreach { tn =>
        val keys = databaseMetaData.getPrimaryKeys(null, null, tn)
        val pkSet = new mutable.HashSet[String]()
        while (keys.next()) {
          val colName = keys.getString("COLUMN_NAME")
          if (StringUtils.isNotBlank(colName)) {
            pkSet += colName
          }
        }
        tablePkSet.put(tn, pkSet.to[SortedSet])

        val fkeys = databaseMetaData.getImportedKeys(null, null, tn)
        val fkMap = new mutable.HashMap[String, String]()
        val pkMeta = new mutable.ArrayBuffer[PrimaryKeyMetadata]()
        while (fkeys.next()) {
          val fkTableName = fkeys.getString("FKTABLE_NAME")
          val fkColName = fkeys.getString("FKCOLUMN_NAME")
          val pkTableName = fkeys.getString("PKTABLE_NAME")
          val pkColName = fkeys.getString("PKCOLUMN_NAME")
          if (StringUtils.isNotBlank(fkColName)) {
            fkMap.put(fkColName, pkTableName)
          }
          pkMeta += PrimaryKeyMetadata(fkTableName = fkTableName
            , fkColName = fkColName, pkTableName = pkTableName, pkColName = pkColName)
        }
        tableFkMap.put(tn, fkMap.toMap)
        tablePrimaryKeyMetadataMap.put(tn, pkMeta.toIndexedSeq)

        forwardMap.put(tn, fkMap.values.to[SortedSet])
        val bwMap: Map[String, mutable.HashSet[String]] = fkMap.values.toSet[String].map(tn => tn -> backwardMap(tn)).toMap
        bwMap.foreach(_._2.add(tn))
        backwardMap ++= bwMap
      }

      TableMetadata(tables.to[SortedSet], tablePrimaryKeyMetadataMap.toMap
        , tablePkSet.toMap, tableFkMap.toMap, forwardMap.toMap, backwardMap.mapValues(_.to[SortedSet]).toMap)
    }
  }

  def buildLevels(tableMeta: TableMetadata): Map[String, DimLevel] = {
    val tLevels = new mutable.HashMap[String, DimLevel]
    val workingSet = new mutable.HashSet[String]
    val forwardMap: mutable.Map[String, mutable.HashSet[String]] = new mutable.HashMap[String, mutable.HashSet[String]]().withDefault(d => new mutable.HashSet[String]())
    val backwardMap: mutable.Map[String, mutable.HashSet[String]] = new mutable.HashMap[String, mutable.HashSet[String]]().withDefault(d => new mutable.HashSet[String]())

    tableMeta.forwardMap.foreach {
      case (tn, dset) => forwardMap.put(tn, forwardMap(tn) ++= dset)
    }
    tableMeta.backwardMap.foreach {
      case (tn, dset) => backwardMap.put(tn, backwardMap(tn) ++= dset)
    }

    var level: DimLevel = LevelOne

    workingSet ++= tableMeta.tables

    while (workingSet.nonEmpty) {
      val workingSetMap = workingSet.map(tn => tn -> forwardMap(tn))
      val levelSet = workingSetMap.filter(_._2.isEmpty)
      require(levelSet.nonEmpty, s"Circular dependency in working set: ${workingSetMap}")
      levelSet.foreach {
        case (tn, fSet) =>
          tLevels.put(tn, level)
          val bSet = backwardMap(tn)
          val fSet = bSet.toList.map(forwardMap)
          fSet.foreach(_.remove(tn))
          workingSet.remove(tn)
      }
      level += 1
    }

    tLevels.toMap
  }

  def dump(jdbcConnection: JdbcConnection): Try[SchemaDump] = {
    val tableMetadataTry = buildTableMetadata(jdbcConnection)
    tableMetadataTry.flatMap {
      tm =>
        Try {
          SchemaDump(tm, buildLevels(tm))
        }
    }
  }
}
