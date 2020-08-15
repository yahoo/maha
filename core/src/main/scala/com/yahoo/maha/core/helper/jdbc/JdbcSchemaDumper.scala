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

/**
 * Inspired by:
 * https://www.progress.com/blogs/jdbc-tutorial-extracting-database-metadata-via-jdbc-driver
 */
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
    def extractResultSetMetaData: Map[String, ResultSetColumnMetadata] = {
      if (resultSetMetaData == null)
        return Map.empty
      try {
        var count = 1
        val map = new mutable.HashMap[String, ResultSetColumnMetadata]
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
            , ResultSetColumnMetadata(label = label
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
      val rs = databaseMetaData.getTables(null, null, null, Array[String]("TABLE", "VIEW"))
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
      val tableColMetaMap: mutable.Map[String, IndexedSeq[ColumnMetadata]] = new mutable.HashMap[String, IndexedSeq[ColumnMetadata]]()

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

        /**
         * <P>Each column description has the following columns:
         * <OL>
         * <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
         * <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
         * <LI><B>TABLE_NAME</B> String {@code =>} table name
         * <LI><B>COLUMN_NAME</B> String {@code =>} column name
         * <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
         * <LI><B>TYPE_NAME</B> String {@code =>} Data source dependent type name,
         * for a UDT the type name is fully qualified
         * <LI><B>COLUMN_SIZE</B> int {@code =>} column size.
         * <LI><B>BUFFER_LENGTH</B> is not used.
         * <LI><B>DECIMAL_DIGITS</B> int {@code =>} the number of fractional digits. Null is returned for data types where
         * DECIMAL_DIGITS is not applicable.
         * <LI><B>NUM_PREC_RADIX</B> int {@code =>} Radix (typically either 10 or 2)
         * <LI><B>NULLABLE</B> int {@code =>} is NULL allowed.
         * <UL>
         * <LI> columnNoNulls - might not allow <code>NULL</code> values
         * <LI> columnNullable - definitely allows <code>NULL</code> values
         * <LI> columnNullableUnknown - nullability unknown
         * </UL>
         * <LI><B>REMARKS</B> String {@code =>} comment describing column (may be <code>null</code>)
         * <LI><B>COLUMN_DEF</B> String {@code =>} default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be <code>null</code>)
         * <LI><B>SQL_DATA_TYPE</B> int {@code =>} unused
         * <LI><B>SQL_DATETIME_SUB</B> int {@code =>} unused
         * <LI><B>CHAR_OCTET_LENGTH</B> int {@code =>} for char types the
         * maximum number of bytes in the column
         * <LI><B>ORDINAL_POSITION</B> int {@code =>} index of column in table
         * (starting at 1)
         * <LI><B>IS_NULLABLE</B> String  {@code =>} ISO rules are used to determine the nullability for a column.
         * <UL>
         * <LI> YES           --- if the column can include NULLs
         * <LI> NO            --- if the column cannot include NULLs
         * <LI> empty string  --- if the nullability for the
         * column is unknown
         * </UL>
         * <LI><B>SCOPE_CATALOG</B> String {@code =>} catalog of table that is the scope
         * of a reference attribute (<code>null</code> if DATA_TYPE isn't REF)
         * <LI><B>SCOPE_SCHEMA</B> String {@code =>} schema of table that is the scope
         * of a reference attribute (<code>null</code> if the DATA_TYPE isn't REF)
         * <LI><B>SCOPE_TABLE</B> String {@code =>} table name that this the scope
         * of a reference attribute (<code>null</code> if the DATA_TYPE isn't REF)
         * <LI><B>SOURCE_DATA_TYPE</B> short {@code =>} source type of a distinct type or user-generated
         * Ref type, SQL type from java.sql.Types (<code>null</code> if DATA_TYPE
         * isn't DISTINCT or user-generated REF)
         * <LI><B>IS_AUTOINCREMENT</B> String  {@code =>} Indicates whether this column is auto incremented
         * <UL>
         * <LI> YES           --- if the column is auto incremented
         * <LI> NO            --- if the column is not auto incremented
         * <LI> empty string  --- if it cannot be determined whether the column is auto incremented
         * </UL>
         * <LI><B>IS_GENERATEDCOLUMN</B> String  {@code =>} Indicates whether this is a generated column
         * <UL>
         * <LI> YES           --- if this a generated column
         * <LI> NO            --- if this not a generated column
         * <LI> empty string  --- if it cannot be determined whether this is a generated column
         * </UL>
         * </OL>
         */
        val columns = databaseMetaData.getColumns(null, null, tn, null)
        val colList = new mutable.ArrayBuffer[ColumnMetadata]()
        while (columns.next()) {
          val columnName = columns.getString("COLUMN_NAME")
          val typeName = columns.getString("TYPE_NAME")
          val dataTypeInt = columns.getInt("DATA_TYPE")
          val dataType = columnTypeMap.getOrElse(dataTypeInt, s"UNKNOWN type $dataTypeInt")
          val columnSize = columns.getInt("COLUMN_SIZE")
          val decimalDigits = columns.getInt("DECIMAL_DIGITS")
          val isNullable = columns.getString("IS_NULLABLE")
          val isAutoIncrement = columns.getString("IS_AUTOINCREMENT")
          colList += ColumnMetadata(columnName, typeName, dataType, columnSize, decimalDigits, isNullable, isAutoIncrement)
        }
        tableColMetaMap.put(tn, colList)
      }

      TableMetadata(tables.to[SortedSet], tablePrimaryKeyMetadataMap.toMap
        , tablePkSet.toMap, tableFkMap.toMap, forwardMap.toMap, backwardMap.mapValues(_.to[SortedSet]).toMap
        , tableColMetaMap.toMap
      )
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
