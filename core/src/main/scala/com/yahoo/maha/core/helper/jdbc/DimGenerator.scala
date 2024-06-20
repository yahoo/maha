package com.yahoo.maha.core.helper.jdbc

import com.yahoo.maha.core.dimension.{BaseDerivedDimCol, DimCol, Dimension, DimensionAnnotation, DimensionColumn, HivePartDimCol, OraclePartDimCol, PostgresPartDimCol, PrestoPartDimCol, PubCol, PublicDimColumn, PublicDimension}
import com.yahoo.maha.core.request.RequestType
import com.yahoo.maha.core.{ColumnAnnotation, ColumnContext, DataType, DateType, DecType, Engine, FilterOperation, ForcedFilter, ForeignKey, HiveEngine, IntType, OracleEngine, PostgresEngine, PrestoEngine, PrimaryKey, Schema, StrType, TimestampType}
import com.yahoo.maha.core.FilterOperation._

import scala.collection.SortedSet
import scala.collection.mutable.ArrayBuffer
import ColumnGenerator._

case class PublicDimColumnOverrides(filters: Set[FilterOperation] = Set.empty)

case class DimColumnOverrides(partitionColumn: Boolean = false
                              , annotations: List[ColumnAnnotation] = List.empty)

case class DerivedDimColumnDef(dimCol: BaseDerivedDimCol, pubCol: PublicDimColumn)

case class DimTableOverrides(annotations: Set[DimensionAnnotation] = Set.empty
                             , columnOverrides: Map[String, DimColumnOverrides] = Map.empty
                             , pubColumnOverrides: Map[String, PublicDimColumnOverrides] = Map.empty
                             , schemaColMap: Map[Schema, String] = Map.empty
                             , derivedColumns: ColumnContext => IndexedSeq[DerivedDimColumnDef] = _ => IndexedSeq.empty
                             , ignoreColumns: Set[String] = Set.empty
                             , pubColPrefixFromTableName: String => String = tn => {
  if (tn.last == 's') {
    tn.dropRight(1)
  } else tn
}
                             , forcedFilters: Set[ForcedFilter] = Set.empty
                             , revision: Int = 0
                            )

case class DimDefaultConfig(engine: Engine
                            , schemas: SortedSet[Schema]
                            , maxDaysLookBack: Option[Map[RequestType, Int]]
                           )

case class DimConfig(defaults: DimDefaultConfig
                     , tables: SortedSet[String]
                     , tableOverrides: Map[String, DimTableOverrides]
                    )

object DimGenerator {

  def getDimensionColumn(colName: String, typeName: String, pkSet: collection.Set[String]
                         , fkMap: collection.Map[String, String]
                         , columnOverrides: Map[String, DimColumnOverrides]
                         , engine: Engine)(implicit cc: ColumnContext): DimensionColumn = {
    val annotations = new scala.collection.mutable.HashSet[ColumnAnnotation]
    if (pkSet(colName) && fkMap.contains(colName)) {
      annotations.add(PrimaryKey)
      annotations.add(ForeignKey(fkMap(colName)))
    } else if (pkSet(colName)) {
      annotations.add(PrimaryKey)
    } else if (fkMap.contains(colName)) {
      annotations.add(ForeignKey(fkMap(colName)))
    } else {
      //nothing
    }

    if (columnOverrides.get(colName).exists(_.partitionColumn)) {
      engine match {
        case HiveEngine =>
          HivePartDimCol(colName, getColumnType(typeName, colName), annotations = annotations.toSet)
        case OracleEngine =>
          OraclePartDimCol(colName, getColumnType(typeName, colName), annotations = annotations.toSet)
        case PrestoEngine =>
          PrestoPartDimCol(colName, getColumnType(typeName, colName), annotations = annotations.toSet)
        case PostgresEngine =>
          PostgresPartDimCol(colName, getColumnType(typeName, colName), annotations = annotations.toSet)
        case _ =>
          DimCol(colName, getColumnType(typeName, colName), annotations = annotations.toSet)
      }
    } else {
      DimCol(colName, getColumnType(typeName, colName), annotations = annotations.toSet)
    }
  }

  def getPublicDimColumn(tn: String, columnName: String, pkSet: collection.Set[String]
                         , fkMap: collection.Map[String, String]
                         , overrides: Map[String, PublicDimColumnOverrides]
                         , prefixFunction: Option[String => String]
                         , isPublicDimension: Boolean = true
                         , publicNameOverride: Option[String] = None
                        ): PublicDimColumn = {
    val ov = overrides.get(columnName)
    val name: String = if (tn.last == 's') {
      tn.dropRight(1)
    } else tn
    if (pkSet(columnName) || fkMap.contains(columnName)) {
      val publicName = publicNameOverride.getOrElse(getAlias(prefixFunction.map(_(tn)).getOrElse(name), columnName))
      PubCol(columnName, publicName, ov.map(_.filters).getOrElse(InEquality))
    } else {
      val colName = if(isPublicDimension) {
        val prefix = prefixFunction.map(_(tn)).getOrElse(name)
        if(columnName.toLowerCase().startsWith(prefix.toLowerCase())) columnName else s"${prefix}_$columnName"
      } else columnName
      val publicName = publicNameOverride.getOrElse(getAlias(prefixFunction.map(_(tn)).getOrElse(name), colName))
      PubCol(columnName, publicName, ov.map(_.filters).getOrElse(InEquality))
    }
  }

  def buildPublicDimensions(schemaDump: SchemaDump, dimConfig: DimConfig, printDimensions: Boolean): IndexedSeq[PublicDimension] = {
    val tableMetadata = schemaDump.tableMetadata
    val publicDimensions = new ArrayBuffer[PublicDimension](dimConfig.tables.size)
    val tableLevels = schemaDump.tableLevels
    tableLevels.filterKeys(dimConfig.tables).toList.sortBy(_._1).sortBy(_._2.level).foreach {
      case (tn, _) =>
        val overrides = dimConfig.tableOverrides.get(tn)
        val columnOverrides = overrides.map(_.columnOverrides).getOrElse(Map.empty)
        val publicColumnOverrides = overrides.map(_.pubColumnOverrides).getOrElse(Map.empty)
        val pkSet = tableMetadata.pkSet(tn)
        val fkMap = tableMetadata.fkMap(tn)
        val colList = tableMetadata.colMap(tn)
        val columnsBuilt = new ArrayBuffer[DimensionColumn]()
        val publicDimColumnsBuilt = new ArrayBuffer[PublicDimColumn]()
        ColumnContext.withColumnContext {
          implicit cc =>
            colList.foreach {
              columnMetadata =>
                val columnName = columnMetadata.columnName
                val typeName = columnMetadata.typeName
                if (overrides.exists(_.ignoreColumns(columnName))) {
                  //ignore column
                } else {
                  columnsBuilt += getDimensionColumn(columnName, typeName, pkSet, fkMap, columnOverrides, dimConfig.defaults.engine)
                  publicDimColumnsBuilt += getPublicDimColumn(tn, columnName, pkSet, fkMap, publicColumnOverrides, overrides.map(_.pubColPrefixFromTableName))
                }
            }
            overrides.foreach {
              _.derivedColumns(cc).foreach {
                case DerivedDimColumnDef(dimCol, pubCol) =>
                  columnsBuilt += dimCol
                  publicDimColumnsBuilt += pubCol
              }
            }
            val schemaColMap: Map[Schema, String] = overrides.fold(Map.empty[Schema, String])(_.schemaColMap)
            val annotations: Set[DimensionAnnotation] = overrides.fold(Set.empty[DimensionAnnotation])(_.annotations)
            publicDimensions += Dimension.newDimension(
              name = tn
              , engine = dimConfig.defaults.engine
              , dimLevel = tableLevels(tn)
              , schemas = dimConfig.defaults.schemas.toSet
              , columns = columnsBuilt.toSet
              , maxDaysLookBack = dimConfig.defaults.maxDaysLookBack
              , schemaColMap = schemaColMap
              , annotations = annotations
            ).toPublicDimension(
              name = tn
              , grainKey = tn
              , columns = publicDimColumnsBuilt.toSet
              , forcedFilters = overrides.map(_.forcedFilters).getOrElse(Set.empty)
              , revision = overrides.map(_.revision).getOrElse(0)
            )
        }
    }

    if (printDimensions)
      printer.pprintln(publicDimensions)
    publicDimensions
  }
}
