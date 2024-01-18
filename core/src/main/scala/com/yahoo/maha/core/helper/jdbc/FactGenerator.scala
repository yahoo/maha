package com.yahoo.maha.core.helper.jdbc

import com.yahoo.maha.core.{ColumnAnnotation, ColumnContext, DataType, Engine, FilterOperation, ForcedFilter, Grain, Schema}
import com.yahoo.maha.core.request.RequestType
import com.yahoo.maha.core.FilterOperation._

import scala.collection.{SortedSet, mutable}
import scala.collection.mutable.ArrayBuffer
import ColumnGenerator._
import DimGenerator._
import com.yahoo.maha.core.ddl.DDLAnnotation
import com.yahoo.maha.core.dimension.{DimensionColumn, PublicDimColumn}
import com.yahoo.maha.core.fact.Fact.{DEFAULT_CARDINALITY, DEFAULT_COST_MULTIPLIER_MAP, DEFAULT_ROWCOUNT}
import com.yahoo.maha.core.fact.{BaseFactCol, CostMultiplier, Fact, FactAnnotation, FactCol, FactColumn, FkFactMapStorage, ForceFilter, PublicFact, PublicFactCol, PublicFactColumn, RoaringBitmapFkFactMapStorage, RollupExpression, SumRollup}
import com.yahoo.maha.core.lookup.LongRangeLookup
import grizzled.slf4j.Logging

case class PublicFactDefaultConfig(
                                    forcedFilters: Set[ForcedFilter]
                                    , maxDaysWindow: Map[(RequestType, Grain), Int]
                                    , maxDaysLookBack: Map[(RequestType, Grain), Int]
                                    , enableUTCTimeConversion: Boolean = false
                                    , renderLocalTimeFilter: Boolean = false
                                    , revision: Int = 0
                                    , dimRevision: Int = 0
                                    , dimToRevisionMap: Map[String, Int] = Map.empty
                                    , requiredFilterColumns: Map[Schema, Set[String]] = Map.empty
                                    , powerSetStorage: FkFactMapStorage = RoaringBitmapFkFactMapStorage()
                                  )

case class FactDefaultConfig(engine: Engine
                             , schemas: Set[Schema]
                             , dayField: String
                             , grain: Grain
                             , publicDefaults: PublicFactDefaultConfig
                             , annotations: Set[FactAnnotation] = Set.empty
                             , defaultRollupExpression: RollupExpression = SumRollup
                             , defaultPublicFactFilterOperations: Set[FilterOperation] = InBetweenEquality
                             , costMultiplierMap: Map[RequestType, CostMultiplier] = DEFAULT_COST_MULTIPLIER_MAP
                             , forceFilters: Set[ForceFilter] = Set.empty
                             , defaultCardinality: Int = DEFAULT_CARDINALITY
                             , defaultRowCount: Int = DEFAULT_ROWCOUNT
                            )

case class PublicFactColumnOverrides(alias: Option[String]
                                     , filters: Set[FilterOperation] = InBetweenEquality
                                     , dependsOnColumns: Set[String] = Set.empty
                                     , incompatibleColumns: Set[String] = Set.empty
                                     , required: Boolean = false
                                     , hiddenFromJson: Boolean = false
                                     , filteringRequired: Boolean = false
                                     , isImageColumn: Boolean = false
                                     , isReplacement: Boolean = false
                                     , restrictedSchemas: Set[Schema] = Set.empty)

case class FactColumnOverrides(dataType: Option[DataType] = None,
                               rollupExpression: Option[RollupExpression] = None,
                               alias: Option[String] = None,
                               annotations: Set[ColumnAnnotation] = Set.empty,
                               filterOperationOverrides: Set[FilterOperation] = Set.empty)

case class DerivedFactColumnDef(factCol: BaseFactCol, publicFactCol: PublicFactColumn)

case class PublicFactTableOverrides(
                                     forcedFilters: Option[Set[ForcedFilter]] = None
                                     , maxDaysWindow: Option[Map[(RequestType, Grain), Int]] = None
                                     , maxDaysLookBack: Option[Map[(RequestType, Grain), Int]] = None
                                     , enableUTCTimeConversion: Option[Boolean] = None
                                     , renderLocalTimeFilter: Option[Boolean] = None
                                     , revision: Option[Int] = None
                                     , dimRevision: Option[Int] = None
                                     , dimToRevisionMap: Option[Map[String, Int]] = None
                                     , requiredFilterColumns: Option[Map[Schema, Set[String]]] = None
                                     , powerSetStorage: Option[FkFactMapStorage] = None
                                   )

case class FactTableOverrides(annotations: Option[Set[FactAnnotation]] = None
                              , dayField: Option[String] = None
                              , columnOverrides: Map[String, DimColumnOverrides] = Map.empty
                              , factColumnOverrides: Map[String, FactColumnOverrides] = Map.empty
                              , pubColumnOverrides: Map[String, PublicDimColumnOverrides] = Map.empty
                              , pubFactColumnOverrides: Map[String, PublicFactColumnOverrides] = Map.empty
                              , schemaColMap: Map[Schema, String] = Map.empty
                              , derivedDimColumns: ColumnContext => IndexedSeq[DerivedDimColumnDef] = _ => IndexedSeq.empty
                              , derivedFactColumns: ColumnContext => IndexedSeq[DerivedFactColumnDef] = _ => IndexedSeq.empty
                              , ignoreColumns: Set[String] = Set.empty
                              , maxDaysWindow: Option[Map[RequestType, Int]] = None
                              , maxDaysLookBack: Option[Map[RequestType, Int]] = None
                              , grain: Option[Grain] = None
                              , engine: Option[Engine] = None
                              , schemas: Option[Set[Schema]] = None
                              , defaultRollupExpression: Option[RollupExpression] = None
                              , defaultPublicFactFilterOperations: Option[Set[FilterOperation]] = None
                              , ddlAnnotations: Option[DDLAnnotation] = None
                              , costMultiplierMap: Option[Map[RequestType, CostMultiplier]] = None
                              , forceFilters: Option[Set[ForceFilter]] = None
                              , defaultCardinality: Option[Int] = None
                              , defaultRowCount: Option[Int] = None
                              , dimCardinalityLookup: Option[LongRangeLookup[Map[RequestType, Map[Engine, Int]]]] = None
                              , viewBaseTable: Option[String] = None
                              , availableOnwardsDate: Option[String] = None
                              , underlyingTableName: Option[String] = None
                              , publicFactTableOverrides: Option[PublicFactTableOverrides] = None
                              , pubColPrefixFromTableName: String => String = tn => {
  if (tn.last == 's') {
    tn.dropRight(1)
  } else tn
}
                             )

case class CubeName(publicName: String, tableName: String)

object CubeName {
  implicit def ordering: Ordering[CubeName] = Ordering.by(_.publicName)
}

case class FactConfig(defaults: FactDefaultConfig
                      , tables: SortedSet[CubeName]
                      , tableOverrides: Map[String, FactTableOverrides]
                     )

object FactGenerator extends Logging {

  def getFactColumn(colName: String
                    , typeName: String
                    , factConfig: FactConfig
                    , factTableOverrides: Option[FactTableOverrides]
                   )(implicit cc: ColumnContext): FactColumn = {
    val overrides = factTableOverrides.flatMap(_.factColumnOverrides.get(colName))
    val dataType = overrides.flatMap(_.dataType).getOrElse(getColumnType(typeName, colName))
    val rollupExpression = (overrides.flatMap(_.rollupExpression) orElse factTableOverrides.flatMap(_.defaultRollupExpression)).getOrElse(factConfig.defaults.defaultRollupExpression)
    val alias = overrides.flatMap(_.alias)
    val annotations = overrides.map(_.annotations).getOrElse(Set.empty)
    val filterOperationOverrides = overrides.map(_.filterOperationOverrides).getOrElse(Set.empty)
    FactCol(name = colName
      , dataType = dataType
      , rollupExpression = rollupExpression
      , alias = alias
      , annotations = annotations
      , filterOperationOverrides = filterOperationOverrides)
  }

  def getPublicFactColumn(tn: String
                          , columnName: String
                          , factConfig: FactConfig
                          , factTableOverrides: Option[FactTableOverrides]
                         ): PublicFactColumn = {
    val overrides = factTableOverrides.flatMap(_.pubFactColumnOverrides.get(columnName))
    val alias = overrides.flatMap(_.alias).getOrElse(getAlias(factTableOverrides.map(_.pubColPrefixFromTableName(tn)).getOrElse(tn), columnName))
    val filters = (overrides.map(_.filters) orElse factTableOverrides.flatMap(_.defaultPublicFactFilterOperations)).getOrElse(factConfig.defaults.defaultPublicFactFilterOperations)
    val dependsOnColumns = overrides.map(_.dependsOnColumns).getOrElse(Set.empty)
    val incompatibleColumns = overrides.map(_.incompatibleColumns).getOrElse(Set.empty)
    val required = overrides.map(_.required).getOrElse(false)
    val hiddenFromJson = overrides.map(_.hiddenFromJson).getOrElse(false)
    val filteringRequired = overrides.map(_.filteringRequired).getOrElse(false)
    val isImageColumn = overrides.map(_.isImageColumn).getOrElse(false)
    val isReplacement = overrides.map(_.isReplacement).getOrElse(false)
    val restrictedSchemas = overrides.map(_.restrictedSchemas).getOrElse(Set.empty)
    PublicFactCol(name = columnName
      , alias = alias
      , filters = filters
      , dependsOnColumns = dependsOnColumns
      , incompatibleColumns = incompatibleColumns
      , required = required
      , hiddenFromJson = hiddenFromJson
      , filteringRequired = filteringRequired
      , isImageColumn = isImageColumn
      , isReplacement = isReplacement
      , restrictedSchemas = restrictedSchemas
    )
  }

  def buildPublicFacts(schemaDump: SchemaDump, factConfig: FactConfig, printFacts: Boolean): IndexedSeq[PublicFact] = {
    val tableMetadata = schemaDump.tableMetadata
    val publicFacts = new ArrayBuffer[PublicFact](factConfig.tables.size)
    factConfig.tables.toList.sorted.foreach {
      cubeName =>
        val overrides = factConfig.tableOverrides.get(cubeName.publicName)
        val pkSet = tableMetadata.pkSet(cubeName.tableName)
        val fkMap = tableMetadata.fkMap(cubeName.tableName)
        val colList = tableMetadata.colMap(cubeName.tableName)
        val columnsBuilt = new ArrayBuffer[DimensionColumn]()
        val publicColumnsBuilt = new ArrayBuffer[PublicDimColumn]()
        val factColumnsBuilt = new ArrayBuffer[FactColumn]()
        val publicFactColumnsBuilt = new ArrayBuffer[PublicFactColumn]()
        val columnSet = new mutable.HashSet[String]()
        val engine = overrides.flatMap(_.engine).getOrElse(factConfig.defaults.engine)
        val grain = overrides.flatMap(_.grain).getOrElse(factConfig.defaults.grain)
        ColumnContext.withColumnContext {
          implicit cc =>
            val dayField = overrides.flatMap(_.dayField).getOrElse(factConfig.defaults.dayField)
            colList.foreach {
              columnMetadata =>
                try {
                  val columnName = columnMetadata.columnName
                  val typeName = columnMetadata.typeName
                  if (overrides.exists(_.ignoreColumns(columnName))) {
                    //ignore column
                  } else {
                    typeName.toLowerCase() match {
                      case a if (a.startsWith("int") || a.startsWith("float") || a.startsWith("real") || a.startsWith("numeric"))
                        && (!pkSet(columnName) && !fkMap.contains(columnName)) =>
                        factColumnsBuilt += getFactColumn(columnName, typeName, factConfig, overrides)
                        publicFactColumnsBuilt += getPublicFactColumn(cubeName.tableName, columnName, factConfig, overrides)
                      case _ =>
                        columnsBuilt += getDimensionColumn(columnName
                          , typeName, pkSet, fkMap, overrides.map(_.columnOverrides).getOrElse(Map.empty), engine)
                        val publicNameOverride = if(columnName == dayField) Option("Day") else None
                        publicColumnsBuilt += getPublicDimColumn(cubeName.tableName
                          , columnName, pkSet, fkMap, overrides.map(_.pubColumnOverrides).getOrElse(Map.empty), overrides.map(_.pubColPrefixFromTableName), false, publicNameOverride)
                    }
                    columnSet += columnName
                  }
                } catch {
                  case t: Throwable =>
                    error("error on column processing", t)
                    throw t
                }
            }
            overrides.foreach {
              _.derivedDimColumns(cc).foreach {
                case DerivedDimColumnDef(dimCol, pubCol) =>
                  columnsBuilt += dimCol
                  publicColumnsBuilt += pubCol
              }
            }
            overrides.foreach {
              _.derivedFactColumns(cc).foreach {
                case DerivedFactColumnDef(factCol, pubFactCol) =>
                  factColumnsBuilt += factCol
                  publicFactColumnsBuilt += pubFactCol
              }
            }

            //check if we have Day column already defined
            require(cc.getColumnByName(dayField).isDefined, s"Failed to find day field in table : ${factConfig.defaults.dayField}")

            val schemas = overrides.flatMap(_.schemas).getOrElse(factConfig.defaults.schemas)
            val annotations = overrides.flatMap(_.annotations).getOrElse(factConfig.defaults.annotations)
            val costMultiplierMap = overrides.flatMap(_.costMultiplierMap).getOrElse(factConfig.defaults.costMultiplierMap)
            val forceFilters = overrides.flatMap(_.forceFilters).getOrElse(factConfig.defaults.forceFilters)
            val defaultCardinality = overrides.flatMap(_.defaultCardinality).getOrElse(factConfig.defaults.defaultCardinality)
            val defaultRowCount = overrides.flatMap(_.defaultRowCount).getOrElse(factConfig.defaults.defaultRowCount)
            val maxDaysWindow = overrides.flatMap(_.maxDaysWindow).getOrElse(factConfig.defaults.publicDefaults.maxDaysWindow.toList.filter(_._1._2 == grain).map(tpl => tpl._1._1 -> tpl._2).toMap)
            val maxDaysLookBack = overrides.flatMap(_.maxDaysLookBack).getOrElse(factConfig.defaults.publicDefaults.maxDaysLookBack.toList.filter(_._1._2 == grain).map(tpl => tpl._1._1 -> tpl._2).toMap)
            val builder = Fact.newFact(
              name = cubeName.tableName
              , grain = grain
              , engine = engine
              , schemas = schemas
              , dimCols = columnsBuilt.toSet
              , factCols = factColumnsBuilt.toSet
              , annotations = annotations
              , ddlAnnotation = overrides.flatMap(_.ddlAnnotations)
              , costMultiplierMap = costMultiplierMap
              , forceFilters = forceFilters
              , defaultCardinality = defaultCardinality
              , defaultRowCount = defaultRowCount
              , dimCardinalityLookup = overrides.flatMap(_.dimCardinalityLookup)
              , viewBaseTable = overrides.flatMap(_.viewBaseTable)
              , maxDaysWindow = Option(maxDaysWindow)
              , maxDaysLookBack = Option(maxDaysLookBack)
              , availableOnwardsDate = overrides.flatMap(_.availableOnwardsDate)
              , underlyingTableName = overrides.flatMap(_.underlyingTableName)
            )

            val publicForcedFilters = overrides.flatMap(_.publicFactTableOverrides.flatMap(_.forcedFilters)).getOrElse(factConfig.defaults.publicDefaults.forcedFilters)
            val publicMaxDaysWindow = overrides.flatMap(_.publicFactTableOverrides.flatMap(_.maxDaysWindow)).getOrElse(factConfig.defaults.publicDefaults.maxDaysWindow)
            val publicMaxDaysLookBack = overrides.flatMap(_.publicFactTableOverrides.flatMap(_.maxDaysLookBack)).getOrElse(factConfig.defaults.publicDefaults.maxDaysLookBack)
            val enableUTCTimeConversion = overrides.flatMap(_.publicFactTableOverrides.flatMap(_.enableUTCTimeConversion)).getOrElse(factConfig.defaults.publicDefaults.enableUTCTimeConversion)
            val renderLocalTimeFilter = overrides.flatMap(_.publicFactTableOverrides.flatMap(_.renderLocalTimeFilter)).getOrElse(factConfig.defaults.publicDefaults.renderLocalTimeFilter)
            val revision = overrides.flatMap(_.publicFactTableOverrides.flatMap(_.revision)).getOrElse(factConfig.defaults.publicDefaults.revision)
            val dimRevision = overrides.flatMap(_.publicFactTableOverrides.flatMap(_.dimRevision)).getOrElse(factConfig.defaults.publicDefaults.dimRevision)
            val dimToRevisionMap = overrides.flatMap(_.publicFactTableOverrides.flatMap(_.dimToRevisionMap)).getOrElse(factConfig.defaults.publicDefaults.dimToRevisionMap)
            val requiredFilterColumns = overrides.flatMap(_.publicFactTableOverrides.flatMap(_.requiredFilterColumns)).getOrElse(factConfig.defaults.publicDefaults.requiredFilterColumns)
            val powerSetStorage = overrides.flatMap(_.publicFactTableOverrides.flatMap(_.powerSetStorage)).getOrElse(factConfig.defaults.publicDefaults.powerSetStorage)
            val f = builder.toPublicFact(
              name = cubeName.publicName
              , dimCols = publicColumnsBuilt.toSet
              , factCols = publicFactColumnsBuilt.toSet
              , forcedFilters = publicForcedFilters
              , maxDaysWindow = publicMaxDaysWindow
              , maxDaysLookBack = publicMaxDaysLookBack
              , enableUTCTimeConversion = enableUTCTimeConversion
              , renderLocalTimeFilter = renderLocalTimeFilter
              , revision = revision
              , dimRevision = dimRevision
              , dimToRevisionMap = dimToRevisionMap
              , requiredFilterColumns = requiredFilterColumns
              , powerSetStorage = powerSetStorage
            )
            publicFacts += f
        }
    }
    if (printFacts)
      printer.pprintln(publicFacts)
    publicFacts
  }
}
