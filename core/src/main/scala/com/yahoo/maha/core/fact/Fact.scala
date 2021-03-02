// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.fact

import com.yahoo.maha.core._
import com.yahoo.maha.core.NoopSchema.NoopSchema
import com.yahoo.maha.core.ddl.{DDLAnnotation, HiveDDLAnnotation}
import com.yahoo.maha.core.dimension.{BaseFunctionDimCol, ConstDimCol, DimensionColumn, PublicDimColumn}
import com.yahoo.maha.core.fact.Fact.ViewTable
import com.yahoo.maha.core.lookup.{LongRange, LongRangeLookup}
import com.yahoo.maha.core.request.{AsyncRequest, RequestType, SyncRequest}
import grizzled.slf4j.Logging

import scala.collection.{SortedSet, mutable}
import scala.util.Try
import org.json4s.JsonAST.{JArray, JNull, JObject, JValue}
import org.json4s.scalaz.JsonScalaz._

/**
 * Created by hiral on 10/7/15.
 */

//support depends on condition
//support fact columns which are derived aggregates
//support fact columns which are derived post aggregate
//derived expression as a function

trait FactColumn extends Column {
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : FactColumn
  def rollupExpression: RollupExpression

  val hasRollupWithEngineRequirement: Boolean = rollupExpression.isInstanceOf[EngineRequirement]

  override def asJSON: JObject =
    makeObj(
      List(
        ("FactColumn" -> super.asJSON)
        ,("hasRollupWithEngineRequirement" -> toJSON(hasRollupWithEngineRequirement))
      )
    )
}

trait ConstFactColumn extends FactColumn with ConstColumn

trait DerivedFactColumn extends FactColumn with DerivedColumn

trait ConstDerivedFactColumn extends DerivedFactColumn with ConstColumn

trait PostResultDerivedFactColumn extends FactColumn with DerivedColumn with PostResultColumn {
  override val isDerivedColumn: Boolean = true
}

abstract class BaseFactCol extends FactColumn {
  columnContext.register(this)
}

abstract class BaseConstFactCol extends BaseFactCol with ConstFactColumn

abstract class BaseDerivedFactCol extends BaseFactCol with DerivedFactColumn

abstract class BaseConstDerivedFactCol extends BaseFactCol with ConstDerivedFactColumn

abstract class BasePostResultDerivedFactCol extends BaseFactCol with PostResultDerivedFactColumn

case class FactCol(name: String,
                   dataType: DataType,
                   columnContext: ColumnContext,
                   rollupExpression: RollupExpression,
                   alias: Option[String],
                   annotations: Set[ColumnAnnotation],
                   filterOperationOverrides: Set[FilterOperation]
                   ) extends BaseFactCol {
  override val isDerivedColumn: Boolean = false
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : FactColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias))
    }
  }

  def validate() : Unit = {
    //add validation for custom rollups
    rollupExpression match {
      case HiveCustomRollup(de) =>
        de.sourceColumns.foreach((name: String) => columnContext.render(name, Map.empty))
      case OracleCustomRollup(de) =>
        de.sourceColumns.foreach((name: String) => columnContext.render(name, Map.empty))
      case PostgresCustomRollup(de) =>
        de.sourceColumns.foreach((name: String) => columnContext.render(name, Map.empty))
      case BigqueryCustomRollup(de) =>
        de.sourceColumns.foreach((name: String) => columnContext.render(name, Map.empty))
      case DruidCustomRollup(de) =>
        de.sourceColumns.foreach((name: String) => columnContext.render(name, Map.empty))
      case DruidFilteredRollup(filter, de, delegateAggregatorRollupExpression) =>
        columnContext.render(de.fieldNamePlaceHolder, Map.empty)
        columnContext.render(filter.field, Map.empty)
      case PrestoCustomRollup(de) =>
        de.sourceColumns.foreach((name: String) => columnContext.render(name, Map.empty))
      case DruidFilteredListRollup(filters, de, delegateAggregatorRollupExpression) =>
        columnContext.render(de.fieldNamePlaceHolder, Map.empty)
        for( filter <- filters) {
          columnContext.render(filter.field, Map.empty)
        }
      case DruidThetaSketchRollup =>
      case DruidHyperUniqueRollup(f) =>
      case customRollup: CustomRollup =>
        //error, we missed a check on custom rollup
        throw new IllegalArgumentException(s"Need a check on custom rollup")
      case _ =>
        //normal rollup, do nothing
    }
  }

  private val jUtils = JsonUtils


  override def asJSON: JObject =
    makeObj(
      List(
        ("FactCol" -> super.asJSON)
        ,("name" -> toJSON(name))
        ,("dataType" -> dataType.asJSON)
        ,("rollupExpression" -> rollupExpression.asJSON)
        ,("aliasOrName" -> toJSON(alias.getOrElse("")))
        ,("annotations" -> jUtils.asJSON(annotations))
        ,("filterOperationOverrides" -> jUtils.asJSON(filterOperationOverrides.map(op => op.toString)))
      )
    )
}

object FactCol {
  def apply(name: String,
            dataType: DataType,
            rollupExpression: RollupExpression = SumRollup,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            filterOperationOverrides: Set[FilterOperation] = Set.empty)(implicit cc: ColumnContext) : FactCol = {
    FactCol(name, dataType, cc, rollupExpression, alias, annotations, filterOperationOverrides)
  }
}

case class ConstFactCol(name: String,
                   dataType: DataType,
                   constantValue: String,
                   columnContext: ColumnContext,
                   rollupExpression: RollupExpression,
                   alias: Option[String],
                   annotations: Set[ColumnAnnotation],
                   filterOperationOverrides: Set[FilterOperation]
                    ) extends BaseConstFactCol {
  override val isDerivedColumn: Boolean = false
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : FactColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias))
    }
  }
}

object ConstFactCol {
  def apply(name: String,
            dataType: DataType,
            constantValue: String,
            rollupExpression: RollupExpression = SumRollup,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            filterOperationOverrides: Set[FilterOperation] = Set.empty)(implicit cc: ColumnContext) : ConstFactCol = {
    ConstFactCol(name, dataType, constantValue, cc, rollupExpression, alias, annotations, filterOperationOverrides)
  }
}

case class HiveDerFactCol(name: String,
                          alias: Option[String],
                          dataType: DataType,
                          columnContext: ColumnContext,
                          derivedExpression: HiveDerivedExpression,
                          annotations: Set[ColumnAnnotation],
                          rollupExpression: RollupExpression,
                          filterOperationOverrides: Set[FilterOperation]
                          ) extends BaseDerivedFactCol with WithHiveEngine {
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : FactColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name), derivedExpression = derivedExpression.copyWith(columnContext))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias), derivedExpression = derivedExpression.copyWith(columnContext))
    }
  }
}

object HiveDerFactCol {
  def apply(name: String,
            dataType: DataType,
            derivedExpression: HiveDerivedExpression,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            rollupExpression: RollupExpression = SumRollup,
            filterOperationOverrides: Set[FilterOperation] = Set.empty)(implicit cc: ColumnContext) : HiveDerFactCol = {
    HiveDerFactCol(name, alias, dataType, cc, derivedExpression, annotations, rollupExpression, filterOperationOverrides)
  }
}

case class PrestoDerFactCol(name: String,
                          alias: Option[String],
                          dataType: DataType,
                          columnContext: ColumnContext,
                          derivedExpression: PrestoDerivedExpression,
                          annotations: Set[ColumnAnnotation],
                          rollupExpression: RollupExpression,
                          filterOperationOverrides: Set[FilterOperation]
                         ) extends BaseDerivedFactCol with WithPrestoEngine {
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : FactColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name), derivedExpression = derivedExpression.copyWith(columnContext))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias), derivedExpression = derivedExpression.copyWith(columnContext))
    }
  }
}

object PrestoDerFactCol {
  def apply(name: String,
            dataType: DataType,
            derivedExpression: PrestoDerivedExpression,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            rollupExpression: RollupExpression = SumRollup,
            filterOperationOverrides: Set[FilterOperation] = Set.empty)(implicit cc: ColumnContext) : PrestoDerFactCol = {
    PrestoDerFactCol(name, alias, dataType, cc, derivedExpression, annotations, rollupExpression, filterOperationOverrides)
  }
}

case class OracleDerFactCol(name: String,
                          alias: Option[String],
                          dataType: DataType,
                          columnContext: ColumnContext,
                          derivedExpression: OracleDerivedExpression,
                          annotations: Set[ColumnAnnotation],
                          rollupExpression: RollupExpression,
                          filterOperationOverrides: Set[FilterOperation]
                          ) extends BaseDerivedFactCol with WithOracleEngine {
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : FactColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name), derivedExpression = derivedExpression.copyWith(columnContext))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias), derivedExpression = derivedExpression.copyWith(columnContext))
    }
  }
}

object OracleDerFactCol {
  def apply(name: String,
            dataType: DataType,
            derivedExpression: OracleDerivedExpression,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            rollupExpression: RollupExpression = SumRollup,
            filterOperationOverrides: Set[FilterOperation] = Set.empty)(implicit cc: ColumnContext) : OracleDerFactCol = {
    OracleDerFactCol(name, alias, dataType, cc, derivedExpression, annotations, rollupExpression, filterOperationOverrides)
  }
}

case class PostgresDerFactCol(name: String,
                              alias: Option[String],
                              dataType: DataType,
                              columnContext: ColumnContext,
                              derivedExpression: PostgresDerivedExpression,
                              annotations: Set[ColumnAnnotation],
                              rollupExpression: RollupExpression,
                              filterOperationOverrides: Set[FilterOperation]
                             ) extends BaseDerivedFactCol with WithPostgresEngine {
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : FactColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name), derivedExpression = derivedExpression.copyWith(columnContext))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias), derivedExpression = derivedExpression.copyWith(columnContext))
    }
  }
}

object PostgresDerFactCol {
  def apply(name: String,
            dataType: DataType,
            derivedExpression: PostgresDerivedExpression,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            rollupExpression: RollupExpression = SumRollup,
            filterOperationOverrides: Set[FilterOperation] = Set.empty)(implicit cc: ColumnContext) : PostgresDerFactCol = {
    PostgresDerFactCol(name, alias, dataType, cc, derivedExpression, annotations, rollupExpression, filterOperationOverrides)
  }
}

case class BigqueryDerFactCol(name: String,
                              alias: Option[String],
                              dataType: DataType,
                              columnContext: ColumnContext,
                              derivedExpression: BigqueryDerivedExpression,
                              annotations: Set[ColumnAnnotation],
                              rollupExpression: RollupExpression,
                              filterOperationOverrides: Set[FilterOperation]
                             ) extends BaseDerivedFactCol with WithBigqueryEngine {
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean): FactColumn = {
    if (resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name), derivedExpression = derivedExpression.copyWith(columnContext))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias), derivedExpression = derivedExpression.copyWith(columnContext))
    }
  }
}

object BigqueryDerFactCol {
  def apply(name: String,
            dataType: DataType,
            derivedExpression: BigqueryDerivedExpression,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            rollupExpression: RollupExpression = SumRollup,
            filterOperationOverrides: Set[FilterOperation] = Set.empty)(implicit cc: ColumnContext): BigqueryDerFactCol = {
    BigqueryDerFactCol(name, alias, dataType, cc, derivedExpression, annotations, rollupExpression, filterOperationOverrides)
  }
}

case class DruidDerFactCol(name: String,
                           alias: Option[String],
                           dataType: DataType,
                           columnContext: ColumnContext,
                           derivedExpression: DruidDerivedExpression,
                           annotations: Set[ColumnAnnotation],
                           rollupExpression: RollupExpression,
                           filterOperationOverrides: Set[FilterOperation]
                           ) extends BaseDerivedFactCol with WithDruidEngine {
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : FactColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name), derivedExpression = derivedExpression.copyWith(columnContext))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias), derivedExpression = derivedExpression.copyWith(columnContext))
    }
  }
}

object DruidDerFactCol {
  def apply(name: String,
            dataType: DataType,
            derivedExpression: DruidDerivedExpression,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            rollupExpression: RollupExpression = SumRollup,
            filterOperationOverrides: Set[FilterOperation] = Set.empty)(implicit cc: ColumnContext) :  DruidDerFactCol = {
    DruidDerFactCol(name, alias, dataType, cc, derivedExpression, annotations, rollupExpression, filterOperationOverrides)
  }
}

case class DruidConstDerFactCol(name: String,
                           alias: Option[String],
                           dataType: DataType,
                           constantValue: String,
                           columnContext: ColumnContext,
                           derivedExpression: DruidDerivedExpression,
                           annotations: Set[ColumnAnnotation],
                           rollupExpression: RollupExpression,
                           filterOperationOverrides: Set[FilterOperation]
                          ) extends BaseConstDerivedFactCol with WithDruidEngine {
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : FactColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name), derivedExpression = derivedExpression.copyWith(columnContext))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias), derivedExpression = derivedExpression.copyWith(columnContext))
    }
  }
}

object DruidConstDerFactCol {
  def apply(name: String,
            dataType: DataType,
            derivedExpression: DruidDerivedExpression,
            constantValue: String,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            rollupExpression: RollupExpression = SumRollup,
            filterOperationOverrides: Set[FilterOperation] = Set.empty)(implicit cc: ColumnContext) :  DruidConstDerFactCol = {
    DruidConstDerFactCol(name, alias, dataType, constantValue, cc, derivedExpression, annotations, rollupExpression, filterOperationOverrides)
  }
}

case class DruidPostResultDerivedFactCol(name: String,
                           alias: Option[String],
                           dataType: DataType,
                           columnContext: ColumnContext,
                           derivedExpression: DruidDerivedExpression,
                           annotations: Set[ColumnAnnotation],
                           rollupExpression: RollupExpression,
                           filterOperationOverrides: Set[FilterOperation],
                           postResultFunction: DruidPostResultFunction
                          ) extends BasePostResultDerivedFactCol with WithDruidEngine {
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : FactColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name), derivedExpression = derivedExpression.copyWith(columnContext))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias), derivedExpression = derivedExpression.copyWith(columnContext))
    }
  }

  def validate(): Unit = {
    postResultFunction.validate()
  }
}

object DruidPostResultDerivedFactCol {
  def apply(name: String,
            dataType: DataType,
            derivedExpression: DruidDerivedExpression,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            rollupExpression: RollupExpression = SumRollup,
            filterOperationOverrides: Set[FilterOperation] = Set.empty,
            postResultFunction: DruidPostResultFunction)(implicit cc: ColumnContext) :  DruidPostResultDerivedFactCol = {
    DruidPostResultDerivedFactCol(name, alias, dataType, cc, derivedExpression, annotations, rollupExpression, filterOperationOverrides, postResultFunction)
  }
}

case class CostMultiplier(rows: LongRangeLookup[BigDecimal])
object CostMultiplier {
  val default : CostMultiplier = CostMultiplier(
    LongRangeLookup(IndexedSeq(LongRange(0, Long.MaxValue) -> 1))
  )
}

case class ForceFilter(filter: ForcedFilter) {
  require(filter.isForceFilter, "Filter must be declared with isForceFilter = true")
  def isOverridable: Boolean = filter.isOverridable
}

trait Fact extends BaseTable {
  def level: Int
  def grain: Grain
  def engine: Engine
  def name: String
  def dimCols: Set[DimensionColumn]
  def factCols: Set[FactColumn]
  def annotations: Set[FactAnnotation]
  def factConditionalHints: SortedSet[FactConditionalHint]
  def queryConditions: Set[QueryCondition]
  def ddlAnnotation: Option[DDLAnnotation]
  def from : Option[Fact]
  def fields: Set[String]
  def costMultiplierMap: Map[RequestType, CostMultiplier]
  def dimColMap : Map[String, DimensionColumn]
  def factColMap : Map[String, FactColumn]
  def publicDimToForeignKeyMap : Map[String, String]
  def publicDimToForeignKeyColMap: Map[String, Column]
  def defaultCardinality: Int
  def defaultRowCount: Int
  def forceFilters: Set[ForceFilter]
  def postValidate(publicFact: PublicFact): Unit
  def partitionCols: SortedSet[PartitionColumn]
  def viewBaseTable : Option[String]
  def maxDaysWindow: Option[Map[RequestType, Int]]
  def maxDaysLookBack: Option[Map[RequestType, Int]]
  def availableOnwardsDate: Option[String] // Local Date
  def underlyingTableName: Option[String]
}

trait FactView extends Fact {
  def constantColNameToValueMap: Map[String, String]
  val factConditionalHints: SortedSet[FactConditionalHint] = annotations
    .filter(_.isInstanceOf[FactConditionalHint])
    .map(_.asInstanceOf[FactConditionalHint]).to[SortedSet]
  val queryConditions: Set[QueryCondition] = factConditionalHints.flatMap(_.conditions).toSet
}

object Fact {
  implicit val ordering: Ordering[Fact] = Ordering.by(f => s"${f.level}-${f.grain.level}-${f.name}")
  val DEFAULT_CARDINALITY = 10
  val DEFAULT_ROWCOUNT=100
  val DEFAULT_COST_MULTIPLIER_MAP: Map[RequestType, CostMultiplier] = Map(SyncRequest -> CostMultiplier.default, AsyncRequest -> CostMultiplier.default)
  def newFact(
    name: String,
    grain: Grain,
    engine: Engine,
    schemas: Set[Schema],
    dimCols: Set[DimensionColumn],
    factCols: Set[FactColumn],
    annotations: Set[FactAnnotation] = Set.empty,
    ddlAnnotation: Option[DDLAnnotation] = None,
    costMultiplierMap: Map[RequestType, CostMultiplier] = DEFAULT_COST_MULTIPLIER_MAP,
    forceFilters: Set[ForceFilter] = Set.empty,
    defaultCardinality: Int = DEFAULT_CARDINALITY,
    defaultRowCount: Int = DEFAULT_ROWCOUNT,
    dimCardinalityLookup : Option[LongRangeLookup[Map[RequestType,Map[Engine, Int]]]] = None,
    viewBaseTable : Option[String] = None,
    maxDaysWindow: Option[Map[RequestType, Int]] = None,
    maxDaysLookBack: Option[Map[RequestType, Int]] = None,
    availableOnwardsDate: Option[String] = None, //Local Date,\
    underlyingTableName: Option[String] = None
    ): FactBuilder = {
    val baseFact = new FactTable(name, 9999, grain, engine, schemas, dimCols, factCols, None, annotations, ddlAnnotation, costMultiplierMap, forceFilters, defaultCardinality, defaultRowCount, viewBaseTable, maxDaysWindow, maxDaysLookBack, availableOnwardsDate, underlyingTableName)
    val tableMap = Map(baseFact.name -> baseFact)
    FactBuilder(baseFact, tableMap, dimCardinalityLookup)
  }

  case class ViewTable private[fact](view: View
                                     , level: Int
                                     , grain: Grain
                                     , engine: Engine
                                     , schemas: Set[Schema]
                                     , dimCols: Set[DimensionColumn]
                                     , factCols: Set[FactColumn]
                                     , from: Option[Fact]
                                     , annotations: Set[FactAnnotation]
                                     , ddlAnnotation: Option[DDLAnnotation]
                                     , costMultiplierMap: Map[RequestType, CostMultiplier]
                                     , forceFilters: Set[ForceFilter]
                                     , defaultCardinality: Int
                                     , defaultRowCount: Int
                                     , viewBaseTable : Option[String]
                                     , maxDaysWindow: Option[Map[RequestType, Int]]
                                     , maxDaysLookBack: Option[Map[RequestType, Int]]
                                     , availableOnwardsDate: Option[String]
                                     , underlyingTableName: Option[String]
                                      ) extends FactView {

    View.validateView(view, dimCols, factCols)

    val name = view.viewName
    val fields : Set[String] = dimCols.map(_.name) ++ factCols.map(_.name)
    val dimColMap : Map[String, DimensionColumn] = dimCols.map(c => (c.name, c)).toMap
    val factColMap : Map[String, FactColumn] = factCols.map(c => (c.name, c)).toMap
    val columnsByNameMap : Map[String, Column] = dimColMap ++ factColMap
    val constantColNameToValueMap: Map[String, String] = {
      val dimColMap = dimCols.filter(_.isInstanceOf[ConstDimCol]).map(d=> d.name -> d.asInstanceOf[ConstDimCol].constantValue).toMap
      val factColMap = factCols.filter(_.isInstanceOf[ConstFactCol]).map(d=> d.name -> d.asInstanceOf[ConstFactCol].constantValue).toMap
      dimColMap++factColMap
    }

    val publicDimToForeignKeyMap : Map[String, String] =
      dimCols
        .filter(_.isForeignKey)
        .map(col => col.getForeignKeySource.get -> col.name)
        .toMap

    val publicDimToForeignKeyColMap : Map[String, Column] =
      dimCols
        .filter(_.isForeignKey)
        .map(col => col.getForeignKeySource.get -> col)
        .toMap

    val partitionCols : SortedSet[PartitionColumn] = {
      dimCols.filter(_.isInstanceOf[PartitionColumn]).map(c=> c.asInstanceOf[PartitionColumn]).to[SortedSet]
    }

    def postValidate(pubFact : PublicFact): Unit = {

    }
  }


  def newUnionView(view: View,
                      grain: Grain,
                      engine: Engine,
                      schemas: Set[Schema],
                      dimCols: Set[DimensionColumn],
                      factCols: Set[FactColumn],
                      annotations: Set[FactAnnotation] = Set.empty,
                      ddlAnnotation: Option[DDLAnnotation] = None,
                      costMultiplierMap: Map[RequestType, CostMultiplier] = DEFAULT_COST_MULTIPLIER_MAP,
                      forceFilters: Set[ForceFilter] = Set.empty,
                      defaultCardinality: Int = DEFAULT_CARDINALITY,
                      defaultRowCount: Int = DEFAULT_ROWCOUNT,
                      dimCardinalityLookup : Option[LongRangeLookup[Map[RequestType,Map[Engine, Int]]]] = None,
                      viewBaseTable : Option[String] = None,
                      maxDaysWindow: Option[Map[RequestType, Int]] = None,
                      maxDaysLookBack: Option[Map[RequestType, Int]] = None,
                      availableOnwardsDate: Option[String] = None,
                      underlyingTableName: Option[String] = None) : FactBuilder = {
    val baseView = new ViewTable(view, 9999, grain, engine, schemas, dimCols, factCols, None, annotations, ddlAnnotation, costMultiplierMap,forceFilters,defaultCardinality,defaultRowCount, viewBaseTable, maxDaysWindow, maxDaysLookBack, availableOnwardsDate, underlyingTableName)
    val tableMap = Map(view.viewName -> baseView)
    FactBuilder(baseView, tableMap, dimCardinalityLookup)
  }

  def newFactForView(
               name: String,
               grain: Grain,
               engine: Engine,
               schemas: Set[Schema],
               dimCols: Set[DimensionColumn],
               factCols: Set[FactColumn],
               annotations: Set[FactAnnotation] = Set.empty,
               forceFilters: Set[ForceFilter] = Set.empty,
               costMultiplierMap: Map[RequestType, CostMultiplier] = DEFAULT_COST_MULTIPLIER_MAP): ViewBaseTable = {
    new ViewBaseTable(name, 9999, grain, engine, schemas, dimCols, factCols, None, annotations, None, costMultiplierMap, forceFilters,DEFAULT_CARDINALITY,DEFAULT_ROWCOUNT, None, None, None, None, None)
  }
}

case class FactTable private[fact](name: String
                                   , level: Int
                                   , grain: Grain
                                   , engine: Engine
                                   , schemas: Set[Schema]
                                   , dimCols: Set[DimensionColumn]
                                   , factCols: Set[FactColumn]
                                   , from: Option[Fact]
                                   , annotations: Set[FactAnnotation]
                                   , ddlAnnotation: Option[DDLAnnotation]
                                   , costMultiplierMap: Map[RequestType, CostMultiplier]
                                   , forceFilters: Set[ForceFilter]
                                   , defaultCardinality: Int
                                   , defaultRowCount: Int
                                   , viewBaseTable : Option[String]
                                   , maxDaysWindow: Option[Map[RequestType, Int]]
                                   , maxDaysLookBack: Option[Map[RequestType, Int]]
                                   , availableOnwardsDate: Option[String]
                                   , underlyingTableName: Option[String]
                                    ) extends Fact {
  validate()

  val fields : Set[String] = dimCols.map(_.name) ++ factCols.map(_.name)
  val dimColMap : Map[String, DimensionColumn] = dimCols.map(c => (c.name, c)).toMap
  val factColMap : Map[String, FactColumn] = factCols.map(c => (c.name, c)).toMap
  val columnsByNameMap : Map[String, Column] = dimColMap ++ factColMap

  val publicDimToForeignKeyMap : Map[String, String] =
    dimCols
      .filter(_.isForeignKey)
      .map(col => col.getForeignKeySource.get -> col.name)
      .toMap

  val publicDimToForeignKeyColMap : Map[String, Column] =
    dimCols
      .filter(_.isForeignKey)
      .map(col => col.getForeignKeySource.get -> col)
      .toMap

  val partitionCols : SortedSet[PartitionColumn] = {
    dimCols.filter(_.isInstanceOf[PartitionColumn]).map(c=> c.asInstanceOf[PartitionColumn]).to[SortedSet]
  }

  val factConditionalHints: SortedSet[FactConditionalHint] = annotations
    .filter(_.isInstanceOf[FactConditionalHint])
    .map(_.asInstanceOf[FactConditionalHint]).to[SortedSet]
  val queryConditions: Set[QueryCondition] = factConditionalHints.flatMap(_.conditions).toSet

  private[this] def validate() : Unit = {
    schemaRequired()
    engineValidations(dimCols)
    engineValidations(factCols)
    ddlAnnotationValidations(ddlAnnotation)
    validateDerivedExpression(dimCols)
    validateDerivedExpression(factCols ++ dimCols)
    validateForeignKeyCols(factCols)
    ColumnContext.validateColumnContext(dimCols ++ factCols, s"fact=$name")
    validateFactCols(factCols)
    validateDerivedFunction(dimCols)
    validatePostResultFunction(factCols)
    validateAvailableOnwardsDate()
  }

  private[this] def ddlAnnotationValidations(ddLAnnotation: Option[DDLAnnotation]) : Unit = {
    if (ddlAnnotation.isDefined) {
      require(ddlAnnotation.get.asInstanceOf[EngineRequirement].acceptEngine(engine),
        s"Failed engine requirement fact=$name, engine=$engine, ddlAnnotation=$ddlAnnotation")
      engine match {
              case HiveEngine =>
                val da = ddlAnnotation.get.asInstanceOf[HiveDDLAnnotation]
                val columnOrderingSet = da.columnOrdering.toSet
                dimCols.filterNot(_.isDerivedColumn).filterNot(_.isInstanceOf[PartitionColumn]).foreach {
                  c =>
                    val nameOrAlias = c.alias.getOrElse(c.name)
                    require(columnOrderingSet.contains(nameOrAlias) || columnOrderingSet.contains(c.name) , s"Dimension Column: ${c.name} is not present in column order set : nameOrAlias = $nameOrAlias , fact = $name")
                }
                factCols.filterNot(_.isDerivedColumn).foreach {
                  c =>
                    val nameOrAlias = c.alias.getOrElse(c.name)
                    require(columnOrderingSet(nameOrAlias) || columnOrderingSet.contains(c.name), s"Fact column: ${c.name} is not present in column order set : nameOrAlias = $nameOrAlias , fact = $name")
                }
              case any => /* Do nothing */
            }
    }
  }

  private[this] def schemaRequired(): Unit = {
    require(schemas.nonEmpty, s"Failed schema requirement fact=$name, engine=$engine, schemas=$schemas")
  }

  //validation for engine
  private[this] def engineValidations[T <: Column](columns: Set[T]): Unit ={
    columns.foreach { c =>
      c.annotations.filter(_.isInstanceOf[EngineRequirement]).map { er =>
        require(er.asInstanceOf[EngineRequirement].acceptEngine(engine),
          s"Failed engine requirement fact=$name, engine=$engine, col=${c.name}, annotation=$er")
      }
      if(c.isInstanceOf[EngineRequirement]) {
        require(c.asInstanceOf[EngineRequirement].acceptEngine(engine),
          s"Failed engine requirement fact=$name, engine=$engine, col=$c")
      }
    }
    factCols.foreach { fc =>
      if(fc.hasRollupWithEngineRequirement) {
        require(fc.rollupExpression.asInstanceOf[EngineRequirement].acceptEngine(engine),
          s"Failed engine requirement fact=$name, engine=$engine, col=${fc.name}, rollup=${fc.rollupExpression}")
      }
    }
    annotations.filter(_.isInstanceOf[EngineRequirement]).map { er =>
      require(er.asInstanceOf[EngineRequirement].acceptEngine(engine),
        s"Failed engine requirement fact=$name, engine=$engine, annotation=$er")
    }
  }

  //validation for derived expression columns
  private[this] def validateDerivedExpression[T <: Column](columns: Set[T]): Unit = {
    val columnNames : Set[String] = columns.map(_.name)
    columns.view.filter(_.isInstanceOf[DerivedColumn]).map(_.asInstanceOf[DerivedColumn]).foreach { c =>
      require(c.derivedExpression != null,
        s"Derived expression should be defined for a derived column $c")
      c.derivedExpression.sourceColumns.foreach { cn =>
        require(columnNames.contains(cn),
          s"Failed derived expression validation, unknown referenced column in fact=$name, $cn in $c")
        require(cn != c.name,
          s"Derived column is referring to itself, this will cause infinite loop : fact=$name, column=$cn, sourceColumns=${c.derivedExpression.sourceColumns}")
      }
      //validate we can render it
      c.derivedExpression.render(c.name)
    }
  }

  private[this] def validateForeignKeyCols[T <: Column](columns: Set[T]) : Unit = {
    columns.foreach {
      col =>
        if(col.annotations.exists(_.isInstanceOf[ForeignKey])) {
          require(col.isInstanceOf[DimensionColumn], s"Non dimension column cannot have foreign key annotation : $col")
        }
    }
  }

  private[this] def validateFactCols[T <: Column](columns: Set[T]): Unit = {
    columns.view.filter(_.isInstanceOf[FactCol]).map(_.asInstanceOf[FactCol]).foreach { c =>
      //validate we can render it
      c.validate()
    }
  }

  private[this] def validateDerivedFunction[T <: Column](columns: Set[T]): Unit = {
    columns.view.filter(_.isInstanceOf[BaseFunctionDimCol]).map(_.asInstanceOf[BaseFunctionDimCol]).foreach { c =>
      //validate we can render it
      c.validate()
    }
  }

  private[this] def validatePostResultFunction[T <: Column](columns: Set[T]): Unit = {
    columns.view.filter(_.isInstanceOf[BasePostResultDerivedFactCol]).map(_.asInstanceOf[BasePostResultDerivedFactCol]).foreach { c =>
      c.validate()
    }
  }

  private[this] def validateAvailableOnwardsDate(): Unit = {
    if(availableOnwardsDate.isDefined) {
      DailyGrain.validateFormat("availableOnwardsDate, (has to be Local Date)", availableOnwardsDate.get)
    }
  }

  private[this] def validateForceFilters(filters: Set[ForceFilter], publicFact: PublicFact): Unit = {
    //validate filter is valid
    publicFact.aliasToNameColumnMap
    filters.foreach { f =>
      val alias = f.filter.field
      require(publicFact.aliasToNameColumnMap.contains(alias), s"Invalid fact forced filter : alias=$alias , fact=$name, publicFact=${publicFact.name}")
      val nameFromAlias = publicFact.aliasToNameColumnMap(alias)
      require(fields(nameFromAlias), s"Invalid fact forced filter : alias=$alias, name=$name , $name does not exist in fact=$name")
      val pc = publicFact.columnsByAliasMap(alias)
      require(pc.filters(f.filter.operator), s"Invalid fact forced filter operation on alias=$alias , operations supported : ${pc.filters} operation not supported : ${f.filter.operator}")
    }
  }

  def validateForceFilterUniqueness(publicFact : PublicFact): Unit = {
    val baseNameAliasMap: Map[String, String] = { //If this column has a declared Option alias, use it
      val factAliasMap: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
      columnsByNameMap.map{
        case (name, col)  => {
          if (col.alias != None) {
            factAliasMap += (name -> col.alias.map(_.toString).getOrElse(""))
          }
          else {
            factAliasMap += (name -> name)
          }}}
      factAliasMap.toMap
    }
    val forcedFiltersByBasenameMap: Map[String, Filter] = publicFact.forcedFilters.map{
      val backwardColByAlias: Map[String, String] =
        publicFact.dimCols.map(col => col.alias -> col.name).toMap ++
          publicFact.factCols.map(col => col.alias -> col.name).toMap
      forceFilter =>
        val wrappedField = forceFilter.field
        if(backwardColByAlias.contains(wrappedField) && baseNameAliasMap.contains(backwardColByAlias(wrappedField))){
          baseNameAliasMap(backwardColByAlias(wrappedField)) -> forceFilter
        }
        else{
          throw new IllegalStateException(s"requirement failed: ${forceFilter.field} doesn't have a known alias")
        }
    }.toMap

    require(forcedFiltersByBasenameMap.size == publicFact.forcedFilters.size, "Forced Filters public fact and map of forced base cols differ in size")

  }

  def postValidate(publicFact: PublicFact) : Unit = {
    validateForceFilters(forceFilters, publicFact)
    validateForceFilterUniqueness(publicFact)
  }
}

case class FactBuilder private[fact](private val baseFact: Fact, private var tableMap: Map[String, Fact], private val dimCardinalityLookup: Option[LongRangeLookup[Map[RequestType, Map[Engine, Int]]]]) {

  private[this] def baseValidate(from: String, to: String) = {
    require(from != to, "from should not be equal to to")
    require(tableMap.contains(from), "from table does not exist")
    require(!tableMap.contains(to), "to table should not exist")
  }

  private[this] def dateValidate(to: String, date: String, engine: Engine): Unit = {
    tableMap.foreach{ case (name, fact) =>
      if(name != to) {
        //either the base table has no availableOnwardsDate, the current engine is different from the new one, or (if both engines the same) different dates
        require(!fact.availableOnwardsDate.isDefined || engine != fact.engine || date != fact.availableOnwardsDate.get, s"Base date $date in fact $to is already defined in $name")
      }
    }
  }

  private[this] def getUpdatedDiscardingSet(discarding: Set[String], cols: Iterable[Column]): Set[String] = {
    val mutableDiscardingSet: mutable.Set[String] = new mutable.HashSet[String]()
    discarding.foreach(mutableDiscardingSet.add)
    var hasAdditionalDiscards = false
    do {
      hasAdditionalDiscards = false
      cols
        .filterNot(col => mutableDiscardingSet.contains(col.name))
        .filter(col => (col.isDerivedColumn
          && col.asInstanceOf[DerivedColumn].derivedExpression.sourceColumns.intersect(mutableDiscardingSet).nonEmpty))
        .foreach { col =>
          mutableDiscardingSet += col.name
          hasAdditionalDiscards = true
        }
    } while (hasAdditionalDiscards)
    mutableDiscardingSet.toSet
  }

  def withNewGrain(name: String
                   , from: String
                   , grain: Grain
                   , columnAliasMap : Map[String, String] = Map.empty
                   , forceFilters: Set[ForceFilter] = Set.empty
                   , resetAliasIfNotPresent: Boolean = false
                   , availableOnwardsDate : Option[String] = None
                   , underlyingTableName: Option[String] = None) : FactBuilder= {
    baseValidate(from, name)
    ColumnContext.withColumnContext { columnContext =>
      val fromTable = tableMap.get(from).get
      require(fromTable.grain != grain, s"grain is already the same! from: ${fromTable.grain}, to: $grain")

      val dimCols = fromTable.dimCols.map(d => d.copyWith(columnContext, columnAliasMap, resetAliasIfNotPresent))
      val factCols = fromTable.factCols.map(f => f.copyWith(columnContext, columnAliasMap, resetAliasIfNotPresent))

      val newForceFilters = if(forceFilters.isEmpty) fromTable.forceFilters else forceFilters
      tableMap += name -> new FactTable(
        name
        , fromTable.level
        , grain
        , fromTable.engine
        , fromTable.schemas
        , dimCols
        , factCols
        , Option(fromTable)
        , fromTable.annotations
        , fromTable.ddlAnnotation
        , fromTable.costMultiplierMap
        , newForceFilters
        , fromTable.defaultCardinality
        , fromTable.defaultRowCount
        , fromTable.viewBaseTable
        , fromTable.maxDaysWindow
        , fromTable.maxDaysLookBack
        , availableOnwardsDate
        , underlyingTableName
      )
      this
    }
  }

  def withNewSchema(name: String
                    , from: String
                    , schemas: Set[Schema]
                    , columnAliasMap : Map[String, String] = Map.empty
                    , forceFilters: Set[ForceFilter] = Set.empty
                    , resetAliasIfNotPresent: Boolean = false
                    , availableOnwardsDate : Option[String] = None
                    , underlyingTableName: Option[String] = None) : FactBuilder = {
    baseValidate(from, name)
    ColumnContext.withColumnContext { columnContext =>
      val fromTable = tableMap.get(from).get
      require(fromTable.schemas != schemas, s"schemas are the same! from: ${fromTable.schemas}, to: $schemas")

      val dimCols = fromTable.dimCols.map(d => d.copyWith(columnContext, columnAliasMap, resetAliasIfNotPresent))
      val factCols = fromTable.factCols.map(f => f.copyWith(columnContext, columnAliasMap, resetAliasIfNotPresent))

      val newForceFilters = if(forceFilters.isEmpty) fromTable.forceFilters else forceFilters
      tableMap += name -> new FactTable(
        name
        , fromTable.level
        , fromTable.grain
        , fromTable.engine
        , schemas
        , dimCols
        , factCols
        , Option(fromTable)
        , fromTable.annotations
        , fromTable.ddlAnnotation
        , fromTable.costMultiplierMap
        , newForceFilters
        , fromTable.defaultCardinality
        , fromTable.defaultRowCount
        , fromTable.viewBaseTable
        , fromTable.maxDaysWindow
        , fromTable.maxDaysLookBack
        , availableOnwardsDate
        , underlyingTableName
      )
      this
    }
  }

  def withNewSchemaAndGrain(name: String
                            , from: String
                            , schemas: Set[Schema]
                            , grain: Grain
                            , columnAliasMap : Map[String, String] = Map.empty
                            , forceFilters: Set[ForceFilter] = Set.empty
                            , resetAliasIfNotPresent: Boolean = false
                            , availableOnwardsDate : Option[String] = None
                            , underlyingTableName: Option[String] = None) : FactBuilder = {
    baseValidate(from, name)
    ColumnContext.withColumnContext { columnContext =>
      val fromTable = tableMap.get(from).get
      require(fromTable.schemas != schemas, s"schemas are the same! from: ${fromTable.schemas}, to: $schemas")
      require(fromTable.grain != grain, s"grain is already the same! from: ${fromTable.grain}, to: $grain")

      val dimCols = fromTable.dimCols.map(d => d.copyWith(columnContext, columnAliasMap, resetAliasIfNotPresent))
      val factCols = fromTable.factCols.map(f => f.copyWith(columnContext, columnAliasMap, resetAliasIfNotPresent))

      val newForceFilters = if(forceFilters.isEmpty) fromTable.forceFilters else forceFilters
      tableMap += name -> new FactTable(
        name
        , fromTable.level
        , grain
        , fromTable.engine
        , schemas
        , dimCols
        , factCols
        , Option(fromTable)
        , fromTable.annotations
        , fromTable.ddlAnnotation
        , fromTable.costMultiplierMap
        , newForceFilters
        , fromTable.defaultCardinality
        , fromTable.defaultRowCount
        , fromTable.viewBaseTable
        , fromTable.maxDaysWindow
        , fromTable.maxDaysLookBack
        , availableOnwardsDate
        , underlyingTableName
      )
      this
    }
  }

  def withAlternativeEngine(name: String
                            , from: String
                            , engine: Engine
                            , overrideDimCols: Set[DimensionColumn] = Set.empty
                            , overrideFactCols: Set[FactColumn] = Set.empty
                            , schemas: Option[Set[Schema]] = None
                            , overrideAnnotations: Set[FactAnnotation] = Set.empty
                            , overrideDDLAnnotation: Option[DDLAnnotation] = None
                            , costMultiplierMap: Map[RequestType, CostMultiplier] = Fact.DEFAULT_COST_MULTIPLIER_MAP
                            , columnAliasMap : Map[String, String] = Map.empty
                            , forceFilters: Set[ForceFilter] = Set.empty
                            , grain: Option[Grain] = None
                            , defaultCardinality:Int = 10
                            , defaultRowCount:Int = 100
                            , maxDaysWindow: Option[Map[RequestType, Int]] = None
                            , maxDaysLookBack: Option[Map[RequestType, Int]] = None
                            , availableOnwardsDate : Option[String] = None
                            , underlyingTableName: Option[String] = None
                            , discarding: Set[String] = Set.empty)(implicit cc: ColumnContext) : FactBuilder = {

    require(!tableMap.contains(name), "should not export with existing table name")
    require(tableMap.nonEmpty, "no tables found")
    require(tableMap.contains(from), s"from table not found : $from")

    val fromTable = tableMap(from)

    // engine specific columns for export should have the different engine
    require(engine != fromTable.engine, s"Alternate must have different engine from source fact : $engine")

    require(!tableMap.values.find(_.from.exists(_.name == from)).exists(_.engine == engine),
            s"Alternate must have different engine from existing alternate engines : $engine")

    require(discarding.subsetOf(fromTable.columnsByNameMap.keySet), "Discarding columns should be present in fromTable")
    val updatedDiscardingSet: Set[String] = getUpdatedDiscardingSet(discarding, fromTable.columnsByNameMap.values)

    //override dim cols by name
    val overrideDimColsByName: Set[String] = overrideDimCols.map(_.name)
    //override fact cols by name
    val overrideFactColsByName: Set[String] = overrideFactCols.map(_.name)

    require(overrideDimColsByName.intersect(updatedDiscardingSet).isEmpty, "Cannot override dim col that is supposed to be discarded: " + overrideDimColsByName.intersect(updatedDiscardingSet).mkString(","))
    require(overrideFactColsByName.intersect(updatedDiscardingSet).isEmpty, "Cannot override fact col that is supposed to be discarded: " + overrideFactColsByName.intersect(updatedDiscardingSet).mkString(","))

    // non engine specific columns (engine specific columns not needed) and filter out overrides
    var dimColMap = fromTable
      .dimCols.filter(c => !c.hasEngineRequirement && !overrideDimColsByName(c.name) && !updatedDiscardingSet.contains(c.name)).map(d => d.name -> d.copyWith(cc, columnAliasMap, true)).toMap
    var factColMap = fromTable
      .factCols.filter(c => !c.hasEngineRequirement && !overrideFactColsByName(c.name) && !updatedDiscardingSet.contains(c.name)).map(f => f.name -> f.copyWith(cc, columnAliasMap, true)).toMap

    val isFromTableView = fromTable.isInstanceOf[FactView]
    // override non engine specific columns or add new columns
    overrideDimCols foreach {
      d =>
        if(!isFromTableView && fromTable.dimColMap.contains(d.name)) {
          val baseDimCol = fromTable.dimColMap(d.name)
          require((!d.dataType.hasStaticMapping) || //If new has no map, we don't care about old.
            (d.dataType.hasStaticMapping &&
              (baseDimCol.dataType.reverseStaticMapping == d.dataType.reverseStaticMapping)),//If old has a map, it must match new map
            s"Override column cannot have static mapping : name=$name, from=$from, engine=$engine col=$d")
        }                                                                                     // If no old col, don't care
        dimColMap += (d.name -> d)
    }

    require(dimColMap.values.exists(_.isForeignKey), s"Fact has no foreign keys after discarding $discarding")

    overrideFactCols foreach {
      f =>
        if(!isFromTableView && fromTable.factColMap.contains(f.name)) {
          val baseFactCol = fromTable.factColMap(f.name)
          require((!f.dataType.hasStaticMapping) || //If new has no map, we don't care about old.
            (f.dataType.hasStaticMapping &&
              (baseFactCol.dataType.reverseStaticMapping == f.dataType.reverseStaticMapping)),//If old has a map, it must match new map
            s"Override column cannot have static mapping : name=$name, from=$from, engine=$engine col=$f")
        }                                                                                     // If no old col, don't care
        factColMap += (f.name -> f)
    }


    val dimsWithAnnotationsWithEngineRequirement = dimColMap
      .values
      .filter(_.hasAnnotationsWithEngineRequirement)

    val missingDimOverrides = dimsWithAnnotationsWithEngineRequirement
      .map(c => (c.name, c.annotationsWithEngineRequirement.map(_.asInstanceOf[EngineRequirement]).filterNot(_.acceptEngine(engine))))
      .filter(_._2.nonEmpty)

    val factsWithAnnotationsWithEngineRequirement = factColMap
      .values
      .filter(c => c.hasAnnotationsWithEngineRequirement)

    val missingFactAnnotationOverrides = factsWithAnnotationsWithEngineRequirement
      .map(c => (c.name, c.annotationsWithEngineRequirement.map(_.asInstanceOf[EngineRequirement]).filterNot(_.acceptEngine(engine))))
      .filter(_._2.nonEmpty)

    val factsWithRollupWithEngineRequirement = factColMap
      .values
      .filter(c => c.hasRollupWithEngineRequirement)

    val missingFactRollupOverrides = factsWithRollupWithEngineRequirement
      .filterNot(c => c.rollupExpression.asInstanceOf[EngineRequirement].acceptEngine(engine))
      .map(c => (c.name, c.rollupExpression))

    val nonEngineFactAnnotations = fromTable.annotations.filterNot(_.isInstanceOf[EngineRequirement])
    val newGrain = grain.getOrElse(fromTable.grain)

    require(missingDimOverrides.isEmpty && missingFactAnnotationOverrides.isEmpty && missingFactRollupOverrides.isEmpty,
            s"""name=$name, from=$from, engine=$engine, missing dim overrides = $missingDimOverrides ,
                                                                                                      |missing fact annotation overrides = $missingFactAnnotationOverrides,
                                                                                                                                                                            |missing fact rollup overrides = $missingFactRollupOverrides""".stripMargin)


    tableMap = tableMap +
               (name -> new FactTable(
                 name
                 , fromTable.level
                 , newGrain
                 , engine
                 , schemas.getOrElse(fromTable.schemas)
                 , dimColMap.values.toSet
                 , factColMap.values.toSet
                 , Option(fromTable)
                 , annotations = nonEngineFactAnnotations ++ overrideAnnotations
                 , ddlAnnotation = overrideDDLAnnotation
                 , costMultiplierMap = costMultiplierMap
                 , forceFilters = forceFilters
                 , defaultCardinality = defaultCardinality
                 , defaultRowCount=defaultRowCount
                 , fromTable.viewBaseTable
                 , if (maxDaysWindow.isDefined) maxDaysWindow else fromTable.maxDaysWindow
                 , if (maxDaysLookBack.isDefined) maxDaysLookBack else fromTable.maxDaysLookBack
                 , availableOnwardsDate
                 , underlyingTableName
               ))

    this
  }

  def withAvailableOnwardsDate(name: String
                            , from: String
                            , discarding: Set[String] = Set.empty
                            , engine: Engine
                            , overrideDimCols: Set[DimensionColumn] = Set.empty
                            , overrideFactCols: Set[FactColumn] = Set.empty
                            , schemas: Option[Set[Schema]] = None
                            , overrideAnnotations: Set[FactAnnotation] = Set.empty
                            , overrideDDLAnnotation: Option[DDLAnnotation] = None
                            , costMultiplierMap: Map[RequestType, CostMultiplier] = Fact.DEFAULT_COST_MULTIPLIER_MAP
                            , columnAliasMap : Map[String, String] = Map.empty
                            , forceFilters: Set[ForceFilter] = Set.empty
                            , grain: Option[Grain] = None
                            , defaultCardinality:Int = 10
                            , defaultRowCount:Int = 100
                            , maxDaysWindow: Option[Map[RequestType, Int]] = None
                            , maxDaysLookBack: Option[Map[RequestType, Int]] = None
                            , availableOnwardsDate : Option[String] = None
                            , underlyingTableName: Option[String] = None)(implicit cc: ColumnContext) : FactBuilder = {

    require(!tableMap.contains(name), s"should not export with existing table name $name")
    require(tableMap.nonEmpty, "no tables found")
    require(tableMap.contains(from), s"from table not found : $from")
    require(availableOnwardsDate.isDefined, s"availableOnwardsDate parameter must be defined in withAvailableOnwardsDate in $name")
    dateValidate(name, availableOnwardsDate.get, engine)

    val fromTable = tableMap(from)

    discarding foreach {
      d =>
        require(fromTable.factCols.map(_.name).contains(d) || fromTable.dimCols.map(_.name).contains(d), s"column $d does not exist")
    }

    //override dim cols by name
    val overrideDimColsByName: Set[String] = overrideDimCols.map(_.name)
    //override fact cols by name
    val overrideFactColsByName: Set[String] = overrideFactCols.map(_.name)

    // non engine specific columns (engine specific columns not needed) and filter out overrides
    var dimColMap = fromTable
      .dimCols
      .filter(dim => !discarding.contains(dim.name))
      .filter(c => !c.hasEngineRequirement && !overrideDimColsByName(c.name))
      .map(d => d.name -> d.copyWith(cc, columnAliasMap, true))
      .toMap
    var factColMap = fromTable
      .factCols
      .filter(fact => !discarding.contains(fact.name))
      .filter(c => !c.hasEngineRequirement && !overrideFactColsByName(c.name))
      .map(f => f.name -> f.copyWith(cc, columnAliasMap, true))
      .toMap

    val isFromTableView = fromTable.isInstanceOf[FactView]
    // override non engine specific columns or add new columns
    overrideDimCols foreach {
      d =>
        if(!isFromTableView && fromTable.dimColMap.contains(d.name)) {
          val baseDimCol = fromTable.dimColMap(d.name)
          require((!d.dataType.hasStaticMapping) || //If new has no map, we don't care about old.
            (d.dataType.hasStaticMapping &&
              (baseDimCol.dataType.reverseStaticMapping == d.dataType.reverseStaticMapping)),//If old has a map, it must match new map
            s"Override column cannot have static mapping : name=$name, from=$from, engine=$engine col=$d")
        }                                                                                     // If no old col, don't care
        dimColMap += (d.name -> d)
    }

    overrideFactCols foreach {
      f =>
        if(!isFromTableView && fromTable.factColMap.contains(f.name)) {
          val baseFactCol = fromTable.factColMap(f.name)
          require((!f.dataType.hasStaticMapping) || //If new has no map, we don't care about old.
            (f.dataType.hasStaticMapping &&
              (baseFactCol.dataType.reverseStaticMapping == f.dataType.reverseStaticMapping)),//If old has a map, it must match new map
            s"Override column cannot have static mapping : name=$name, from=$from, engine=$engine col=$f")
        }                                                                                     // If no old col, don't care
        factColMap += (f.name -> f)
    }


    val dimsWithAnnotationsWithEngineRequirement = dimColMap
      .values
      .filter(_.hasAnnotationsWithEngineRequirement)

    val missingDimOverrides = dimsWithAnnotationsWithEngineRequirement
      .map(c => (c.name, c.annotationsWithEngineRequirement.map(_.asInstanceOf[EngineRequirement]).filterNot(_.acceptEngine(engine))))
      .filter(_._2.nonEmpty)

    val factsWithAnnotationsWithEngineRequirement = factColMap
      .values
      .filter(c => c.hasAnnotationsWithEngineRequirement)

    val missingFactAnnotationOverrides = factsWithAnnotationsWithEngineRequirement
      .map(c => (c.name, c.annotationsWithEngineRequirement.map(_.asInstanceOf[EngineRequirement]).filterNot(_.acceptEngine(engine))))
      .filter(_._2.nonEmpty)

    val factsWithRollupWithEngineRequirement = factColMap
      .values
      .filter(c => c.hasRollupWithEngineRequirement)

    val missingFactRollupOverrides = factsWithRollupWithEngineRequirement
      .filterNot(c => c.rollupExpression.asInstanceOf[EngineRequirement].acceptEngine(engine))
      .map(c => (c.name, c.rollupExpression))

    val nonEngineFactAnnotations = fromTable.annotations.filterNot(_.isInstanceOf[EngineRequirement])
    val newGrain = grain.getOrElse(fromTable.grain)

    require(missingDimOverrides.isEmpty && missingFactAnnotationOverrides.isEmpty && missingFactRollupOverrides.isEmpty,
      s"""name=$name, from=$from, engine=$engine, missing dim overrides = $missingDimOverrides ,
         |missing fact annotation overrides = $missingFactAnnotationOverrides,
         |missing fact rollup overrides = $missingFactRollupOverrides""".stripMargin)


    val newDDLAnnotation = engine match {
      case HiveEngine =>
        if (overrideDDLAnnotation.isDefined) {
          val hiveAnnotation = overrideDDLAnnotation.get.asInstanceOf[HiveDDLAnnotation]
          val newColOrder = hiveAnnotation.columnOrdering.filter(c => !discarding.contains(c))
          val newHiveDDLAnnotation = new HiveDDLAnnotation(hiveAnnotation.annotations, newColOrder)
          Option(newHiveDDLAnnotation.asInstanceOf[DDLAnnotation])
        } else {
          fromTable.ddlAnnotation
        }
      case _ =>
        overrideDDLAnnotation
    }

    tableMap = tableMap +
      (name -> new FactTable(
        name
        , fromTable.level
        , newGrain
        , engine
        , schemas.getOrElse(fromTable.schemas)
        , dimColMap.values.toSet
        , factColMap.values.toSet
        , Option(fromTable)
        , annotations = nonEngineFactAnnotations ++ overrideAnnotations
        , ddlAnnotation = newDDLAnnotation
        , costMultiplierMap = costMultiplierMap
        , forceFilters = forceFilters
        , defaultCardinality = defaultCardinality
        , defaultRowCount=defaultRowCount
        , fromTable.viewBaseTable
        , if (maxDaysWindow.isDefined) maxDaysWindow else fromTable.maxDaysWindow
        , if (maxDaysLookBack.isDefined) maxDaysLookBack else fromTable.maxDaysLookBack
        , availableOnwardsDate
        , underlyingTableName
      ))

    this
  }

  // discard fact cols to create a subset of a fact table
  def createSubset(name: String
                   , from: String
                   , discarding: Set[String]
                   , schemas: Set[Schema]
                   , columnAliasMap : Map[String, String] = Map.empty
                   , forceFilters: Set[ForceFilter] = Set.empty
                   , resetAliasIfNotPresent: Boolean = false
                   , availableOnwardsDate : Option[String] = None
                   , underlyingTableName: Option[String] = None) : FactBuilder = {
    require(tableMap.nonEmpty, "no table to create subset from")
    require(tableMap.contains(from), s"from table not valid $from")
    require(!tableMap.contains(name), s"table $name already exists")
    require(discarding.nonEmpty, "discarding set should never be empty")
    require(availableOnwardsDate.isDefined || schemas == Set(NoopSchema), "Public rollups should have a defined availableOnwardsDate")

    val fromTable = tableMap(from)

    if(schemas.nonEmpty) {
      require(fromTable.schemas != schemas, s"schemas are the same! from: ${fromTable.schemas}, to: $schemas")
    }

    discarding foreach {
      d =>
        require(!fromTable.dimCols.map(_.name).contains(d), s"Cannot discard dim column $d with createSubset, use newRollup to discard")
        require(fromTable.factCols.map(_.name).contains(d), s"column $d does not exist")
    }

    ColumnContext.withColumnContext { columnContext =>
      val dimCols = fromTable
        .dimCols
        .filter(dimCol => !dimCol.isDerivedColumn
          || (dimCol.isDerivedColumn
          && dimCol.asInstanceOf[DerivedColumn].derivedExpression.sourceColumns.intersect(discarding).isEmpty) )map(_.copyWith(columnContext, columnAliasMap, resetAliasIfNotPresent))
      dimCols.foreach {
        c =>
          require(c.columnContext eq columnContext, "Failed to copy fact columns with new column context!")
      }

      val rolledUpFacts = fromTable
        .factCols
        .filter(fact => !discarding.contains(fact.name))
        .filter(factCol => !factCol.isDerivedColumn
          || (factCol.isDerivedColumn
          && factCol.asInstanceOf[DerivedColumn].derivedExpression.sourceColumns.intersect(discarding).isEmpty))
        .map(_.copyWith(columnContext, columnAliasMap, resetAliasIfNotPresent))

      rolledUpFacts.foreach {
        c =>
          require(c.columnContext eq columnContext, "Failed to copy dim columns with new column context!")
      }

      val newSchemas = if(schemas.isEmpty) fromTable.schemas else schemas
      val newForceFilters = if(forceFilters.isEmpty) fromTable.forceFilters else forceFilters

      val ddlAnnotation = fromTable.ddlAnnotation
      val newDDLAnnotation = fromTable.engine match {
        case HiveEngine =>
          if (ddlAnnotation.isDefined) {
            val hiveAnnotation = ddlAnnotation.get.asInstanceOf[HiveDDLAnnotation]
            val newColOrder = hiveAnnotation.columnOrdering.filter(c => !discarding.contains(c))
            val newHiveDDLAnnotation = new HiveDDLAnnotation(hiveAnnotation.annotations, newColOrder)
            Option(newHiveDDLAnnotation.asInstanceOf[DDLAnnotation])
          } else {
            ddlAnnotation
          }
        case _ =>
          ddlAnnotation
      }
      tableMap = tableMap +
        (name -> new FactTable(
          name
          , fromTable.level - 1
          , fromTable.grain
          , fromTable.engine
          , newSchemas
          , dimCols
          , rolledUpFacts
          , Option(fromTable)
          , fromTable.annotations
          , newDDLAnnotation
          , fromTable.costMultiplierMap
          , newForceFilters
          , fromTable.defaultCardinality
          , fromTable.defaultRowCount
          , fromTable.viewBaseTable
          , fromTable.maxDaysWindow
          , fromTable.maxDaysLookBack
          , availableOnwardsDate
          , underlyingTableName
        ))

      this

    }
  }

  def newRollUp(name: String
                , from: String
                , discarding: Set[String]
                , costMultiplier: Option[BigDecimal] = None
                , overrideAnnotations: Set[FactAnnotation] = Set.empty
                , overrideDDLAnnotations: Option[DDLAnnotation] = None
                , columnAliasMap : Map[String, String] = Map.empty
                , forceFilters: Set[ForceFilter] = Set.empty
                , viewBaseTable : Option[String] = None
                , resetAliasIfNotPresent: Boolean = false
                , grain: Option[Grain] = None
                , maxDaysWindow: Option[Map[RequestType, Int]] = None
                , maxDaysLookBack: Option[Map[RequestType, Int]] = None
                , availableOnwardsDate : Option[String] = None
                , underlyingTableName: Option[String] = None
                , schemas: Set[Schema] = Set.empty
               ) : FactBuilder = {
    require(tableMap.nonEmpty, "no table to roll up from")
    require(tableMap.contains(from), s"from table not valid $from")
    require(!tableMap.contains(name), s"table $name already exists")
    require(discarding.nonEmpty, "discardings should never be empty in rollup")
    require(availableOnwardsDate.isDefined || schemas == Set(NoopSchema), "Public rollups should have a defined availableOnwardsDate")

    val fromTable = tableMap(from)
    discarding foreach {
      d =>
        require(!fromTable.factCols.map(_.name).contains(d), s"Cannot discard fact column $d with newRollup, use createSubset to discard")
        require(fromTable.dimCols.map(_.name).contains(d), s"dim column $d does not exist")
    }

    ColumnContext.withColumnContext { columnContext =>
      var updatedDiscardingSet: Set[String] = getUpdatedDiscardingSet(discarding, fromTable.dimCols.view)
      val rolledUpDims = {
        fromTable
          .dimCols
          .filter(dim => !discarding.contains(dim.name))
          .filter(dimCol => !dimCol.isDerivedColumn
          || (dimCol.isDerivedColumn
          && dimCol.asInstanceOf[DerivedColumn].derivedExpression.sourceColumns.intersect(updatedDiscardingSet).isEmpty) )
          .map(_.copyWith(columnContext, columnAliasMap, resetAliasIfNotPresent))
      }

      rolledUpDims.foreach {
        c =>
          require(c.columnContext eq columnContext, "Failed to copy dim columns with new column context!")
      }

      require(rolledUpDims.exists(_.isForeignKey), s"Fact has no foreign keys after discarding $discarding")

      val factCols = {
        updatedDiscardingSet = getUpdatedDiscardingSet(updatedDiscardingSet, fromTable.factCols.view)
        fromTable
          .factCols
          .filter(factCol => !factCol.isDerivedColumn
          || (factCol.isDerivedColumn
          && factCol.asInstanceOf[DerivedColumn].derivedExpression.sourceColumns.intersect(updatedDiscardingSet).isEmpty) )
          .map(_.copyWith(columnContext, columnAliasMap, resetAliasIfNotPresent))
      }

      factCols.foreach {
        c =>
          require(c.columnContext eq columnContext, "Failed to copy fact columns with new column context!")
      }

      val ddlAnnotations : Option[DDLAnnotation] = overrideDDLAnnotations match {
        case None =>
          val annotation = fromTable.ddlAnnotation
          fromTable.engine match {
            case HiveEngine =>
              if (annotation.isDefined) {
                val hiveAnnotation = annotation.get.asInstanceOf[HiveDDLAnnotation]
                val newColOrder = hiveAnnotation.columnOrdering.filter(c => !discarding.contains(c))
                val newHiveDDLAnnotation = new HiveDDLAnnotation(hiveAnnotation.annotations, newColOrder)
                Option(newHiveDDLAnnotation.asInstanceOf[DDLAnnotation])
              } else {
                annotation
              }
            case _ =>
              annotation
          }
        case Some(_) => overrideDDLAnnotations
      }

      val newGrain = grain.getOrElse(fromTable.grain)
      val newForceFilters = if(forceFilters.isEmpty) fromTable.forceFilters else forceFilters

      tableMap = tableMap +
        (name -> new FactTable(
          name
          , fromTable.level - 1
          , newGrain
          , fromTable.engine
          , if (schemas.isEmpty) fromTable.schemas else schemas
          , rolledUpDims
          , factCols
          , Option(fromTable)
          , fromTable.annotations ++ overrideAnnotations
          , ddlAnnotations
          , fromTable.costMultiplierMap
          , newForceFilters
          , fromTable.defaultCardinality
          , fromTable.defaultRowCount
          , if (viewBaseTable.isDefined) viewBaseTable else fromTable.viewBaseTable
          , if (maxDaysWindow.isDefined) maxDaysWindow else fromTable.maxDaysWindow
          , if (maxDaysLookBack.isDefined) maxDaysLookBack else fromTable.maxDaysLookBack
          , availableOnwardsDate
          , underlyingTableName
      ))
      this
    }
  }

  def newViewTableRollUp(newView: UnionView
                , from: String
                , discarding: Set[String]) : FactBuilder = {
    require(tableMap.nonEmpty, "no table to roll up from")
    require(tableMap.contains(from), s"from table not valid $from")
    require(!tableMap.contains(newView.viewName), s"table ${newView.viewName} already exists")
    require(discarding.nonEmpty, "discardings should never be empty in rollup")
    val fromTable = tableMap(from)
    discarding foreach {
      d =>
        require(!fromTable.factCols.map(_.name).contains(d), s"Cannot discard fact column $d with newRollup")
        require(fromTable.dimCols.map(_.name).contains(d), s"dim column $d does not exist")
    }

    ColumnContext.withColumnContext { columnContext =>
      var updatedDiscardingSet: Set[String] = getUpdatedDiscardingSet(discarding, fromTable.dimCols.view)
      val rolledUpDims = {
        fromTable
          .dimCols
          .filter(dim => !discarding.contains(dim.name))
          .filter(dimCol => !dimCol.isDerivedColumn
            || (dimCol.isDerivedColumn
            && dimCol.asInstanceOf[DerivedColumn].derivedExpression.sourceColumns.intersect(updatedDiscardingSet).isEmpty) )
          .map(_.copyWith(columnContext, Map.empty, false))
      }

      rolledUpDims.foreach {
        c =>
          require(c.columnContext eq columnContext, "Failed to copy dim columns with new column context!")
      }

      require(rolledUpDims.exists(_.isForeignKey), s"Fact has no foreign keys after discarding $discarding")

      val factCols = {
        updatedDiscardingSet = getUpdatedDiscardingSet(updatedDiscardingSet, fromTable.factCols.view)
        fromTable
          .factCols
          .filter(factCol => !factCol.isDerivedColumn
            || (factCol.isDerivedColumn
            && factCol.asInstanceOf[DerivedColumn].derivedExpression.sourceColumns.intersect(updatedDiscardingSet).isEmpty) )
          .map(_.copyWith(columnContext, Map.empty, false))
      }

      factCols.foreach {
        c =>
          require(c.columnContext eq columnContext, "Failed to copy fact columns with new column context!")
      }

      tableMap = tableMap +
        (newView.viewName -> new ViewTable(
          newView
          , fromTable.level - 1
          , fromTable.grain
          , fromTable.engine
          , fromTable.schemas
          , rolledUpDims
          , factCols
          , Option(fromTable)
          , fromTable.annotations
          , fromTable.ddlAnnotation
          , fromTable.costMultiplierMap
          , fromTable.forceFilters
          , fromTable.defaultCardinality
          , fromTable.defaultRowCount
          , fromTable.viewBaseTable
          , fromTable.maxDaysWindow
          , fromTable.maxDaysLookBack
          , None // availableFromDate is not inherited
          , None // underlyingTableName is not inherited
        ))
      this
    }

    ColumnContext.withColumnContext { columnContext =>
      this
    }
  }

  def toPublicFact(name: String
                   , dimCols: Set[PublicDimColumn]
                   , factCols: Set[PublicFactColumn]
                   , forcedFilters: Set[ForcedFilter]
                   , maxDaysWindow: Map[(RequestType, Grain), Int]
                   , maxDaysLookBack: Map[(RequestType, Grain), Int]
                   , enableUTCTimeConversion: Boolean = true
                   , renderLocalTimeFilter: Boolean = true
                   , revision: Int = 0
                   , dimRevision: Int = 0
                   , dimToRevisionMap: Map[String, Int] = Map.empty
                   , requiredFilterColumns: Map[Schema, Set[String]] = Map.empty
                   , powerSetStorage: FkFactMapStorage = RoaringBitmapFkFactMapStorage()
                   ) : PublicFact = {
    new PublicFactTable(name
      , baseFact
      , dimCols
      , factCols
      , tableMap
      , forcedFilters
      , maxDaysWindow
      , maxDaysLookBack
      , dimCardinalityLookup
      , enableUTCTimeConversion
      , renderLocalTimeFilter
      , revision
      , dimRevision
      , None
      , dimToRevisionMap
      , requiredFilterColumns
      , powerSetStorage
    )
  }

  def copyPublicFact(alias: String
                     , revision: Int
                     , publicFact: PublicFact
                     , dimToRevisionOverrideMap: Map[String, Int] = Map.empty
                     , dimColOverrides: Set[PublicDimColumn] = Set.empty
                     , factColOverrides: Set[PublicFactColumn] = Set.empty
                     , requiredFilterColumns: Map[Schema, Set[String]] = Map.empty): PublicFact = {
    val overrideNames = dimColOverrides.map(col => col.alias) ++ factColOverrides.map(col => col.alias)
    val publicDimsWithOverrides = publicFact.dimCols.filterNot(col => overrideNames.contains(col.alias)) ++ dimColOverrides
    val publicFactsWithOverrides = publicFact.factCols.filterNot(col => overrideNames.contains(col.alias)) ++ factColOverrides
    new PublicFactTable(
      alias
      , publicFact.baseFact
      , publicDimsWithOverrides
      , publicFactsWithOverrides
      , publicFact.facts
      , publicFact.forcedFilters
      , publicFact.maxDaysWindow
      , publicFact.maxDaysLookBack
      , publicFact.dimCardinalityLookup
      , publicFact.enableUTCTimeConversion
      , publicFact.renderLocalTimeFilter
      , revision
      , publicFact.dimRevision
      , Some(publicFact)
      , publicFact.dimToRevisionMap ++ dimToRevisionOverrideMap
      , requiredFilterColumns
      , publicFact.getFkFactMapStorage
    )
  }
}

trait PublicFactColumn extends PublicColumn

case class PublicFactCol (name: String
                          , alias: String
                          , filters: Set[FilterOperation]
                          , dependsOnColumns: Set[String] = Set.empty
                          , incompatibleColumns: Set[String] = Set.empty
                          , required: Boolean = false
                          , hiddenFromJson: Boolean = false
                          , filteringRequired: Boolean = false
                          , isImageColumn: Boolean = false
                          , isReplacement: Boolean = false
                          , restrictedSchemas: Set[Schema] = Set.empty) extends PublicFactColumn

case class FactBestCandidate(fkCols: SortedSet[String]
                             , nonFkCols: Set[String]
                             , requestCols: Set[String]
                             , requestJoinCols: Set[String]
                             , filterCols: Set[String]
                             , factCost: Long
                             , factRows: RowsEstimate
                             , fact: Fact
                             , publicFact: PublicFact
                             , dimColMapping: Map[String, String]
                             , factColMapping: Map[String, String]
                             , filters: SortedSet[Filter]
                             , duplicateAliasMapping: Map[String, Set[String]]
                             , schemaRequiredAliases: Set[String]
                             , requestModel: RequestModel
                             , isGrainOptimized: Boolean
                             , isIndexOptimized: Boolean
                              ) {
  def debugString: String = {
    s"""
       fact.name=${fact.name}
       publicFact.name=${publicFact.name}
       fkCols=$fkCols
       nonFkCols=$nonFkCols
       requestCols=$requestCols
       filterCols=$filterCols
       factCost=$factCost
       factRows=$factRows
       dimColMapping=$dimColMapping
       factColMapping=$factColMapping
       filters=$filters
       duplicateAliasMapping=$duplicateAliasMapping
       schemaRequiredAliases=$schemaRequiredAliases
       isGrainOptimized=$isGrainOptimized
       isIndexOptimized=$isIndexOptimized
     """
  }

  lazy val resolvedFactCols: Set[String] = factColMapping.keys.map(fact.columnsByNameMap.apply).map {
    col =>
      col.alias.getOrElse(col.name)
  }.toSet
}

case class FactCandidate(fact: Fact, publicFact: PublicFact, filterCols: Set[String])
case class BestCandidates(fkCols: SortedSet[String],
                          nonFkCols: Set[String],
                          requestCols: Set[String],
                          requestJoinCols: Set[String],
                          facts: Map[String, FactCandidate],
                          publicFact: PublicFact,
                          dimColMapping: Map[String, String],
                          factColMapping: Map[String, String],
                          dimColAliases: Set[String],
                          factColAliases: Set[String],
                          duplicateAliasMapping: Map[String, Set[String]]
                           ) {

  def getFactBestCandidate(factName: String, requestModel: RequestModel) : FactBestCandidate = {
    val fc = facts(factName)
    val factRowsCostEstimate = requestModel.factCost((fc.fact.name, fc.fact.engine))
    FactBestCandidate(
      fkCols,
      nonFkCols,
      requestCols,
      requestJoinCols,
      fc.filterCols,
      factRowsCostEstimate.costEstimate,
      factRowsCostEstimate.rowsEstimate,
      fc.fact,
      publicFact,
      dimColMapping,
      factColMapping,
      requestModel.factFilters ++ (fc.fact.forceFilters.filter(forceFilter
                                     => !(forceFilter.isOverridable &&  requestModel.factFilters.contains(forceFilter.filter))).map(_.filter)),
      duplicateAliasMapping,
      requestModel.factSchemaRequiredAliasesMap(factName),
      requestModel,
      factRowsCostEstimate.isGrainOptimized,
      factRowsCostEstimate.isIndexOptimized
    )
  }
}

/**
 * requiredFilterColumns - Map from schema to all possibly filter-required cols in that schema.
 * Only one of these columns is required in the request.
 */
trait PublicFact extends PublicTable {
  def baseFact: Fact
  def name: String
  def dimCols: Set[PublicDimColumn]
  def factCols: Set[PublicFactColumn]
  def getCandidatesFor(schema: Schema, requestType: RequestType, requestAliases: Set[String], requestJoinAliases: Set[String], filterAliasAndOperation: Map[String, FilterOperation], requestedDaysWindow:Int, requestedDaysLookBack:Int, localTimeDayFilter:Filter): Option[BestCandidates]
  def columnsByAlias: Set[String]
  def foreignKeyAliases: Set[String]
  def lowerCaseAliases: Set[String]
  def foreignKeySources: Set[String]
  def aliasToNameColumnMap : Map[String, String]
  def nameToAliasColumnMap : Map[String, Set[String]]
  def dataTypeForAlias(alias: String): DataType
  def foreignKeySourceForAlias(alias: String): Option[String]
  def factSchemaMap : Map[String, Set[Schema]]
  def aliasToReverseStaticMapping : Map[String, Map[String, Set[String]]]
  def maxDaysWindow: Map[(RequestType, Grain), Int]
  def maxDaysLookBack: Map[(RequestType, Grain), Int]
  def factList: Iterable[Fact]
  def dimCardinalityEnginePreference(cardinality: BigDecimal): Map[RequestType, Map[Engine, Int]]
  def enableUTCTimeConversion: Boolean
  def renderLocalTimeFilter: Boolean
  def revision: Int
  def dimRevision: Int
  def dimCardinalityLookup: Option[LongRangeLookup[Map[RequestType, Map[Engine, Int]]]]
  def facts: Map[String, Fact]
  def parentFactTable: Option[PublicFact]
  def dimToRevisionMap: Map[String, Int]
  def requiredFilterColumns: Map[Schema, Set[String]]
  //def getSecondaryDimFactMap: Map[SortedSet[String], SortedSet[String]]
  def getFkFactMapStorage: FkFactMapStorage
}

case class PublicFactTable private[fact](name: String
                                         , baseFact: Fact
                                         , dimCols: Set[PublicDimColumn]
                                         , factCols: Set[PublicFactColumn]
                                         , facts: Map[String, Fact]
                                         , forcedFilters: Set[ForcedFilter]
                                         , maxDaysWindow: Map[(RequestType, Grain), Int]
                                         , maxDaysLookBack: Map[(RequestType, Grain), Int]
                                         , dimCardinalityLookup: Option[LongRangeLookup[Map[RequestType, Map[Engine, Int]]]]
                                         , enableUTCTimeConversion: Boolean
                                         , renderLocalTimeFilter: Boolean
                                         , revision: Int
                                         , dimRevision: Int
                                         , parentFactTable: Option[PublicFact] =  None
                                         , dimToRevisionMap: Map[String, Int] = Map.empty
                                         , requiredFilterColumns: Map[Schema, Set[String]] = Map.empty
                                         , fkFactMapStorage: FkFactMapStorage
                                        ) extends PublicFact with Logging {

  def factList: Iterable[Fact] = facts.values
  //generate column list by alias
  val columnsByAlias: Set[String] = dimCols.map(col => col.alias) ++ factCols.map(col => col.alias)
  val allColumnsByAlias: Set[String] = columnsByAlias
  val forcedFiltersByAliasMap: Map[String, ForcedFilter] = forcedFilters.map(f => f.field -> f).toMap

  val foreignKeyAliases: Set[String] = dimCols.filter(col => facts
    .values
    .map(_.dimCols.filter(_.annotations.exists(_.isInstanceOf[ForeignKey])).map(_.name))
    .flatten
    .toSet.contains(col.name)).map(col => col.alias)
  val lowerCaseAliases: Set[String] = dimCols.map(col => col.alias.toLowerCase) ++ factCols.map(col => col.alias.toLowerCase)

  val foreignKeySources: Set[String] = {
    facts.values.map(_.dimCols.map(_.getForeignKeySource).flatten).flatten.toSet
  }

  val aliasToNameColumnMap : Map[String, String] =
    dimCols.map(col => col.alias -> col.name).toMap ++ factCols.map(col => col.alias -> col.name).toMap

  val nameToAliasColumnMap : Map[String, Set[String]] =
    (dimCols.map(col => col.name -> col.alias).toList ++ factCols.map(col => col.name -> col.alias).toList)
      .groupBy(_._1)
      .map {
      case (colName, nameAndAliasList) => colName -> nameAndAliasList.map(_._2).toSet
    }

  val factSchemaMap: Map[String, Set[Schema]] = facts.mapValues(_.schemas)

  val aliasToReverseStaticMapping : Map[String, Map[String, Set[String]]] = {
    val mutableMap = new mutable.HashMap[String, Map[String, Set[String]]]
    facts.values.foreach {
      fact =>
        val smMap = fact.columnsByNameMap.filter {
          case (_, col) => col.dataType.hasStaticMapping
        }
        smMap.foreach {
          case (colName, col) =>
            if (nameToAliasColumnMap.contains(colName)) {
              val aliasSet =  nameToAliasColumnMap(colName)
              val reverseMap = col.dataType.reverseStaticMapping
              aliasSet.foreach {
                alias =>
                  if (mutableMap.contains(alias)) {
                    require(mutableMap(alias) == reverseMap,
                      s"Static mapping must be same across all facts within a cube : cube=$name, check failed on column : $col")
                  } else {
                    mutableMap.put(alias, reverseMap)
                  }
              }
            } else {
              trace(s"Cannot create reverse static mapping on non public column $colName")
            }
        }
    }
    mutableMap.toMap
  }

  val columnsByAliasMap: Map[String, PublicColumn] =
    dimCols.map(pdc => pdc.alias -> pdc).toMap ++ factCols.map(pdc => pdc.alias -> pdc).toMap

  val requiredAliases : Set[String] =
    dimCols.filter(_.required).map(_.alias) ++ factCols.filter(_.required).map(_.alias)

  val dependentColumns: Set[String] =
    dimCols.filter(_.dependsOnColumns.nonEmpty).map(_.alias) ++ factCols.filter(_.dependsOnColumns.nonEmpty).map(_.alias)

  val incompatibleColumns: Map[String, Set[String]] =
    dimCols.filter(_.incompatibleColumns.nonEmpty).map(col => (col.alias, col.incompatibleColumns)).toMap ++
      factCols.filter(_.incompatibleColumns.nonEmpty).map(col => (col.alias,col.incompatibleColumns)).toMap

  val requiredFilterAliases : Set[String] =
    dimCols.filter(_.filteringRequired).map(_.alias) ++ factCols.filter(_.filteringRequired).map(_.alias)

  val restrictedSchemasMap: Map[String, Set[Schema]] = dimCols.filter(_.restrictedSchemas.nonEmpty).map(col => (col.alias , col.restrictedSchemas)).toMap ++
    factCols.filter(_.restrictedSchemas.nonEmpty).map(col => (col.alias , col.restrictedSchemas)).toMap

  //perform validation on construction
  validate()

  //post validation check constructs
  val aliasToDataTypeMap: Map[String, DataType] =
    dimCols.map(col => col.alias -> baseFact.dimCols.find(_.name == col.name).get.dataType).toMap ++
      factCols.map(col => col.alias -> baseFact.factCols.find(_.name == col.name).get.dataType).toMap

  val aliasToForeignKeySourceMap: Map[String, String] = dimCols
    .filter(col => foreignKeyAliases.contains(col.alias))
    .map(col => col.alias -> baseFact.dimCols.find(_.name == col.name))
    .collect {
    case (alias, Some(col)) => alias -> col.getForeignKeySource.get
  }.toMap

  private[this] val fkColumnNameSet: SortedSet[String] =
    facts
      .values
      .map(_.dimCols.filter(_.annotations.exists(_.isInstanceOf[ForeignKey])).map(_.name))
      .flatten
      .to[SortedSet]

  /*
  private[this] val primaryDimFactMap: Map[SortedSet[String], SortedSet[Fact]] =
    facts
      .values
      .map(f => (f.dimCols.filter(_.annotations.exists(_.isInstanceOf[ForeignKey])).map(col => col.name).to[SortedSet], f))
      .groupBy(_._1)
      .mapValues(_.map(tpl => tpl._2)
      .to[SortedSet])*/

/*
  private[this] val secondaryDimFactMap: Map[SortedSet[String], SortedSet[String]] =
    if (this.parentFactTable.isDefined) parentFactTable.get.getSecondaryDimFactMap
    else
*/
  getFkFactMapStorage.store(facts.values)

  private[this] val dimColsByName = dimCols.map(_.name)

  def getFkFactMapStorage: FkFactMapStorage = {
    if (parentFactTable.isDefined) parentFactTable.get.getFkFactMapStorage
    else fkFactMapStorage
  }

  def getCandidatesFor(schema: Schema, requestType: RequestType, requestAliases: Set[String], requestJoinAliases: Set[String], filterAliasAndOperation: Map[String, FilterOperation], requestedDaysWindow:Int, requestedDaysLookBack:Int, localTimeDayFilter:Filter) : Option[BestCandidates] = {
    val aliases = requestAliases ++ filterAliasAndOperation.keySet
    //alias to col names
    val colsByNameAndMapTry = Try {
      //cols by name
      val colsByNameSet = new mutable.TreeSet[String]()
      //dim col aliases
      val dimColAliases = new mutable.TreeSet[String]()
      //fact col aliases
      val factColAliases = new mutable.TreeSet[String]()
      //dim col name to alias map
      val dimColNameToAliasMap = new mutable.HashMap[String, String]()
      //fact col name to alias map
      val factColNameToAliasMap = new mutable.HashMap[String, String]()
      //identify the foreign key dim cols
      val fkCols = new mutable.TreeSet[String]
      //identify the remaining cols
      val remainingFields = new mutable.TreeSet[String]
      //duplicate mappings
      val duplicateAliasMapping = new mutable.HashMap[String, Set[String]]()
      
      aliases.foreach {
        alias =>
          val name = aliasToNameColumnMap(alias)

          colsByNameSet += name

          if(fkColumnNameSet(name)) {
            fkCols += name
          } else {
            if(remainingFields(name)) {
              nameToAliasColumnMap(name).foreach {
                alias =>
                  duplicateAliasMapping += (alias -> nameToAliasColumnMap(name).-(alias) )
              }
            } else {
              remainingFields += name
            }
          }
          if(dimColsByName(name)) {
            dimColAliases += alias
            dimColNameToAliasMap.put(name, alias)
          } else {
            factColAliases += alias
            factColNameToAliasMap.put(name, alias)
          }
      }
      (colsByNameSet.toSet
        , fkCols.to[SortedSet]
        , remainingFields.toSet
        , dimColNameToAliasMap.toMap
        , factColNameToAliasMap.toMap
        , dimColAliases.toSet
        , factColAliases.toSet
        , duplicateAliasMapping.toMap)
    }.toOption

    //@tailrec
    def recursiveFindFact(fact: Fact, fields: Set[String]) : Option[Fact] = {
      if(fields.forall(fact.fields)) {
        return Option(fact)
      }
      None
      //if(fact.from.isEmpty)
      //  return None
      //recursiveFindFact(fact.from.get, fields)
    }

    colsByNameAndMapTry.flatMap {
      case (colsByNameSet, fkCols, remainingFields, dimColNameToAliasMap, factColNameToAliasMap, dimColAliases, factColAliases, duplicateAliasMapping) =>

        //require(factColNameToAliasMap.nonEmpty, s"must have at least one fact column : $aliases")
        /* TODO: this may be point less since we may be weeding out certain engines prematurely
        val primarySearch = for {
          facts <- primaryDimFactMap.get(fkCols)
          satisfyingFacts = facts.collect {
            case f if recursiveFindFact(f, remainingFields).isDefined => f
          }
          if satisfyingFacts.nonEmpty
        } yield satisfyingFacts*/

        val factsToSearch = {
          if(fkCols.nonEmpty) {
            getFkFactMapStorage.search(fkCols)
          } else {
            Option(facts.values)
          }
        }


        val finalSearch =  for {
          facts <- factsToSearch
          satisfyingFacts = facts.collect {
            case f if recursiveFindFact(f, remainingFields).isDefined => f
          }
          if satisfyingFacts.nonEmpty
        } yield satisfyingFacts

        val qualifiedFinalSearch = finalSearch.map {
          facts => facts.filter {
            fact =>
              val withinMaxDaysWindowOption = {
                val maxDaysWindow = fact.maxDaysWindow.map(_.getOrElse(requestType, 0)).getOrElse(Int.MaxValue)
                requestedDaysWindow <= maxDaysWindow
              }
              val withinMaxDaysLookBackOption = {
                val maxDaysLookBack = fact.maxDaysLookBack.map(_.getOrElse(requestType, 0)).getOrElse(Int.MaxValue)
                requestedDaysLookBack <= maxDaysLookBack
              }
              val isAvailableFromDate = isSupportingTheRequestedDate(fact.availableOnwardsDate, localTimeDayFilter)
              withinMaxDaysWindowOption && withinMaxDaysLookBackOption && isAvailableFromDate
          }
        }

        qualifiedFinalSearch match {
          case Some(sf) =>
            val requestColumnNames = requestAliases.map(aliasToNameColumnMap.apply)
            val requestJoinColumnNames = requestJoinAliases.map(aliasToNameColumnMap.apply)
            val filterColumnNameAndOperation = filterAliasAndOperation.map {
              case (alias, op) => aliasToNameColumnMap(alias) -> op
            }
            val filterColumnNames = filterColumnNameAndOperation.keySet
            val factsWithFilterOperations = sf.view.filter {
              f => filterColumnNameAndOperation.forall {
                case (columnName, op) =>
                  val col = f.columnsByNameMap(columnName)
                  col.filterOperationOverrides.isEmpty || (col.filterOperationOverrides.nonEmpty && col.filterOperationOverrides(op))
              }
            }.map { fact =>
              val forceFilterColumnNames: Set[String] = fact.forceFilters.map(f => aliasToNameColumnMap(f.filter.field))
              FactCandidate(fact, this, filterColumnNames ++ forceFilterColumnNames)
            }

            Some(
              BestCandidates(
                fkCols,
                remainingFields,
                requestColumnNames,
                requestJoinColumnNames,
                factsWithFilterOperations
                  .filter(_.fact.schemas.contains(schema))
                  .map(f => f.fact.name -> f)
                  .toMap,
                this,
                dimColNameToAliasMap,
                factColNameToAliasMap,
                dimColAliases,
                factColAliases,
                duplicateAliasMapping
              ))
          case _ => None
        }
    }
  }

  private[this] def validate(): Unit = {
    require(maxDaysWindow.nonEmpty, "No max days window defined, public fact supports no request types!")
    require(maxDaysLookBack.nonEmpty, "No max days look back window defined, public fact supports no request types!")
    require(facts.nonEmpty, s"public fact $name has no underlying facts")
    require(columnsByAlias.size == (dimCols.size + factCols.size), "Column names size mismatch with dimCols + factCols")
    require(lowerCaseAliases.size == (dimCols.filterNot(c => c.isReplacement).size + factCols.size), "Column aliases size mismatch with dimCols + factCols")
    
    //run post validate on each fact
    facts.values.foreach(_.postValidate(this))

    var dimColNames: Set[String] = Set()
    dimCols.filterNot(c => c.isReplacement).foreach { col =>
      require(!dimColNames.contains(col.name), "dim column names should be unique")
      dimColNames += col.name
    }

    dimCols.foreach { col =>
      require(facts.values.exists(_.dimCols.exists(dc => (dc.name == col.name) || dc.alias.contains(col.name))),
        s"dim column ${col.name} in public fact $name does not exist in any underlying facts")
    }
    factCols.foreach { col =>
      require(facts.values.exists(_.factCols.exists(fc => (fc.name == col.name) || fc.alias.contains(col.name))),
        s"fact column ${col.name} in public fact $name does not exist in any underlying facts")
    }
    validateForcedFilters()
  }

  def dataTypeForAlias(alias: String): DataType = {
    require(aliasToDataTypeMap.contains(alias), "column not exist in this table")
    aliasToDataTypeMap(alias)
  }

  def foreignKeySourceForAlias(alias: String): Option[String] = {
    aliasToForeignKeySourceMap.get(alias)
  }

  def dimCardinalityEnginePreference(cardinality: BigDecimal): Map[RequestType, Map[Engine, Int]] = {
    val longValue = cardinality.longValue()
    dimCardinalityLookup.flatMap(_.find(longValue)).getOrElse(Map.empty)
  }

  def isSupportingTheRequestedDate(availableOnwardsDate: Option[String], localTimeDayFilter:Filter): Boolean = {
    if(availableOnwardsDate.isDefined) {
      val availableFromDate = DailyGrain.fromFormattedString(availableOnwardsDate.get)
      val isSupportingDate: Boolean = {
        localTimeDayFilter match {
          case BetweenFilter(_,from,to) =>
            val fromDateMills = DailyGrain.fromFormattedString(from).getMillis
            availableFromDate.isBefore(fromDateMills) || availableFromDate.isEqual(fromDateMills)
          case dtf: DateTimeBetweenFilter =>
            val fromDateMills = dtf.fromDateTime.getMillis
            availableFromDate.isBefore(fromDateMills) || availableFromDate.isEqual(fromDateMills)
          case InFilter(_,dates, _, _) =>
            val minDateMills = dates.map(DailyGrain.fromFormattedString(_).getMillis).min
            availableFromDate.isBefore(minDateMills) || availableFromDate.isEqual(minDateMills)
          case EqualityFilter(_,date, _, _) =>
            val dateMills = DailyGrain.fromFormattedString(date).getMillis
            availableFromDate.isBefore(dateMills) || availableFromDate.isEqual(dateMills)
          case a =>
            throw new IllegalArgumentException(s"Filter operation not supported. Day filter can be between, in, equality filter : $a")
        }
      }
      isSupportingDate
    } else {
      true
    }
  }

}
