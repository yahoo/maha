// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.dimension


import com.google.common.annotations.VisibleForTesting
import com.yahoo.maha.core.DruidDerivedFunction.GET_INTERVAL_DATE
import com.yahoo.maha.core.ddl.{DDLAnnotation, HiveDDLAnnotation}
import com.yahoo.maha.core._
import com.yahoo.maha.core.request.RequestType
import scala.collection.{SortedSet, mutable}
import scala.collection.immutable.TreeSet
import org.json4s.JsonAST.{JArray, JObject, JValue}
import org.json4s.scalaz.JsonScalaz._
/**
 * Created by hiral on 10/2/15.
 */

sealed trait DimensionColumn extends Column {
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : DimensionColumn
  val isForeignKey: Boolean = {
    annotations.exists(_.isInstanceOf[ForeignKey])
  }
  def getForeignKeySource: Option[String] = {
    annotations.find(_.isInstanceOf[ForeignKey]).map(ca => ca.asInstanceOf[ForeignKey].publicDimName)
  }

  override def asJSON: JObject =
    makeObj(
      List(
        ("DimensionColumn" -> super.asJSON)
        ,("isForeignKey" -> toJSON(isForeignKey))
      )
    )
}

trait ConstDimensionColumn extends DimensionColumn with ConstColumn

trait DerivedDimensionColumn extends DimensionColumn with DerivedColumn

abstract class BaseDimCol extends DimensionColumn {
  columnContext.register(this)
}

abstract class BaseConstDimCol extends BaseDimCol with ConstDimensionColumn

abstract class BaseDerivedDimCol extends BaseDimCol with DerivedDimensionColumn {
  def isAggregateColumn: Boolean = false
}

abstract class BaseDerivedAggregateDimCol extends BaseDerivedDimCol {
  override def isAggregateColumn: Boolean = true
}

abstract class BaseFunctionDimCol extends BaseDimCol with DerivedFunctionColumn {
  def validate()
}

abstract class BasePostResultFunctionDimCol extends BaseDimCol with PostResultColumn

case class DimCol(name: String,
                  dataType: DataType,
                  columnContext: ColumnContext,
                  alias: Option[String],
                  annotations: Set[ColumnAnnotation],
                  filterOperationOverrides: Set[FilterOperation]) extends BaseDimCol {

  override val isDerivedColumn: Boolean = false
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : DimensionColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias))
    }
  }

  private val jUtils = JsonUtils

  override def asJSON: JObject =
    makeObj(
      List(
        ("DimCol" -> super.asJSON)
        ,("name" -> toJSON(name))
        ,("dataType" -> dataType.asJSON)
        ,("aliasOrName" ->  toJSON(alias.getOrElse("")))
        ,("annotations" -> jUtils.asJSON(annotations))
        ,("filterOperationOverrides" -> jUtils.asJSON(filterOperationOverrides.map(fo => fo.toString)))
      )
    )
}

object DimCol {
  def apply(name: String,
            dataType: DataType,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            filterOperationOverrides: Set[FilterOperation] = Set.empty)(implicit cc: ColumnContext) : DimCol = {
    DimCol(name, dataType, cc, alias, annotations, filterOperationOverrides)
  }
}

case class ConstDimCol(name: String,
                       dataType: DataType,
                       constantValue: String,
                       columnContext: ColumnContext,
                       alias: Option[String],
                       annotations: Set[ColumnAnnotation],
                       filterOperationOverrides: Set[FilterOperation]) extends BaseConstDimCol {

  override val isDerivedColumn: Boolean = false
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : DimensionColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias))
    }
  }
  def copyWith(columnContext: ColumnContext, newConstantValue: String) : DimensionColumn = {
      this.copy(columnContext = columnContext,constantValue = newConstantValue)
  }
}

object ConstDimCol {
  def apply(name: String,
            dataType: DataType,
            constantValue: String,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            filterOperationOverrides: Set[FilterOperation] = Set.empty)(implicit cc: ColumnContext) : ConstDimCol = {
    ConstDimCol(name, dataType, constantValue, cc, alias, annotations, filterOperationOverrides)
  }
}

case class HiveDimCol(name: String,
                  dataType: DataType,
                  columnContext: ColumnContext,
                  alias: Option[String],
                  annotations: Set[ColumnAnnotation],
                  filterOperationOverrides: Set[FilterOperation]) extends BaseDimCol with WithHiveEngine {
  override val isDerivedColumn: Boolean = false
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : DimensionColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias))
    }
  }
}

object HiveDimCol {
  def apply(name: String,
            dataType: DataType,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            filterOperationOverrides: Set[FilterOperation] = Set.empty)(implicit cc: ColumnContext) : HiveDimCol = {
    HiveDimCol(name, dataType, cc, alias, annotations, filterOperationOverrides)
  }

}

case class HiveDerDimCol(name: String,
               dataType: DataType,
               columnContext: ColumnContext,
               derivedExpression: HiveDerivedExpression,
               alias: Option[String],
               annotations: Set[ColumnAnnotation],
               filterOperationOverrides: Set[FilterOperation]) extends BaseDerivedDimCol with WithHiveEngine {
  require(derivedExpression != null,
    s"Derived expression should be defined for a derived column $name")
  require(!derivedExpression.expression.hasRollupExpression, s"Cannot have rollup expression for dimension column : $name - $derivedExpression")
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : DimensionColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name), derivedExpression = derivedExpression.copyWith(columnContext))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias), derivedExpression = derivedExpression.copyWith(columnContext))
    }
  }
}

object HiveDerDimCol {
  def apply(name: String,
            dataType: DataType,
            derivedExpression: HiveDerivedExpression,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            filterOperationOverrides: Set[FilterOperation] = Set.empty)(implicit cc: ColumnContext) : HiveDerDimCol = {
    HiveDerDimCol(name, dataType, cc, derivedExpression, alias, annotations, filterOperationOverrides)
  }
}

case class HiveDerDimAggregateCol(name: String,
                         dataType: DataType,
                         columnContext: ColumnContext,
                         derivedExpression: HiveDerivedExpression,
                         alias: Option[String],
                         annotations: Set[ColumnAnnotation],
                         filterOperationOverrides: Set[FilterOperation]) extends BaseDerivedAggregateDimCol with WithHiveEngine {
  require(derivedExpression != null,
    s"Derived expression should be defined for a derived column $name")
  derivedExpression.sourceColumns.foreach {
    colName =>
      val colOption = columnContext.getColumnByName(colName)
      require(colOption.isDefined, s"Failed to find the col $colName registered in the context")
      require(colOption.get.isInstanceOf[DimensionColumn], s"Column $colName in the derived expression in not dimension column, All columns referred by HiveDerDimAggregateCol has to be dimension columns")
  }
  require(derivedExpression.expression.hasRollupExpression, s"HiveDerDimAggregateCol should have rollup expression  $name - $derivedExpression")
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : DimensionColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name), derivedExpression = derivedExpression.copyWith(columnContext))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias), derivedExpression = derivedExpression.copyWith(columnContext))
    }
  }
}

object HiveDerDimAggregateCol {
  def apply(name: String,
            dataType: DataType,
            derivedExpression: HiveDerivedExpression,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            filterOperationOverrides: Set[FilterOperation] = Set.empty) (implicit cc: ColumnContext): HiveDerDimAggregateCol =  {
    HiveDerDimAggregateCol(name, dataType, cc, derivedExpression, alias, annotations, filterOperationOverrides)
  }
}

case class PrestoDimCol(name: String,
                      dataType: DataType,
                      columnContext: ColumnContext,
                      alias: Option[String],
                      annotations: Set[ColumnAnnotation],
                      filterOperationOverrides: Set[FilterOperation]) extends BaseDimCol with WithPrestoEngine {
  override val isDerivedColumn: Boolean = false
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : DimensionColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias))
    }
  }
}

object PrestoDimCol {
  def apply(name: String,
            dataType: DataType,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            filterOperationOverrides: Set[FilterOperation] = Set.empty)(implicit cc: ColumnContext) : PrestoDimCol = {
    PrestoDimCol(name, dataType, cc, alias, annotations, filterOperationOverrides)
  }

}

case class PrestoDerDimCol(name: String,
                         dataType: DataType,
                         columnContext: ColumnContext,
                         derivedExpression: PrestoDerivedExpression,
                         alias: Option[String],
                         annotations: Set[ColumnAnnotation],
                         filterOperationOverrides: Set[FilterOperation]) extends BaseDerivedDimCol with WithPrestoEngine {
  require(derivedExpression != null,
    s"Derived expression should be defined for a derived column $name")
  require(!derivedExpression.expression.hasRollupExpression, s"Cannot have rollup expression for dimension column : $name - $derivedExpression")
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : DimensionColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name), derivedExpression = derivedExpression.copyWith(columnContext))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias), derivedExpression = derivedExpression.copyWith(columnContext))
    }
  }
}

object PrestoDerDimCol {
  def apply(name: String,
            dataType: DataType,
            derivedExpression: PrestoDerivedExpression,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            filterOperationOverrides: Set[FilterOperation] = Set.empty)(implicit cc: ColumnContext) : PrestoDerDimCol = {
    PrestoDerDimCol(name, dataType, cc, derivedExpression, alias, annotations, filterOperationOverrides)
  }
}

case class OracleDerDimCol(name: String,
                     dataType: DataType,
                     columnContext: ColumnContext,
                     derivedExpression: OracleDerivedExpression,
                     alias: Option[String],
                     annotations: Set[ColumnAnnotation],
                     filterOperationOverrides: Set[FilterOperation]) extends BaseDerivedDimCol with WithOracleEngine {
  require(derivedExpression != null,
    s"Derived expression should be defined for a derived column $name")
  require(!derivedExpression.expression.hasRollupExpression, s"Cannot have rollup expression for dimension column : $name - $derivedExpression")
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : DimensionColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name), derivedExpression = derivedExpression.copyWith(columnContext))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias), derivedExpression = derivedExpression.copyWith(columnContext))
    }
  }
}

object OracleDerDimCol {
  def apply(name: String,
            dataType: DataType,
            derivedExpression: OracleDerivedExpression,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            filterOperationOverrides: Set[FilterOperation] = Set.empty)(implicit cc: ColumnContext) : OracleDerDimCol = {
    OracleDerDimCol(name, dataType, cc, derivedExpression, alias, annotations, filterOperationOverrides)
  }
}

case class PostgresDerDimCol(name: String,
                             dataType: DataType,
                             columnContext: ColumnContext,
                             derivedExpression: PostgresDerivedExpression,
                             alias: Option[String],
                             annotations: Set[ColumnAnnotation],
                             filterOperationOverrides: Set[FilterOperation]) extends BaseDerivedDimCol with WithPostgresEngine {
  require(derivedExpression != null,
    s"Derived expression should be defined for a derived column $name")
  require(!derivedExpression.expression.hasRollupExpression, s"Cannot have rollup expression for dimension column : $name - $derivedExpression")
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : DimensionColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name), derivedExpression = derivedExpression.copyWith(columnContext))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias), derivedExpression = derivedExpression.copyWith(columnContext))
    }
  }
}

object PostgresDerDimCol {
  def apply(name: String,
            dataType: DataType,
            derivedExpression: PostgresDerivedExpression,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            filterOperationOverrides: Set[FilterOperation] = Set.empty)(implicit cc: ColumnContext) : PostgresDerDimCol = {
    PostgresDerDimCol(name, dataType, cc, derivedExpression, alias, annotations, filterOperationOverrides)
  }
}

case class BigqueryDimCol(name: String,
                          dataType: DataType,
                          columnContext: ColumnContext,
                          alias: Option[String],
                          annotations: Set[ColumnAnnotation],
                          filterOperationOverrides: Set[FilterOperation]) extends BaseDimCol with WithBigqueryEngine {
  override val isDerivedColumn: Boolean = false
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean): DimensionColumn = {
    if (resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias))
    }
  }
}

object BigqueryDimCol {
  def apply(name: String,
            dataType: DataType,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            filterOperationOverrides: Set[FilterOperation] = Set.empty)(implicit cc: ColumnContext): BigqueryDimCol = {
    BigqueryDimCol(name, dataType, cc, alias, annotations, filterOperationOverrides)
  }
}


case class BigqueryDerDimCol(name: String,
                             dataType: DataType,
                             columnContext: ColumnContext,
                             derivedExpression: BigqueryDerivedExpression,
                             alias: Option[String],
                             annotations: Set[ColumnAnnotation],
                             filterOperationOverrides: Set[FilterOperation]) extends BaseDerivedDimCol with WithBigqueryEngine {
  require(derivedExpression != null,
    s"Derived expression should be defined for a derived column $name")
  require(!derivedExpression.expression.hasRollupExpression, s"Cannot have rollup expression for dimension column: $name - $derivedExpression")
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean): DimensionColumn = {
    if (resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name), derivedExpression = derivedExpression.copyWith(columnContext))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias), derivedExpression = derivedExpression.copyWith(columnContext))
    }
  }
}

object BigqueryDerDimCol {
  def apply(name: String,
            dataType: DataType,
            derivedExpression: BigqueryDerivedExpression,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            filterOperationOverrides: Set[FilterOperation] = Set.empty)(implicit cc: ColumnContext) : BigqueryDerDimCol = {
    BigqueryDerDimCol(name, dataType, cc, derivedExpression, alias, annotations, filterOperationOverrides)
  }
}


case class DruidFuncDimCol(name: String,
                           dataType: DataType,
                           columnContext: ColumnContext,
                           derivedFunction: DruidDerivedFunction,
                           alias: Option[String],
                           annotations: Set[ColumnAnnotation],
                           filterOperationOverrides: Set[FilterOperation]) extends BaseFunctionDimCol with WithDruidEngine {
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : DimensionColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias))
    }
  }

  override def validate() : Unit = {
    derivedFunction match {
      case df@GET_INTERVAL_DATE(fieldName, format) =>
        columnContext.render(df.dimColName, Map.empty)
      case _ =>
    }
  }
}

object DruidFuncDimCol {
  def apply(name: String,
            dataType: DataType,
            derivedFunction: DruidDerivedFunction,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            filterOperationOverrides: Set[FilterOperation] = Set.empty)(implicit cc: ColumnContext) : DruidFuncDimCol = {
    DruidFuncDimCol(name, dataType, cc, derivedFunction, alias, annotations, filterOperationOverrides)
  }
}

case class DruidPostResultFuncDimCol(name: String,
                           dataType: DataType,
                           columnContext: ColumnContext,
                           postResultFunction: DruidPostResultFunction,
                           alias: Option[String],
                           annotations: Set[ColumnAnnotation],
                           filterOperationOverrides: Set[FilterOperation]) extends BasePostResultFunctionDimCol with WithDruidEngine {
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : DimensionColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias))
    }
  }

  override def validate() : Unit = {
    postResultFunction.validate()
  }
}

object DruidPostResultFuncDimCol {
  def apply(name: String,
            dataType: DataType,
            postResultFunction: DruidPostResultFunction,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            filterOperationOverrides: Set[FilterOperation] = Set.empty)(implicit cc: ColumnContext) : DruidPostResultFuncDimCol = {
    DruidPostResultFuncDimCol(name, dataType, cc, postResultFunction, alias, annotations, filterOperationOverrides)
  }
}

case class OraclePartDimCol(name: String,
                          dataType: DataType,
                          columnContext: ColumnContext,
                          alias: Option[String],
                          annotations: Set[ColumnAnnotation],
                          partitionLevel: PartitionLevel) extends BaseDimCol with WithOracleEngine with PartitionColumn {
  override val filterOperationOverrides: Set[FilterOperation] = Set.empty
  override val isDerivedColumn: Boolean = false
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : DimensionColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias))
    }
  }
}

object OraclePartDimCol {
  def apply(name: String,
            dataType: DataType,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            partitionLevel: PartitionLevel = NoPartitionLevel )(implicit cc: ColumnContext) : OraclePartDimCol = {
    OraclePartDimCol(name, dataType, cc, alias, annotations, partitionLevel)
  }
}

case class PostgresPartDimCol(name: String,
                            dataType: DataType,
                            columnContext: ColumnContext,
                            alias: Option[String],
                            annotations: Set[ColumnAnnotation],
                            partitionLevel: PartitionLevel) extends BaseDimCol with WithPostgresEngine with PartitionColumn {
  override val filterOperationOverrides: Set[FilterOperation] = Set.empty
  override val isDerivedColumn: Boolean = false
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : DimensionColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias))
    }
  }
}

object PostgresPartDimCol {
  def apply(name: String,
            dataType: DataType,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            partitionLevel: PartitionLevel = NoPartitionLevel )(implicit cc: ColumnContext) : PostgresPartDimCol = {
    PostgresPartDimCol(name, dataType, cc, alias, annotations, partitionLevel)
  }
}

case class BigqueryPartDimCol(name: String,
                              dataType: DataType,
                              columnContext: ColumnContext,
                              alias: Option[String],
                              annotations: Set[ColumnAnnotation],
                              partitionLevel: PartitionLevel) extends BaseDimCol with WithBigqueryEngine with PartitionColumn {
  override val filterOperationOverrides: Set[FilterOperation] = Set.empty
  override val isDerivedColumn: Boolean = false
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean): DimensionColumn = {
    if (resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias))
    }
  }
}

object BigqueryPartDimCol {
  def apply(name: String,
            dataType: DataType,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            partitionLevel: PartitionLevel = NoPartitionLevel )(implicit cc: ColumnContext): BigqueryPartDimCol = {
    BigqueryPartDimCol(name, dataType, cc, alias, annotations, partitionLevel)
  }
}

case class HivePartDimCol(name: String,
                  dataType: DataType,
                  columnContext: ColumnContext,
                  alias: Option[String],
                  annotations: Set[ColumnAnnotation],
                  partitionLevel: PartitionLevel) extends BaseDimCol with WithHiveEngine with PartitionColumn {
  override val filterOperationOverrides: Set[FilterOperation] = Set.empty
  override val isDerivedColumn: Boolean = false
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : DimensionColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias))
    }
  }
}

object PrestoPartDimCol {
  def apply(name: String,
            dataType: DataType,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            partitionLevel: PartitionLevel = NoPartitionLevel)(implicit cc: ColumnContext) : PrestoPartDimCol = {
    PrestoPartDimCol(name, dataType, cc, alias, annotations, partitionLevel)
  }
}

case class PrestoPartDimCol(name: String,
                          dataType: DataType,
                          columnContext: ColumnContext,
                          alias: Option[String],
                          annotations: Set[ColumnAnnotation],
                          partitionLevel: PartitionLevel) extends BaseDimCol with WithPrestoEngine with PartitionColumn {
  override val filterOperationOverrides: Set[FilterOperation] = Set.empty
  override val isDerivedColumn: Boolean = false
  def copyWith(columnContext: ColumnContext, columnAliasMap: Map[String, String], resetAliasIfNotPresent: Boolean) : DimensionColumn = {
    if(resetAliasIfNotPresent) {
      this.copy(columnContext = columnContext, alias = columnAliasMap.get(name))
    } else {
      this.copy(columnContext = columnContext, alias = (columnAliasMap.get(name) orElse this.alias))
    }
  }
}

object HivePartDimCol {
  def apply(name: String,
            dataType: DataType,
            alias: Option[String] = None,
            annotations: Set[ColumnAnnotation] = Set.empty,
            partitionLevel: PartitionLevel = NoPartitionLevel)(implicit cc: ColumnContext) : HivePartDimCol = {
    HivePartDimCol(name, dataType, cc, alias, annotations, partitionLevel)
  }
}

//object DruidDerDimCol {
//  def apply(name: String, dataType: DataType, derivedExpression: )
//}

trait Dimension extends BaseTable {
  def level: Int
  def dimLevel: DimLevel
  def schemaColFilterMap: Map[Schema, String]
  def engine: Engine
  def name: String
  def columns: Set[DimensionColumn]
  def annotations: Set[DimensionAnnotation]
  def ddlAnnotation: Option[DDLAnnotation]
  def from: Option[Dimension]
  def primaryKey: String
  def foreignKeys: Set[String]
  def columnNames: Set[String]
  def singletonColumn: Option[DimensionColumn]
  def partitionColumns: SortedSet[PartitionColumn]
  def publicDimToForeignKeyMap : Map[String, String]
  def postValidate(publicDimension: PublicDim): Unit
  def sourceToForeignKeyColumnMap : Map[String, DimensionColumn]
  def dimensionColumnsByNameMap : Map[String, DimensionColumn]
  def isDerivedDimension: Boolean
  def viewBaseTable: Option[String]
  def maxDaysLookBack: Option[Map[RequestType, Int]]
  def underlyingTableName: Option[String]
}

object Dimension {
  implicit val ordering: Ordering[Dimension] = Ordering.by(f => s"${f.level}-${f.name}")

  def newDimension(
                    name: String
                    , engine: Engine
                    , dimLevel: DimLevel
                    , schemas: Set[Schema]
                    , columns: Set[DimensionColumn]
                    , maxDaysLookBack: Option[Map[RequestType, Int]]
                    , schemaColMap: Map[Schema, String] = Map.empty
                    , annotations: Set[DimensionAnnotation] = Set.empty
                    , ddlAnnotation: Option[DDLAnnotation] = None
                    , viewBaseTable : Option[String] = None
                    , isDerivedDimension: Boolean = false
                    , underlyingTableName : Option[String] = None
                    ) : DimensionBuilder = {
    val baseFact = new DimTable(name, 9999, engine, dimLevel, schemas, columns, None, schemaColMap, annotations, ddlAnnotation, isDerivedDimension, viewBaseTable, maxDaysLookBack, underlyingTableName)
    val map = Map(baseFact.name -> baseFact)
    DimensionBuilder(baseFact, map)
  }
}

case class DimTable private[dimension](name: String
                                       , level: Int
                                       , engine: Engine
                                       , dimLevel: DimLevel
                                       , schemas: Set[Schema]
                                       , columns: Set[DimensionColumn]
                                       , from: Option[Dimension]
                                       , schemaColFilterMap: Map[Schema, String]
                                       , annotations: Set[DimensionAnnotation]
                                       , ddlAnnotation: Option[DDLAnnotation]
                                       , isDerivedDimension: Boolean
                                       , viewBaseTable: Option[String]
                                       , maxDaysLookBack: Option[Map[RequestType, Int]]
                                       , underlyingTableName: Option[String]
                                      ) extends Dimension {

  val primaryKey: String = {
    val pks = columns.filter(_.annotations.contains(PrimaryKey)).map(_.name)
    require(pks.size == 1, s"Each dimension must have 1 primary key : dim = $name, primary keys = $pks")
    pks.head
  }

  val foreignKeys: Set[String] = columns.filter(_.annotations.exists(_.isInstanceOf[ForeignKey])).map(_.name)
  val columnNames : Set[String] = columns.map(_.name)

  //run validations on construction
  validate()

  val singletonColumn: Option[DimensionColumn] = {
    val singletonCols = columns.filter(_.annotations.exists(_.isInstanceOf[SingletonColumn]))
    require(singletonCols.size <= 1, s"Found multiple singleton columns : $singletonCols")
    singletonCols.headOption
  }

  val partitionColumns: SortedSet[PartitionColumn] = {
    columns.filter(_.isInstanceOf[PartitionColumn]).map(c=> c.asInstanceOf[PartitionColumn]).to[SortedSet]
  }

  val publicDimToForeignKeyMap : Map[String, String] =
    columns
      .filter(_.isForeignKey)
      .map(col => col.getForeignKeySource.get -> col.name)
      .toMap

  val sourceToForeignKeyColumnMap : Map[String, DimensionColumn] =
    columns
      .filter(_.isForeignKey)
      .map(col => col.getForeignKeySource.get -> col)
      .toMap

  val dimensionColumnsByNameMap: Map[String, DimensionColumn] = {
    columns.map(c => c.name -> c).toMap
  }

  val columnsByNameMap: Map[String, Column] = dimensionColumnsByNameMap

  private[this] def validate(): Unit = {
    engineValidations()
    validateDerivedExpression()
    ddlAnnotationValidations(ddlAnnotation)
    ColumnContext.validateColumnContext(columns, s"dim=$name")
    require(schemaColFilterMap.values.forall(columnNames.apply),
      s"Schema field mapping invalid for dim=$name, ${schemaColFilterMap.values} not found in $columnNames")
    validateDerivedFunction()
  }

  def validateForceFilterUniqueness(publicDimension : PublicDim): Unit = {
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
    val forcedFiltersByBasenameMap: Map[String, Filter] = publicDimension.forcedFilters.map{
      val backwardColByAlias: Map[String, String] =
        publicDimension.columns.map(col => col.alias -> col.name).toMap
      forceFilter =>
        val wrappedField = forceFilter.field
        if(backwardColByAlias.contains(wrappedField) && baseNameAliasMap.contains(backwardColByAlias(wrappedField))){
          baseNameAliasMap(backwardColByAlias(wrappedField)) -> forceFilter
        }
        else{
          throw new IllegalStateException(s"requirement failed: ${forceFilter.field} doesn't have a known alias")
        }
    }.toMap

    require(forcedFiltersByBasenameMap.size == publicDimension.forcedFilters.size, "Forced Filters public fact and map of forced base cols differ in size")

  }

  private[this] def ddlAnnotationValidations(ddLAnnotation: Option[DDLAnnotation]) : Unit = {
    if (ddlAnnotation.isDefined) {
      require(ddlAnnotation.get.asInstanceOf[EngineRequirement].acceptEngine(engine),
        s"Failed engine requirement dim=$name, engine=$engine, ddlAnnotation=$ddlAnnotation")
      engine match {
              case HiveEngine =>
                val da = ddlAnnotation.get.asInstanceOf[HiveDDLAnnotation]
                val columnOrderingSet = da.columnOrdering.toSet
                columns.filterNot(_.isDerivedColumn).filterNot(_.isInstanceOf[PartitionColumn]).foreach {
                  c =>
                    val nameOrAlias = c.alias.getOrElse(c.name)
                    require(columnOrderingSet.contains(nameOrAlias) || columnOrderingSet.contains(c.name), s"Dimension Column: $c is not present in column order set")
                }
              case any => /* Do nothing */
            }
    }
  }

  //validation for engine
  private[this] def engineValidations(): Unit ={
    columns.foreach { c =>
      c.annotations.filter(_.isInstanceOf[EngineRequirement]).map { er =>
        require(er.asInstanceOf[EngineRequirement].acceptEngine(engine),
          s"Failed engine requirement dim=$name, engine=$engine, col=${c.name}, annotation=$er")
      }
      if(c.isInstanceOf[EngineRequirement]) {
        require(c.asInstanceOf[EngineRequirement].acceptEngine(engine),
          s"Failed engine requirement dim=$name, engine=$engine, col=$c")
      }
    }
    annotations.filter(_.isInstanceOf[EngineRequirement]).map { er =>
      require(er.asInstanceOf[EngineRequirement].acceptEngine(engine),
        s"Failed engine requirement dim=$name, engine=$engine, annotation=$er")
    }
  }

  //validation for derived expression columns
  private[this] def validateDerivedExpression(): Unit = {
    columns.view.filter(_.isInstanceOf[DerivedDimensionColumn]).map(_.asInstanceOf[DerivedDimensionColumn]).foreach { c =>
      require(c.derivedExpression != null,
        s"Derived expression should be defined for a derived column $c")
      c.derivedExpression.sourceColumns.foreach { cn =>
        require(columnNames.contains(cn),
          s"Failed derived expression validation, unknown referenced column in dim=$name, $cn in $c")
        require(cn != c.name,
          s"Derived column is referring to itself, this will cause infinite loop : fact=$name, column=$cn, sourceColumns=${c.derivedExpression.sourceColumns}")
      }
      //validate we can render it
      c.derivedExpression.render(c.name)
    }
  }

  private[this] def validateDerivedFunction(): Unit = {
    columns.view.filter(_.isInstanceOf[BaseFunctionDimCol]).map(_.asInstanceOf[BaseFunctionDimCol]).foreach { c =>
      c.validate()
    }
  }

  def postValidate(publicDim: PublicDim) : Unit = {
    //add validation call for forced filters
    validateForceFilterUniqueness(publicDim)
  }
}

case class DimensionBuilder private[dimension](private val baseDim: Dimension, private var tableMap: Map[String, Dimension]) {
  def withAlternateEngine(name: String
                          , from: String
                          , engine: Engine
                          , columns: Set[DimensionColumn])  : DimensionBuilder = {
    require(!tableMap.contains(name), "should not export with existing table name")
    require(tableMap.nonEmpty, "no tables found")
    require(tableMap.contains(from), s"from table not found : $from")
    val fromTable = tableMap(from)

    withAlternateEngine(name, from, engine, columns, fromTable.maxDaysLookBack)
  }

  def withAlternateEngine(name: String
                          , from: String
                          , engine: Engine
                          , columns: Set[DimensionColumn]
                          , maxDaysLookBack: Option[Map[RequestType, Int]]
                          , annotations: Set[DimensionAnnotation] = Set.empty
                          , ddlAnnotation: Option[DDLAnnotation] = None
                          , schemas:Set[Schema] = Set.empty
                          , underlyingTableName: Option[String] = None)  : DimensionBuilder = {
    require(!tableMap.contains(name), "should not export with existing table name")
    require(tableMap.nonEmpty, "no tables found")
    require(tableMap.contains(from), s"from table not found : $from")

    val fromTable = tableMap(from)
    val newSchemas = if (schemas.isEmpty) fromTable.schemas else  schemas

    require(engine != fromTable.engine, s"Alternate must have different engine from source dim : $engine")
    require(!tableMap.values.find(_.from.exists(_.name == from)).exists(_.engine == engine),
      s"Alternate must have different engine from existing alternate engines : $engine")

    val newAltDim = new DimTable(
      name
      , fromTable.level - 1
      , engine
      , fromTable.dimLevel
      , newSchemas
      , columns
      , Option(fromTable)
      , fromTable.schemaColFilterMap
      , annotations
      , ddlAnnotation
      , fromTable.isDerivedDimension
      , fromTable.viewBaseTable
      , maxDaysLookBack
      , underlyingTableName
    )
    tableMap += newAltDim.name -> newAltDim
    this
  }

  // discard fact cols to create a subset of a fact table
  def createSubset(name: String
                   , from: String
                   , discarding: Set[String]
                   , schemas: Set[Schema] = Set.empty
                   , annotations: Set[DimensionAnnotation] = Set.empty
                   , ddlAnnotation: Option[DDLAnnotation] = None
                   , columnAliasMap : Map[String, String] = Map.empty
                   , resetAliasIfNotPresent: Boolean = false) : DimensionBuilder = {
    require(tableMap.nonEmpty, "no table to create subset from")
    require(tableMap.contains(from), s"from table not valid $from")
    require(!tableMap.contains(name), s"table $name already exists")
    require(discarding.nonEmpty, "discarding set should never be empty")

    val fromTable = tableMap(from)

    discarding foreach {
      d =>
        require(fromTable.columns.map(_.name).contains(d), s"column $d does not exist")
    }

    ColumnContext.withColumnContext { columnContext =>
      val columns = fromTable
        .columns
        .filter(dimCol =>  ( !dimCol.isDerivedColumn
        || (dimCol.isDerivedColumn
        && dimCol.asInstanceOf[DerivedColumn].derivedExpression.sourceColumns.intersect(discarding).isEmpty))).
        filter(c=> !discarding.contains(c.name)) map (_.copyWith(columnContext, columnAliasMap, resetAliasIfNotPresent))
      columns.foreach {
        c =>
          require(c.columnContext eq columnContext, "Failed to copy columns with new column context!")
      }
      val newSchemas = if(schemas.isEmpty) fromTable.schemas else schemas
      val newAnnotations = if(annotations.isEmpty) fromTable.annotations else annotations
      val newDDLAnnotation = if(ddlAnnotation.isEmpty) fromTable.ddlAnnotation else ddlAnnotation
      val newAltDim = new DimTable(
        name
        , fromTable.level - 1
        , fromTable.engine
        , fromTable.dimLevel
        , newSchemas
        , columns
        , Option(fromTable)
        , fromTable.schemaColFilterMap
        , newAnnotations
        , newDDLAnnotation
        , fromTable.isDerivedDimension
        , fromTable.viewBaseTable
        , fromTable.maxDaysLookBack
        , fromTable.underlyingTableName
      )
      tableMap += newAltDim.name -> newAltDim
    }
    this
  }

  def toPublicDimension(name: String, grainKey:String, columns: Set[PublicDimColumn], forcedFilters: Set[ForcedFilter] = Set.empty, revision: Int = 0, highCardinalityFilters: Set[Filter] = Set.empty) : PublicDimension = {
    var ts: TreeSet[Filter] = TreeSet.empty(Filter.baseFilterOrdering)
    ts ++= highCardinalityFilters
    val schemaTableColMap : Map[Schema, Map[String, String]] = {
      val allDims: Set[Dimension] = Set(baseDim) ++ tableMap.values
      val dimSchemColMap: Set[(String, Schema, String)] = allDims.flatMap(d => d.schemaColFilterMap.map(kv => (d.name, kv._1, kv._2)))
      dimSchemColMap.groupBy(_._2).mapValues(_.map(tpl => tpl._1 -> tpl._3).toMap)
    }
    schemaTableColMap.foreach { case (schema, nameColMap) =>
      val groupedByCol: Map[String, Map[String, String]] = nameColMap.groupBy {
        case (name, col) => col
      }
      val groupFlattened = groupedByCol.map {
        case (col, nameColMap) => col -> nameColMap.keys.toSet
      }
      require(groupFlattened.size == 1, s"Schema $schema requires different columns across underlying tables : $groupFlattened")
    }
    new PublicDim(name, grainKey, baseDim, columns, tableMap, forcedFilters, ts, revision)
  }

  @VisibleForTesting
  def tableDefs : Map[String, Dimension] = tableMap
}

trait PublicDimColumn extends PublicColumn

case class PubCol(name: String
                  , alias: String
                  , filters: Set[FilterOperation]
                  , dependsOnColumns: Set[String] = Set.empty
                  , incompatibleColumns: Set[String] = Set.empty
                  , required: Boolean = false
                  , hiddenFromJson: Boolean = false
                  , filteringRequired: Boolean = false
                  , restrictedSchemas: Set[Schema] = Set.empty
                  , isImageColumn: Boolean = false
                  , isReplacement: Boolean = false) extends PublicDimColumn

case class RequiredAlias(name: String, alias: String, isKey: Boolean)
trait PublicDimension extends PublicTable {
  def revision: Int

  def name: String

  def grainKey: String

  def dimLevel: DimLevel

  def schemas: Set[Schema]

  def schemaRequiredAlias(schema: Schema): Option[RequiredAlias]

  //def forSchema(schema: Schema): Option[Map[Engine, Dimension]]
  //def forEngine(engine: Engine, schema: Schema) : Option[Dimension]
  def forColumns(engine: Engine, schema: Schema, columns: Set[String]): Option[Dimension]

  def dimList: Iterable[Dimension]

  /**
   * Contains column alias set excluding primary key
 *
   * @return
   */
  def columnsByAlias: Set[String]

  /**
   * Contains aliases to name map excluding primary key
 *
   * @return
   */
  def aliasToNameMap: Map[String, String]

  /**
   * Contains aliases to name map for all columns
 *
   * @return
   */
  def aliasToNameMapFull: Map[String, String]

  def nameToDataTypeMap: Map[String, DataType]

  def keyColumnToAliasMap: Map[String, String]

  def primaryKeyByAlias: String

  def foreignKeySources: Set[String]

  def partitionColumnAliases: Set[String]

  def partitionColumns : Set[PublicDimColumn]

  def foreignKeyByAlias: Set[String]

  def isPrimaryKeyAlias(alias: String): Boolean = alias == primaryKeyByAlias

  def getBaseDim: Dimension

  def containsHighCardinalityFilter(f: Filter) : Boolean
}

object PublicDimension {
  implicit val ordering: Ordering[PublicDimension] = Ordering.by(dc => s"${dc.dimLevel.level}-${dc.name}")
}

case class PublicDim (name: String
                     ,grainKey: String
                      , baseDim: Dimension
                      , columns: Set[PublicDimColumn]
                      , dims: Map[String, Dimension]
                      , forcedFilters: Set[ForcedFilter]
                      , private val highCardinalityFilters: Set[Filter]
                      , revision: Int = 0) extends PublicDimension {

  def dimList : Iterable[Dimension] = dims.values

  validate()

  val schemas: Set[Schema] = dims.values.map(_.schemas).flatten.toSet
  val forcedFiltersByAliasMap: Map[String, ForcedFilter] = forcedFilters.map(f => f.field -> f).toMap
  val columnsByAliasMap: Map[String, PublicColumn] = columns.map(pdc => pdc.alias -> pdc).toMap
  val nameToDataTypeMap: Map[String, DataType] = dims.values.flatMap(_.columns).map(d=> (d.name,d.dataType)).toMap


  private[this] val dimMap: Map[(Engine, Schema), SortedSet[Dimension]] =
    dims.values
      .flatMap { d => for { s <- d.schemas } yield ((d.engine, s), d) }
      .groupBy(_._1)
      .mapValues(_.map(_._2).to[SortedSet])
  //private[this] val dimSchemaMap: Map[Schema, Map[Engine, Dimension]] =
  //  dims.mapValues(d => d.schemas.map(s => (s, d))).flatten.groupBy(_._1).mapValues(_.map(tpl => (tpl._2.engine, tpl._2)).toMap)
  //private[this] val dimEngineSchemaMap: Map[(Engine, Schema), Dimension] =
  //  dims.map(d => d.schemas.map(s => ((d.engine, s), d))).flatten.toMap

  //if pub column is a primary key in any of the dims
  val primaryKeyByAlias: String = {
    val pks = columns.filter(col => dims.values.exists(_.primaryKey == col.name)).map(_.alias)
    require(pks.size == 1, s"Each public dimension must have 1 primary key, dim = $name, pks = $pks")
    pks.head
  }

  //non primary key column aliases
  val columnsByAlias: Set[String] = 
    columns.filter(col => dims.values.exists(d => !(d.primaryKey == col.name))).map(_.alias)
  
  val allColumnsByAlias: Set[String] = columnsByAlias + primaryKeyByAlias

  val aliasToNameMap: Map[String, String] = 
    columns.filter(col => dims.values.exists(d => !(d.primaryKey == col.name))).map(col => (col.alias, col.name)).toMap

  val aliasToNameMapFull: Map[String, String] = columns.map(col => (col.alias, col.name)).toMap

  val keyColumnToAliasMap : Map[String, String] =
    columns.filter(col => dims.values.flatMap(_.columns).filter(_.isForeignKey).map(_.name).toSet.contains(col.name)).
      map(c=> (c.name, c.alias)).toMap

  val foreignKeySources: Set[String] = {
    dims.values.map(_.columns.map(_.getForeignKeySource).flatten).flatten.toSet
  }

  val partitionColumnAliases: Set[String] = {
    columns.filter(col => dims.values.flatMap(_.columns).filter(_.isInstanceOf[PartitionColumn]).map(_.name).toSet.contains(col.name)).map(_.alias)
  }

  val partitionColumns :Set[PublicDimColumn] =
    columns.filter(col => dims.values.flatMap(_.columns).filter(_.isInstanceOf[PartitionColumn]).map(_.name).toSet.contains(col.name))

  val foreignKeyByAlias = 
    columns.filter(col => dims.values.flatMap(_.columns).filter(_.isForeignKey).map(_.name).toSet.contains(col.name)).map(_.alias)

  val requiredAliases : Set[String] = columns.filter(_.required).map(_.alias)

  val dependentColumns: Set[String] = columns.filter(_.dependsOnColumns.nonEmpty).map(_.alias)

  val incompatibleColumns: Map[String, Set[String]] = columns.filter(_.incompatibleColumns.nonEmpty).map(col => (col.alias, col.incompatibleColumns)).toMap

  val requiredFilterAliases : Set[String] = columns.filter(_.filteringRequired).map(_.alias)

  val restrictedSchemasMap: Map[String, Set[Schema]] = columns.filter(_.restrictedSchemas.nonEmpty).map(col => (col.alias, col.restrictedSchemas)).toMap

  private[this] val aliasColumnMap = columns.map(col => (col.alias, col.name)).toMap
  private[this] val columnAliasMap = columns.map(col => (col.name, col.alias)).toMap
  private[this] val schemaRequiredAliasMap: Map[Schema, RequiredAlias] = baseDim.schemas.collect {
    case schema if baseDim.schemaColFilterMap.contains(schema) =>
      val name = baseDim.schemaColFilterMap(schema)
      schema -> RequiredAlias(name, columnAliasMap(name), baseDim.columnsByNameMap(name).isKey)
  }.toMap

  validateForcedFilters()

  highCardinalityFilters.foreach {
    filter =>
      require(filter.canBeHighCardinalityFilter, s"Filter can not be high cardinality filter : $filter")
      require(allColumnsByAlias(filter.field), s"Unknown high cardinality filter : ${filter.field}")
  }

  private[this] val highCardinalityFilterInValuesMap: Map[String, Set[String]] = {
    highCardinalityFilters
      .filter(f => f.isInstanceOf[InFilter] || f.isInstanceOf[EqualityFilter] || f.isInstanceOf[LikeFilter])
      .groupBy(_.field)
      .mapValues {
        filterSet =>
          filterSet.collect {
            case InFilter(_, values, _, _) =>
              values.toSet
            case EqualityFilter(_, value, _, _) =>
              Set(value)
            case LikeFilter(_, _, _, _) =>
              Set.empty
          }.flatten
      }
  }

  private[this] val highCardinalityFilterNotInValuesMap: Map[String, Set[String]] = {
    highCardinalityFilters
      .filter(f => f.isInstanceOf[NotInFilter] || f.isInstanceOf[NotEqualToFilter])
      .groupBy(_.field)
      .mapValues {
        filterSet =>
          filterSet.collect {
            case NotInFilter(_, values, _, _) =>
              values.toSet
            case NotEqualToFilter(_, value, _, _) =>
              Set(value)
          }.flatten
      }
  }

  postValidate()

  private[this] def validate(): Unit = {
    //validate non empty dim map
    require(dims.nonEmpty, "dims should not be empty")

    //base dim must have all public columns
    val result = columns.forall {
      column => baseDim.columnsByNameMap.contains(column.name)
    }
    require(result, s"publicDim=$name, base dim must have all columns!")
  }

  private[this] def postValidate() : Unit = {
    dims.values.foreach(_.postValidate(this))
  }

  override def schemaRequiredAlias(schema: Schema) : Option[RequiredAlias] = schemaRequiredAliasMap.get(schema)

  override def dimLevel: DimLevel = baseDim.dimLevel

  override def getBaseDim: Dimension = baseDim

  //override def forEngine(engine: Engine, schema: Schema): Option[Dimension] = dimEngineSchemaMap.get((engine, schema))

  //override def forSchema(schema: Schema): Option[Map[Engine, Dimension]] = dimSchemaMap.get(schema)
  def forColumns(engine: Engine, schema: Schema, columnAliases: Set[String]) : Option[Dimension] = {
    val dimCandidates = dimMap.get((engine, schema))
    val columns = columnAliases.map(aliasToNameMapFull.apply)
    dimCandidates.flatMap(_.find(d => columns.forall(d.columnNames.apply)))
  }

  private[this] def containsHighCardinalityInclusiveFilter(f:Filter): Boolean = {
    f match {
      case InFilter(field, values, _, _) =>
        val highCardinalityValues = highCardinalityFilterInValuesMap(f.field)
        values.exists(highCardinalityValues.apply)
      case EqualityFilter(field, value, _, _) =>
        val highCardinalityValues = highCardinalityFilterInValuesMap(f.field)
        highCardinalityValues(value)
      case PushDownFilter(f) => containsHighCardinalityInclusiveFilter(f)
      case LikeFilter(_, _, _, _) =>
        true
      case _ => false
    }
  }

  private[this] def containsHighCardinalityExclusiveFilter(f:Filter): Boolean = {
    f match {
      case NotInFilter(field, values, _, _) =>
        val highCardinalityValues = highCardinalityFilterNotInValuesMap(f.field)
        val inclusiveHighCardinalityValuesOption = highCardinalityFilterInValuesMap.get(f.field)
        values.exists(highCardinalityValues.apply) && !inclusiveHighCardinalityValuesOption.exists(inSet => values.exists(inSet.apply))
      case NotEqualToFilter(field, value, _, _) =>
        val highCardinalityValues = highCardinalityFilterNotInValuesMap(f.field)
        highCardinalityValues(value)
      case PushDownFilter(f) => containsHighCardinalityExclusiveFilter(f)
      case _ => false
    }
  }

  def containsHighCardinalityFilter(f: Filter) : Boolean = {
    val inclusiveCheck: Boolean = if(highCardinalityFilterInValuesMap.contains(f.field)) {
      containsHighCardinalityInclusiveFilter(f)
    } else false
    val exclusiveCheck: Boolean = if(highCardinalityFilterNotInValuesMap.contains(f.field)) {
      containsHighCardinalityExclusiveFilter(f)
    } else false
    inclusiveCheck || exclusiveCheck
  }
}

