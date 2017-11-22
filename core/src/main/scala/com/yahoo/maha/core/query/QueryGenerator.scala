// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension._

import scala.collection.mutable

/**
 * Created by jians on 10/20/15.
 */

class QueryBuilderContext {
  private[this] var aliasIndex = 0
  private[this] val tableAliasMap = new mutable.HashMap[String, String]
  // from alias -> tablename.alias mapping
  private[this] val colAliasToFactColNameMap = new mutable.HashMap[String, String]
  private[this] val colAliasToFactColExpressionMap = new mutable.HashMap[String, String]
  private[this] val colAliasToDimensionColNameMap = new mutable.HashMap[String, String]

  private[this] val colAliasToDimensionMap = new mutable.HashMap[String, PublicDimension]

  private[this] val factAliasToColumnMap = new mutable.HashMap[String, Column]()
  private[this] val dimensionAliasToColumnMap = new mutable.HashMap[String, DimensionColumn]()

  private[this] val preOuterAliasToColumnMap = new mutable.HashMap[String, Column]()
  private[this] val preOuterFinalAliasToAliasMap = new mutable.HashMap[String, String]()

  private[this] val publicDimensionAliasTupleToFinalAlias = new mutable.HashMap[(PublicDimension,String), String]()

  private[this] val columnNames = new mutable.TreeSet[String]

  def getAliasForTable(name: String) : String = {
    tableAliasMap.get(name) match {
      case None => {
        val alias = s"${name.split("_").map(_.substring(0, 1)).mkString("")}$aliasIndex"
        aliasIndex += 1
        tableAliasMap += name -> alias
        alias
      }
      case Some(alias) => alias
    }
  }

  def getAliasForField(TableName: String, fieldName: String) : String = {
    tableAliasMap.get(TableName) match {
      case None => {
        val alias = s"${TableName.split("_").map(_.substring(0, 1)).mkString("")}$aliasIndex"
        aliasIndex += 1
        tableAliasMap += TableName -> alias
        s"${alias}_$fieldName"
      }
      case Some(alias) => s"${alias}_$fieldName"
    }
  }
  
  def getDimensionColByAlias(alias: String) : DimensionColumn = {
    dimensionAliasToColumnMap(alias)
  }

  def isDimensionCol(alias: String) : Boolean = {
    dimensionAliasToColumnMap.contains(alias)
  }

  def getFactColByAlias(alias: String) : Column = {
    factAliasToColumnMap(alias)
  }

  def getDimensionForColAlias(alias: String) : PublicDimension = {
    colAliasToDimensionMap(alias)
  }

  def setDimensionColAlias(alias: String, finalAlias: String, dimensionColumn: DimensionColumn, pubDim: PublicDimension): Unit = {
    dimensionAliasToColumnMap.put(alias, dimensionColumn)
    colAliasToDimensionColNameMap.put(alias, finalAlias)
    setColAliasAndPublicDimAlias(alias, finalAlias, dimensionColumn, pubDim)
  }

  def setDimensionColAliasForDimOnlyQuery(alias: String, finalAlias: String, dimensionColumn: DimensionColumn, pubDim: PublicDimension): Unit = {
    if (!dimensionAliasToColumnMap.contains(alias)) {
      setDimensionColAlias(alias, finalAlias, dimensionColumn, pubDim)
    } else {
      if (dimensionAliasToColumnMap(alias).isForeignKey) {
        dimensionAliasToColumnMap.put(alias, dimensionColumn)
        colAliasToDimensionColNameMap.put(alias, finalAlias)
      }
      setColAliasAndPublicDimAlias(alias, finalAlias, dimensionColumn, pubDim)
    }
  }

  def setColAliasAndPublicDimAlias(alias: String, finalAlias: String, dimensionColumn: DimensionColumn, pubDim: PublicDimension) : Unit = {
    columnNames += dimensionColumn.name
    colAliasToDimensionMap.put(alias, pubDim)
    publicDimensionAliasTupleToFinalAlias.put((pubDim, alias), finalAlias)
  }

  def getPublicDimensionAliasTupleToFinalAlias (pubDim: PublicDimension, alias: String): Option[String] = {
    publicDimensionAliasTupleToFinalAlias.get((pubDim,alias))
  }

  def setFactColAliasAndExpression(alias: String, finalAlias: String, factColumn: Column, exp: Option[String]): Unit = {
    colAliasToFactColNameMap.put(alias, finalAlias)
    if(exp.isDefined) {
      colAliasToFactColExpressionMap.put(alias, exp.get)
    }
    factAliasToColumnMap.put(alias, factColumn)
    columnNames += factColumn.name
  }

  def setFactColAlias(alias: String, finalAlias: String, factColumn: Column): Unit = {
    colAliasToFactColNameMap.put(alias, finalAlias)
    factAliasToColumnMap.put(alias, factColumn)
    columnNames += factColumn.name
  }
  
  def markDuplicateFactCol(alias: String, finalAlias: String, factColumn: Column): Unit = {
    colAliasToFactColNameMap.put(alias, finalAlias)
    factAliasToColumnMap.put(alias, factColumn)
    columnNames += factColumn.name
  }

  def getDimensionColNameForAlias(alias: String) : String = {
    require(colAliasToDimensionColNameMap.contains(alias), s"dim alias does not exist in inner selection : $alias")
    colAliasToDimensionColNameMap(alias)
  }

  def getFactColNameForAlias(alias: String) : String = {
    require(colAliasToFactColNameMap.contains(alias), s"fact alias does not exist in inner selection : $alias")
    colAliasToFactColNameMap(alias)
  }

  def getFactColExpressionOrNameForAlias(alias: String) : String = {
    require(colAliasToFactColNameMap.contains(alias), s"fact alias does not exist in inner selection : $alias")
    colAliasToFactColExpressionMap.getOrElse(alias,colAliasToFactColNameMap(alias))
  }

  def setPreOuterAliasToColumnMap(alias:String, finalAlias: String, column: Column): Unit = {
    preOuterAliasToColumnMap.put(finalAlias, column)
    preOuterFinalAliasToAliasMap.put(finalAlias, alias)
  }

  def getPreOuterFinalAliasToAliasMap(alias :String): Option[String] = {
    preOuterFinalAliasToAliasMap.get(alias)
  }

  def getPreOuterAliasToColumnMap(alias: String): Option[Column] = {
    preOuterAliasToColumnMap.get(alias)
  }

  def containsPreOuterAlias(alias: String): Boolean = {
    preOuterAliasToColumnMap.contains(alias)
  }

  def containsFactColNameForAlias(alias: String): Boolean = {
    colAliasToFactColNameMap.contains(alias)
  }

  def containsFactAliasToColumnMap(alias :String) : Boolean = {
    factAliasToColumnMap.contains(alias)
  }
  
  def containsColByName(name: String) : Boolean = columnNames.contains(name)

  def aliasColumnMap : Map[String, Column] = dimensionAliasToColumnMap.toMap ++ factAliasToColumnMap.toMap

  def getColAliasToFactColNameMap : scala.collection.Map[String, String] = colAliasToFactColNameMap
}

trait QueryGenerator[T <: EngineRequirement] {
  def generate(queryContext: QueryContext): Query
  def engine: Engine
}

trait BaseQueryGenerator[T <: EngineRequirement] extends QueryGenerator[T] {

  def removeDuplicateIfForced(localFilters: Seq[Filter], forcedFilters: Seq[ForcedFilter], inputContext: FactualQueryContext): Array[Filter] = {
    val queryContext = inputContext

    val fact = queryContext.factBestCandidate.fact
    val returnedFilters = new mutable.LinkedHashMap[String, Filter]
    localFilters.foreach {
      filter =>
        val name = queryContext.factBestCandidate.publicFact.aliasToNameColumnMap( filter.field )
        val column = fact.columnsByNameMap( name )
        val real_name = column.alias.getOrElse( name )
        returnedFilters( real_name ) = filter
    }
    forcedFilters.foreach {
      filter =>
        val name = queryContext.factBestCandidate.publicFact.aliasToNameColumnMap( filter.field )
        val column = fact.columnsByNameMap( name )
        val real_name = column.alias.getOrElse( name )
        if( !filter.isOverridable || !returnedFilters.contains( real_name )) {
          returnedFilters( real_name ) = filter
        }
    }
        returnedFilters.values.toArray
  }
}

class QueryGeneratorRegistry {
  private[this] var queryGeneratorRegistry : Map[Engine, QueryGenerator[_]] = Map.empty

  def isEngineRegistered(engine: Engine): Boolean = synchronized {
    queryGeneratorRegistry.contains(engine)
  }

  def register[U <: QueryGenerator[_]](engine: Engine, qg: U) : Unit = synchronized {
    require(!queryGeneratorRegistry.contains(engine), s"Query generator already defined for engine : $engine")
    queryGeneratorRegistry += (engine -> qg)
  }

  def getGenerator(engine: Engine): Option[QueryGenerator[EngineRequirement]] =
    queryGeneratorRegistry.get(engine).asInstanceOf[Option[QueryGenerator[EngineRequirement]]]
}
