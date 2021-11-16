package com.yahoo.maha.core.query

import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact._

import scala.collection.mutable

trait BigqueryHivePrestoQueryCommon {

  val hiveLiteralMapper = new HiveLiteralMapper()
  val prestoLiteralMapper = new HiveLiteralMapper()
  val bigqueryLiteralMapper = new BigqueryLiteralMapper()

  def generateOrderByClause(queryContext: DimFactOuterGroupByQueryQueryContext,
                            queryBuilder: QueryBuilder,
                            queryBuilderContext: QueryBuilderContext): Unit = {
    val model = queryContext.requestModel
    model.requestSortByCols.map {
      case FactSortByColumnInfo(alias, order) if queryBuilderContext.containsPreOuterFinalAliasToAliasMap(alias) =>
        val preOuterAliasOption = queryBuilderContext.getPreOuterFinalAliasToAliasMap(alias)
        s"${preOuterAliasOption.get} ${order}"
      case FactSortByColumnInfo(alias, order) =>
        val colExpression = queryBuilderContext.getFactColNameForAlias(alias)
        s"$colExpression ${order}"
      case DimSortByColumnInfo(alias, order) =>
        val dimColName = queryBuilderContext.getDimensionColNameForAlias(alias)
        s"$dimColName ${order}"
      case a => throw new IllegalArgumentException(s"Unhandled SortByColumnInfo $a")
    }.foreach(queryBuilder.addOrderBy(_))
  }

  /*
  Recursively visits all the derived columns and store the primitive columns in the set.
   */
  def dfsGetPrimitiveCols(fact:Fact, derivedCols: IndexedSeq[Column], primitiveColsSet:mutable.LinkedHashSet[(String, Column)], engine: Engine): Unit = {
    derivedCols.foreach {
      case derCol:DerivedColumn =>
        derCol.derivedExpression.sourceColumns.toList.sorted.foreach {
          sourceCol =>
            val colOption = fact.columnsByNameMap.get(sourceCol)
            require(colOption.isDefined, s"Failed to find the sourceColumn $sourceCol in fact ${fact.name}")
            val col = colOption.get
            if(col.isDerivedColumn) {
              dfsGetPrimitiveCols(fact, IndexedSeq(col.asInstanceOf[DerivedColumn]), primitiveColsSet, engine)
            } else {
              val name = col.alias.getOrElse(col.name)
              if (!primitiveColsSet.contains((name, col))) {
                primitiveColsSet.add((name, col))
              }
            }
        }
      case derCol : FactCol =>
        require(
          engine == HiveEngine && derCol.rollupExpression.isInstanceOf[HiveCustomRollup] ||
          engine == PrestoEngine && derCol.rollupExpression.isInstanceOf[PrestoCustomRollup] ||
          engine == BigqueryEngine && derCol.rollupExpression.isInstanceOf[BigqueryCustomRollup],
          s"Unexpected Rollup expression ${derCol.rollupExpression} in finding primitive cols")
        if (engine == HiveEngine) {
          val customRollup = derCol.rollupExpression.asInstanceOf[HiveCustomRollup]
          customRollup.expression.sourceColumns.toList.sorted.foreach {
            sourceCol =>
              val colOption = fact.columnsByNameMap.get(sourceCol)
              require(colOption.isDefined, s"Failed to find the sourceColumn $sourceCol in fact ${fact.name}")
              val col = colOption.get
              if (col.isDerivedColumn) {
                dfsGetPrimitiveCols(fact, IndexedSeq(col.asInstanceOf[DerivedColumn]), primitiveColsSet, engine)
              } else {
                col match {
                  case fcol: FactCol if !fcol.rollupExpression.isInstanceOf[HiveCustomRollup] =>
                    val name = col.alias.getOrElse(col.name)
                    if (!primitiveColsSet.contains((name, col))) {
                      primitiveColsSet.add((name, col))
                    }
                  case _ =>
                }
              }
          }
        }
        else if (engine == BigqueryEngine) {
          val customRollup = derCol.rollupExpression.asInstanceOf[BigqueryCustomRollup]
          customRollup.expression.sourceColumns.toList.sorted.foreach {
            sourceCol =>
              val colOption = fact.columnsByNameMap.get(sourceCol)
              require(colOption.isDefined, s"Failed to find the sourceColumn $sourceCol in fact ${fact.name}")
              val col = colOption.get
              if (col.isDerivedColumn) {
                dfsGetPrimitiveCols(fact, IndexedSeq(col.asInstanceOf[DerivedColumn]), primitiveColsSet, engine)
              } else {
                col match {
                  case fcol: FactCol if !fcol.rollupExpression.isInstanceOf[BigqueryCustomRollup] =>
                    val name = col.alias.getOrElse(col.name)
                    if (!primitiveColsSet.contains((name, col))) {
                      primitiveColsSet.add((name, col))
                    }
                  case _ =>
                }
              }
          }
        }
        else { //engine == PrestoEngine
          val customRollup = derCol.rollupExpression.asInstanceOf[PrestoCustomRollup]
          customRollup.expression.sourceColumns.toList.sorted.foreach {
            sourceCol =>
              val colOption = fact.columnsByNameMap.get(sourceCol)
              require(colOption.isDefined, s"Failed to find the sourceColumn $sourceCol in fact ${fact.name}")
              val col = colOption.get
              if (col.isDerivedColumn) {
                dfsGetPrimitiveCols(fact, IndexedSeq(col.asInstanceOf[DerivedColumn]), primitiveColsSet, engine)
              } else {
                col match {
                  case fcol: FactCol if !fcol.rollupExpression.isInstanceOf[PrestoCustomRollup] =>
                    val name = col.alias.getOrElse(col.name)
                    if (!primitiveColsSet.contains((name, col))) {
                      primitiveColsSet.add((name, col))
                    }
                  case _ =>
                }
              }
          }
        }

      case e =>
        throw new IllegalArgumentException(s"Unexpected column case found in the dfsGetPrimitiveCols $e")
    }
  }

  /*
method to crawl the NoopRollup fact cols recursively and fill up the parent column
 whose dependent source columns is/are NoopRollup column.
 All such parent noop rollup columns has to be rendered at OuterGroupBy layer
 */
  def dfsNoopRollupCols(fact:Fact, cols: Set[(Column, String)], parentList: List[(Column, String)], noopRollupColSet: mutable.LinkedHashSet[(String, Column)]): Unit = {
    cols.foreach {
      case (col, alias)=>
        col match {
          case factCol@FactCol(_, dt, cc, rollup, _, annotations, _) =>
            rollup match {
              case HiveCustomRollup(e) =>
                parseCustomRollup(e, col, alias)
              case BigqueryCustomRollup(e) =>
                parseCustomRollup(e, col, alias)
              case PrestoCustomRollup(e) =>
                parseCustomRollup(e, col, alias)
              case NoopRollup =>
                pickupLeaf(col, alias)
              case _=> //ignore all other rollup cases
            }
          case derCol@HiveDerFactCol(_, _, dt, cc, de, annotations, rollup, _) =>
            rollup match {
              case HiveCustomRollup(e) =>
                parseCustomRollup(e, col, alias)
              case NoopRollup if grepHasRollupExpression(fact, de.expression) =>
                // If NoopRollup column has Aggregate/rollup Expression then push it inside for OGB
                pickupLeaf(col, alias)
              case _=> //ignore all other rollup cases
            }
            if (rollup != NoopRollup) {
              de.sourceColumns.toList.sorted.foreach {
                sourceColName =>
                  val colOption = fact.columnsByNameMap.get(sourceColName)
                  require(colOption.isDefined, s"Failed to find the sourceColumn $sourceColName in fact ${fact.name}")
                  val sourceCol = colOption.get
                  val sourceColAlias = sourceCol.alias.getOrElse(sourceCol.name)
                  if (col.alias.getOrElse(col.name) != sourceColAlias) {
                    // avoid adding self dependent columns
                    dfsNoopRollupCols(fact, Set((sourceCol, sourceColAlias)), parentList++List((col, alias)), noopRollupColSet)
                  }
              }
            }
          case derCol@BigqueryDerFactCol(_, _, dt, cc, de, annotations, rollup, _) =>
            rollup match {
              case BigqueryCustomRollup(e) =>
                parseCustomRollup(e, col, alias)
              case NoopRollup if grepHasRollupExpression(fact, de.expression) =>
                // If NoopRollup column has Aggregate/rollup Expression then push it inside for OGB
                pickupLeaf(col, alias)
              case _=>
            }
            if (rollup != NoopRollup) {
              de.sourceColumns.toList.sorted.foreach {
                sourceColName =>
                  val colOption = fact.columnsByNameMap.get(sourceColName)
                  require(colOption.isDefined, s"Failed to find the sourceColumn $sourceColName in fact ${fact.name}")
                  val sourceCol = colOption.get
                  val sourceColAlias = sourceCol.alias.getOrElse(sourceCol.name)
                  if (col.alias.getOrElse(col.name) != sourceColAlias) {
                    // avoid adding self dependent columns
                    dfsNoopRollupCols(fact, Set((sourceCol, sourceColAlias)), parentList++List((col, alias)), noopRollupColSet)
                  }
              }
            }
          case derCol@PrestoDerFactCol(_, _, dt, cc, de, annotations, rollup, _) =>
            rollup match {
              case PrestoCustomRollup(e) =>
                parseCustomRollup(e, col, alias)
              case NoopRollup if grepHasRollupExpression(fact, de.expression) =>
                // If NoopRollup column has Aggregate/rollup Expression then push it inside for OGB
                pickupLeaf(col, alias)
              case _=> //ignore all other rollup cases
            }
            if (rollup != NoopRollup) {
              de.sourceColumns.toList.sorted.foreach {
                sourceColName =>
                  val colOption = fact.columnsByNameMap.get(sourceColName)
                  require(colOption.isDefined, s"Failed to find the sourceColumn $sourceColName in fact ${fact.name}")
                  val sourceCol = colOption.get
                  val sourceColAlias = sourceCol.alias.getOrElse(sourceCol.name)
                  if (col.alias.getOrElse(col.name) != sourceColAlias) {
                    // avoid adding self dependent columns
                    dfsNoopRollupCols(fact, Set((sourceCol, sourceColAlias)), parentList++List((col, alias)), noopRollupColSet)
                  }
              }
            }
          case _=>
          //ignore all dim cols cases
        }
    }

    def parseCustomRollup(expression: DerivedExpression[String], col : Column, alias : String): Unit = {
      expression.sourceColumns.toList.sorted.foreach {
        case sourceColName =>
          val colOption = fact.columnsByNameMap.get(sourceColName)
          require(colOption.isDefined, s"Failed to find the sourceColumn $sourceColName in fact ${fact.name}")
          val sourceCol = colOption.get
          val sourceColAlias = sourceCol.alias.getOrElse(sourceCol.name)
          if (col.alias.getOrElse(col.name) != sourceColAlias) {
            // avoid adding self dependent columns
            dfsNoopRollupCols(fact, Set((sourceCol, sourceColAlias)), parentList++List((col, alias)), noopRollupColSet)
          }
      }
    }

    /*
    Grep hasRollupExpression recursively in the all the N Derived level dependent columns
     */
    def grepHasRollupExpression(fact:Fact, expression: Expression[String]): Boolean = {
      if(expression.hasRollupExpression) {
        true
      } else  {
        // Matching case for non rollup expression recursively
        expression match {
          case col@(PrestoExpression.COL(_, _, _) | BigqueryExpression.COL(_, _, _) | HiveExpression.COL(_, _, _)) =>
            val pattern = "\\{([^}]+)\\}".r
            val colNames = pattern.findAllIn(col.asString).toSet
            for (colName <- colNames) {
              val colNameParsed = colName.replaceFirst("\\{", "").replaceFirst("\\}", "").trim
              val colOption = fact.columnsByNameMap.get(colNameParsed)
              val hasRollupExpr = if(colOption.isDefined) {
                colOption.get match {
                  case HiveDerFactCol(_, _, dt, cc, de, annotations, rollup, _) =>
                    grepHasRollupExpression(fact, de.expression)
                  case BigqueryDerFactCol(_, _, dt, cc, de, annotations, rollup, _) =>
                    grepHasRollupExpression(fact, de.expression)
                  case PrestoDerFactCol(_, _, dt, cc, de, annotations, rollup, _) =>
                    grepHasRollupExpression(fact, de.expression)
                  case _=> expression.hasRollupExpression
                }
              } else expression.hasRollupExpression
              if (!hasRollupExpr) {
                return false
              }
            }
            true
          case HiveExpression.ROUND(hiveExp, _)=> grepHasRollupExpression(fact, hiveExp)
          case HiveExpression.COALESCE(hiveExp, _)=> grepHasRollupExpression(fact, hiveExp)
          case HiveExpression.NVL(hiveExp, _)=> grepHasRollupExpression(fact, hiveExp)
          case BigqueryExpression.ROUND(bigqueryExp, _) => grepHasRollupExpression(fact, bigqueryExp)
          case BigqueryExpression.COALESCE(bigqueryExp, _) => grepHasRollupExpression(fact, bigqueryExp)
          case BigqueryExpression.NVL(bigqueryExp, _) => grepHasRollupExpression(fact, bigqueryExp)
          case PrestoExpression.ROUND(prestoExp, _)=> grepHasRollupExpression(fact, prestoExp)
          case PrestoExpression.COALESCE(prestoExp, _)=> grepHasRollupExpression(fact, prestoExp)
          case PrestoExpression.NVL(prestoExp, _)=> grepHasRollupExpression(fact, prestoExp)
          case any => any.hasRollupExpression
        }
      }
    }

    /*
       Pick up the root of the NoopRollup dependent column
     */
    def pickupLeaf(col : Column, alias : String): Unit = {
      val parentCol =  parentList.reverse.headOption
      if(parentCol.isDefined) {
        noopRollupColSet.add(parentCol.get._2, parentCol.get._1)
      } else {
        noopRollupColSet.add(alias, col)
      }
    }
  }

  /*
     Commonly used method
   */
  def renderParentOuterDerivedFactCols(queryBuilderContext:QueryBuilderContext, projectedAlias:String, column:Column): String = {
    val renderedAlias = projectedAlias

    column match {
      case HiveDerDimAggregateCol(_, dt, cc, de, _, annotations, _) =>
        queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
        s"""$renderedAlias"""
      case HiveDerDimCol(_, dt, cc, de, _, annotations, _) =>
        queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
        s"""$renderedAlias"""
      case BigqueryDerDimCol(_, dt, cc, de, _, annotations, _) =>
        queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
        s"""$renderedAlias"""
      case PrestoDerDimCol(_, dt, cc, de, _, annotations, _) =>
        queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
        s"""$renderedAlias"""
      case HivePartDimCol(_, dt, cc, _, annotations, _) =>
        queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
        s"""$renderedAlias"""
      case BigqueryPartDimCol(_, dt, cc, _, annotations, _) =>
        queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
        s"""$renderedAlias"""
      case PrestoPartDimCol(_, dt, cc, _, annotations, _) =>
        queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
        s"""$renderedAlias"""
      case DimCol(_, dt, cc, _, annotations, _) if queryBuilderContext.isDimensionCol(projectedAlias) =>
        queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
        val innerColAlias = queryBuilderContext.getDimensionColNameForAlias(projectedAlias)
        s"""$innerColAlias AS $renderedAlias"""
      case DimCol(_, dt, cc, _, annotations, _) =>
        val innerAlias = renderColumnAlias(projectedAlias)
        val renderedAlias = s"""$innerAlias AS $projectedAlias"""
        queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
        renderedAlias
      case ConstDimCol(_, dt, value, cc, _, annotations, _) =>
        val innerAlias = renderColumnAlias(projectedAlias)
        val renderedAlias = s"""$innerAlias AS $projectedAlias"""
        queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
        renderedAlias
      case factCol@FactCol(_, dt, cc, rollup, _, annotations, _) if factCol.rollupExpression.isInstanceOf[HiveCustomRollup]=>
        val name = factCol.alias.getOrElse(factCol.name)
        val de = factCol.rollupExpression.asInstanceOf[HiveCustomRollup].expression
        queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
        s"""${de.render(name, Map.empty)} AS $renderedAlias"""
      case factCol@FactCol(_, dt, cc, rollup, _, annotations, _) if factCol.rollupExpression.isInstanceOf[BigqueryCustomRollup] =>
        val name = factCol.alias.getOrElse(factCol.name)
        val de = factCol.rollupExpression.asInstanceOf[BigqueryCustomRollup].expression
        queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
        s"""${de.render(name, Map.empty)} AS $renderedAlias"""
      case factCol@FactCol(_, dt, cc, rollup, _, annotations, _) if factCol.rollupExpression.isInstanceOf[PrestoCustomRollup]=>
        val name = factCol.alias.getOrElse(factCol.name)
        val de = factCol.rollupExpression.asInstanceOf[PrestoCustomRollup].expression
        queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
        s"""${de.render(name, Map.empty)} AS $renderedAlias"""
      case FactCol(_, dt, cc, rollup, _, annotations, _) =>
        queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
        s"""$renderedAlias"""
      case HiveDerFactCol(_, _, dt, cc, de, annotations, rollup, _) =>
        val name = column.alias.getOrElse(column.name)
        queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
        s"""${de.render(name, Map.empty)} AS $renderedAlias"""
      case BigqueryDerFactCol(_, _, dt, cc, de, annotations, rollup, _) =>
        val name = column.alias.getOrElse(column.name)
        queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
        s"""${de.render(name, Map.empty)} AS $renderedAlias"""
      case PrestoDerFactCol(_, _, dt, cc, de, annotations, rollup, _) =>
        val name = column.alias.getOrElse(column.name)
        queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
        s"""${de.render(name, Map.empty)} AS $renderedAlias"""
      case _=> throw new IllegalArgumentException(s"Unexpected fact derived column found in outer select $column")
    }
  }

  def renderColumnAlias(colAlias: String) : String = {
    val renderedExp = new StringBuilder
    // Mangle Aliases, Derived expressions except Id's
    if (!colAlias.toLowerCase.endsWith("id") && (Character.isUpperCase(colAlias.charAt(0)) || colAlias.contains(" "))) {
      // All aliases are prefixed with _to relieve namespace collisions with pre-defined columns with same name.
      renderedExp.append("mang_")
    }
    // remove everything that is not a letter, a digit or space
    // replace any whitespace with "_"
    renderedExp.append(colAlias).toString().replaceAll("[^a-zA-Z0-9\\s_]", "").replaceAll("\\s", "_").toLowerCase
  }

  def getConstantColAlias(alias: String) : String = {
    renderColumnAlias(alias.replaceAll("[^a-zA-Z0-9_]", ""))
  }

  /**
    *  render fact/dim columns with derived/rollup expression
    */
  def renderStaticMappedDimension(column: Column, engine: Engine) : String = {
    val nameOrAlias = renderColumnAlias(column.alias.getOrElse(column.name))
    column.dataType match {
      case IntType(_, sm, _, _, _) if sm.isDefined =>
        val defaultValue = sm.get.default
        val whenClauses = sm.get.tToStringMap.map {
          case (from, to) => s"WHEN ($nameOrAlias IN ($from)) THEN '$to'"
        }
        s"CASE ${whenClauses.mkString(" ")} ELSE '$defaultValue' END"
      case StrType(_, sm, _) if sm.isDefined && engine == HiveEngine =>
        val defaultValue = sm.get.default
        val decodeValues = sm.get.tToStringMap.map {
          case (from, to) => s"'$from', '$to'"
        }
        s"""decodeUDF($nameOrAlias, ${decodeValues.mkString(", ")}, '$defaultValue')"""
      case StrType(_, sm, _) if sm.isDefined && engine == BigqueryEngine =>
        val defaultValue = sm.get.default
        val whenClauses = sm.get.tToStringMap.map {
          case (from, to) => s"WHEN ($nameOrAlias IN ('$from')) THEN '$to'"
        }
        s"CASE ${whenClauses.mkString(" ")} ELSE '$defaultValue' END"
      case StrType(_, sm, _) if sm.isDefined && engine == PrestoEngine =>
        val defaultValue = sm.get.default
        val whenClauses = sm.get.tToStringMap.map {
          case (from, to) => s"WHEN ($nameOrAlias IN ('$from')) THEN '$to'"
        }
        s"CASE ${whenClauses.mkString(" ")} ELSE '$defaultValue' END"
      case _ =>
        s"""COALESCE($nameOrAlias, "NA")"""
    }
  }

  def getPkFinalAliasForDim (queryBuilderContext: QueryBuilderContext, dimBundle: DimensionBundle) : String = {
    val pkColName = dimBundle.dim.primaryKey
    val dimAlias = queryBuilderContext.getAliasForTable(dimBundle.publicDim.name)
    s"${dimAlias}_$pkColName"
  }

  /*
  concat column and alias
 */
  protected[this] def concat(tuple: (String, String)): String = {
    if (tuple._2.isEmpty) {
      s"""${tuple._1}"""
    } else {
      s"""${tuple._1} ${tuple._2}"""
    }
  }

  protected[this] def nvl(name:String) :String = {
    s"""NVL($name,'')"""
  }

  protected[this] def to_string(col:String) : String = {
    s"""CAST($col AS STRING)"""
  }

  protected[this] def concat_ws(csvCol:String) : String = {
    s"""CONCAT_WS(',', $csvCol)"""
  }

  def getFactBest(queryContext: QueryContext): FactBestCandidate = {
    queryContext match {
      case fcq: FactualQueryContext=>
        fcq.factBestCandidate
      case _=> throw new IllegalArgumentException(s"Trying to extract FactBestCandidate in the non factual query context ${queryContext.getClass}")
    }
  }

  def renderColumnWithAlias(fact: Fact,
                            column: Column,
                            alias: String,
                            requiredInnerCols: Set[String],
                            isOuterColumn: Boolean,
                            queryContext: QueryContext,
                            queryBuilderContext: QueryBuilderContext,
                            queryBuilder: QueryBuilder,
                            engine: Engine): Unit = {
    val factBestCandidate = getFactBest(queryContext)

    val name = column.alias.getOrElse(column.name)
    val isOgbQuery = queryContext.isInstanceOf[DimFactOuterGroupByQueryQueryContext]
    val exp = column match {
      case any if queryBuilderContext.containsColByNameAndAlias(name, alias) =>
        //do nothing, we've already processed it
        ""
      case DimCol(_, dt, _, _, _, _) if dt.hasStaticMapping && !isOgbQuery =>
        val renderedAlias = renderColumnAlias(alias)
        queryBuilderContext.setFactColAliasAndExpression(alias, renderedAlias, column, Option(column.name))
        s"${renderStaticMappedDimension(column, engine)} ${column.name}"
      case DimCol(_, dt, _, _, _, _) if dt.hasStaticMapping && isOgbQuery && isOuterColumn =>
        val renderedAlias = renderColumnAlias(alias)
        queryBuilderContext.setFactColAliasAndExpression(alias, renderedAlias, column, Option(name))
        s"${renderStaticMappedDimension(column, engine)} $name"
      case DimCol(_, dt, _, _, _, _) =>
        val renderedAlias = renderColumnAlias(alias)
        queryBuilderContext.setFactColAliasAndExpression(alias, renderedAlias, column, Option(name))
        name
      case ConstDimCol(_, dt, value, _, _, _, _) =>
        val renderedAlias = renderColumnAlias(alias)
        queryBuilderContext.setFactColAliasAndExpression(alias, renderedAlias, column, Option(name))
        s"'$value' AS $name"
      case HiveDerDimCol(_, dt, _, de, _, _, _) =>
        val renderedAlias = renderColumnAlias(alias)
        queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
        s"""${de.render(name, Map.empty)} $renderedAlias"""
      case BigqueryDerDimCol(_, dt, _, de, _, _, _) =>
        val renderedAlias = renderColumnAlias(alias)
        queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
        s"""${de.render(name, Map.empty)} $renderedAlias"""
      case PrestoDerDimCol(_, dt, _, de, _, _, _) =>
        val renderedAlias = renderColumnAlias(alias)
        queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
        s"""${de.render(name, Map.empty)} $renderedAlias"""
      case HivePartDimCol(_, dt, _, _, _, _) =>
        val renderedAlias = renderColumnAlias(alias)
        queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
        name
      case BigqueryPartDimCol(_, dt, _, _, _, _) =>
        val renderedAlias = renderColumnAlias(alias)
        queryBuilderContext.setFactColAliasAndExpression(alias, renderedAlias, column, Option(name))
        name
      case PrestoPartDimCol(_, dt, _, _, _, _) =>
        val renderedAlias = renderColumnAlias(alias)
        queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
        name
      case FactCol(_, dt, _, rollup, _, _, _) =>
        dt match {
          case DecType(_, _, Some(default), Some(min), Some(max), _) =>
            val renderedAlias = renderColumnAlias(alias)
            val minMaxClause = s"CASE WHEN (($name >= $min) AND ($name <= $max)) THEN $name ELSE $default END"
            queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
            s"""${renderRollupExpression(name, rollup, Option(minMaxClause))} $renderedAlias"""
          case IntType(_, _, Some(default), Some(min), Some(max)) =>
            val renderedAlias = renderColumnAlias(alias)
            val minMaxClause = s"CASE WHEN (($name >= $min) AND ($name <= $max)) THEN $name ELSE $default END"
            queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
            s"""${renderRollupExpression(name, rollup, Option(minMaxClause))} $renderedAlias"""
          case _ =>
            val renderedAlias = renderColumnAlias(alias)
            queryBuilderContext.setFactColAliasAndExpression(alias, renderedAlias, column, Option(name))
            s"""${renderRollupExpression(name, rollup)} $name"""
        }
      case HiveDerDimAggregateCol(_, dt, cc, de, _, _, _) =>
        // this col always has rollup expresion in derived expression as requirement
        val renderedAlias = renderColumnAlias(alias)
        queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
        s"""${renderRollupExpression(de.render(name, Map.empty), NoopRollup)} $renderedAlias"""
      case HiveDerFactCol(_, _, dt, cc, de, annotations, rollup, _)
        if factBestCandidate.filterCols.contains(name) || de.expression.hasRollupExpression || requiredInnerCols(name)
          || de.isDimensionDriven =>
        val renderedAlias = renderColumnAlias(alias)
        queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
        s"""${renderRollupExpression(de.render(name, Map.empty), rollup)} $renderedAlias"""
      case HiveDerFactCol(_, _, dt, cc, de, annotations, _, _) =>
        //means no fact operation on this column, push expression outside
        de.sourceColumns.foreach {
          case src if src != name =>
            val sourceCol = fact.columnsByNameMap(src)
            //val renderedAlias = renderColumnAlias(sourceCol.name)
            val renderedAlias = sourceCol.alias.getOrElse(sourceCol.name)
            renderColumnWithAlias(fact, sourceCol, renderedAlias, requiredInnerCols, isOuterColumn, queryContext, queryBuilderContext, queryBuilder, engine)
          case _ => //do nothing if we reference ourselves
        }
        //val renderedAlias = renderColumnAlias(alias)
        val renderedAlias = renderColumnAlias(alias)
        queryBuilderContext.setFactColAliasAndExpression(alias, renderedAlias, column, Option(s"""(${de.render(renderedAlias, queryBuilderContext.getColAliasToFactColNameMap, expandDerivedExpression = false)})"""))
        ""
      case BigqueryDerFactCol(_, _, dt, cc, de, annotations, rollup, _)
        if factBestCandidate.filterCols.contains(name) || de.expression.hasRollupExpression || requiredInnerCols(name)
          || de.isDimensionDriven =>
        val renderedAlias = renderColumnAlias(alias)
        queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
        s"""${renderRollupExpression(de.render(name, Map.empty), rollup)} $renderedAlias"""
      case BigqueryDerFactCol(_, _, dt, cc, de, annotations, _, _) =>
        de.sourceColumns.foreach {
          case src if src != name =>
            val sourceCol = fact.columnsByNameMap(src)
            val renderedAlias = sourceCol.alias.getOrElse(sourceCol.name)
            renderColumnWithAlias(fact, sourceCol, renderedAlias, requiredInnerCols, isOuterColumn, queryContext, queryBuilderContext, queryBuilder, engine)
          case _ =>
        }
        val renderedAlias = renderColumnAlias(alias)
        queryBuilderContext.setFactColAliasAndExpression(alias, renderedAlias, column, Option(s"""(${de.render(renderedAlias, queryBuilderContext.getColAliasToFactColNameMap, expandDerivedExpression = false)})"""))
        ""
      case PrestoDerFactCol(_, _, dt, cc, de, annotations, rollup, _)
        if factBestCandidate.filterCols.contains(name) || de.expression.hasRollupExpression || requiredInnerCols(name)
          || de.isDimensionDriven =>
        val renderedAlias = renderColumnAlias(alias)
        queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
        s"""${renderRollupExpression(de.render(name, Map.empty), rollup)} $renderedAlias"""
      case PrestoDerFactCol(_, _, dt, cc, de, annotations, _, _) =>
        //means no fact operation on this column, push expression outside
        de.sourceColumns.foreach {
          case src if src != name =>
            val sourceCol = fact.columnsByNameMap(src)
            //val renderedAlias = renderColumnAlias(sourceCol.name)
            val renderedAlias = sourceCol.alias.getOrElse(sourceCol.name)
            renderColumnWithAlias(fact, sourceCol, renderedAlias, requiredInnerCols, isOuterColumn, queryContext, queryBuilderContext, queryBuilder, engine)
          case _ => //do nothing if we reference ourselves
        }
        val renderedAlias = renderColumnAlias(alias)
        queryBuilderContext.setFactColAliasAndExpression(alias, renderedAlias, column, Option(s"""(${de.render(renderedAlias, queryBuilderContext.getColAliasToFactColNameMap, expandDerivedExpression = false)})"""))
        ""
      case ConstFactCol(_, _, v, _, _, _, _, _) =>
        val renderedAlias = renderColumnAlias(alias)
        queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
        s"'$v' $name"
    }

    queryBuilder.addFactViewColumn(exp)
  }

  def renderRollupExpression(expression: String, rollupExpression: RollupExpression, renderedColExp: Option[String] = None) : String = {
    rollupExpression match {
      case SumRollup => s"SUM($expression)"
      case MaxRollup => s"MAX($expression)"
      case MinRollup => s"MIN($expression)"
      case AverageRollup => s"AVG($expression)"
      case HiveCustomRollup(exp) => s"(${exp.render(expression, Map.empty, renderedColExp)})"
      case BigqueryCustomRollup(exp) => s"(${exp.render(expression, Map.empty, renderedColExp)})"
      case PrestoCustomRollup(exp) => s"(${exp.render(expression, Map.empty, renderedColExp)})"
      case NoopRollup => s"($expression)"
      case CountRollup => s"COUNT(*)"
      case any => throw new UnsupportedOperationException(s"Unhandled rollup expression : $any")
    }
  }

  def handleStaticMappingInt(sm: Option[StaticMapping[Int]], finalAlias: String): String = {
    val defaultValue = sm.get.default
    val whenClauses = sm.get.tToStringMap.map {
      case (from, to) => s"WHEN ($finalAlias IN ($from)) THEN '$to'"
    }
    s"CASE ${whenClauses.mkString(" ")} ELSE '$defaultValue' END"
  }

  def handleStaticMappingString(sm: Option[StaticMapping[String]], finalAlias: String, defaultValue: String): String = {
    val whenClauses = sm.get.tToStringMap.map {
      case (from, to) => s"WHEN ($finalAlias IN ('$from')) THEN '$to'"
    }
    s"CASE ${whenClauses.mkString(" ")} ELSE '$defaultValue' END"
  }
}
