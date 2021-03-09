package com.yahoo.maha.core.query.presto

import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.query._

abstract class PrestoQueryGeneratorCommon(partitionColumnRenderer:PartitionColumnRenderer, udfStatements: Set[UDFRegistration]) extends BaseQueryGenerator[WithPrestoEngine] with BigqueryHivePrestoQueryCommon {

  // render outercols with column expression
  def generateOuterColumns(queryContext: CombinedQueryContext,
                           queryBuilderContext: QueryBuilderContext,
                           queryBuilder: QueryBuilder
                          ) : String = {
    queryContext.requestModel.requestCols foreach {
      columnInfo =>
        /*if (!columnInfo.isInstanceOf[ConstantColumnInfo] && queryBuilderContext.aliasColumnMap.contains(columnInfo.alias)) {
          //aliasColumnMapOfRequestCols += (columnInfo.alias -> queryBuilderContext.aliasColumnMap(columnInfo.alias))
        } else if (queryContext.factBestCandidate.duplicateAliasMapping.contains(columnInfo.alias)) {
          val sourceAliases = queryContext.factBestCandidate.duplicateAliasMapping(columnInfo.alias)
          val sourceAlias = sourceAliases.find(queryBuilderContext.aliasColumnMap.contains)
          require(sourceAlias.isDefined
            , s"Failed to find source column for duplicate alias mapping : ${queryContext.factBestCandidate.duplicateAliasMapping(columnInfo.alias)}")
          //aliasColumnMapOfRequestCols += (columnInfo.alias -> queryBuilderContext.aliasColumnMap(sourceAlias.get))
        }*/
        QueryGeneratorHelper.populateAliasColMapOfRequestCols(columnInfo, queryBuilderContext, queryContext)
        queryBuilder.addOuterColumn(renderOuterColumn(columnInfo, queryBuilderContext, queryContext.factBestCandidate.duplicateAliasMapping, queryContext.factBestCandidate))
    }
    queryBuilder.getOuterColumns

  }

  def renderOuterColumn(columnInfo: ColumnInfo, queryBuilderContext: QueryBuilderContext, duplicateAliasMapping: Map[String, Set[String]], factCandidate: FactBestCandidate): String = {

    def renderFactCol(alias: String, finalAliasOrExpression: String, col: Column, finalAlias: String): (String,String) = {
      val postFilterAlias = renderNormalOuterColumn(col, finalAliasOrExpression)
      (postFilterAlias,finalAlias)
    }

    columnInfo match {
      case FactColumnInfo(alias) =>
        QueryGeneratorHelper.concat(QueryGeneratorHelper.handleOuterFactColInfo(queryBuilderContext, alias, factCandidate, renderFactCol, duplicateAliasMapping, factCandidate.fact.underlyingTableName.getOrElse(factCandidate.fact.name), false))
      /*if (queryBuilderContext.containsFactColNameForAlias(alias)) {
        val col = queryBuilderContext.getFactColByAlias(alias)
        val finalAlias = queryBuilderContext.getFactColNameForAlias(alias)
        val finalAliasOrExpression = {
          if(queryBuilderContext.isDimensionCol(alias)) {
            val factAlias = queryBuilderContext.getAliasForTable(factCandidate.fact.underlyingTableName.getOrElse(factCandidate.fact.name))
            val factExp = queryBuilderContext.getFactColExpressionOrNameForAlias(alias)
            s"$factAlias.$factExp"
          } else  {
            queryBuilderContext.getFactColExpressionOrNameForAlias(alias)
          }
        }
        renderFactCol(alias, finalAliasOrExpression, col, finalAlias)
      } else if (duplicateAliasMapping.contains(alias)) {
        val duplicateAliases = duplicateAliasMapping(alias)
        val renderedDuplicateAlias = duplicateAliases.collectFirst {
          case duplicateAlias if queryBuilderContext.containsFactColNameForAlias(duplicateAlias) =>
            val col = queryBuilderContext.getFactColByAlias(duplicateAlias)
            val finalAliasOrExpression = queryBuilderContext.getFactColExpressionOrNameForAlias(duplicateAlias)
            val finalAlias = queryBuilderContext.getFactColNameForAlias(duplicateAlias)
            renderFactCol(alias, finalAliasOrExpression, col, finalAlias)
        }
        require(renderedDuplicateAlias.isDefined, s"Failed to render column : $alias")
        renderedDuplicateAlias.get
      } else {
        throw new IllegalArgumentException(s"Could not find inner alias for outer column : $alias")
      }*/
      case DimColumnInfo(alias) =>
        val col = queryBuilderContext.getDimensionColByAlias(alias)
        val finalAlias = queryBuilderContext.getDimensionColNameForAlias(alias)
        val publicDim = queryBuilderContext.getDimensionForColAlias(alias)
        val referredAlias = s"${queryBuilderContext.getAliasForTable(publicDim.name)}.$finalAlias"
        val postFilterAlias = renderNormalOuterColumn(col, referredAlias)
        s"""$postFilterAlias $finalAlias"""
      case ConstantColumnInfo(alias, value) =>
        val finalAlias = getConstantColAlias(alias)
        s"""'$value' $finalAlias"""
      case _ => throw new UnsupportedOperationException("Unsupported Column Type")
    }
  }

  def renderNormalOuterColumn(column: Column, finalAlias: String) : String = {
    val renderedCol = column.dataType match {
      case DecType(_, _, Some(default), Some(min), Some(max), _) =>
        val minMaxClause = s"CASE WHEN (($finalAlias >= ${min}) AND ($finalAlias <= ${max})) THEN $finalAlias ELSE ${default} END"
        s"""ROUND(COALESCE($minMaxClause, ${default}), 10)"""
      case DecType(_, _, Some(default), _, _, _) =>
        s"""ROUND(COALESCE($finalAlias, ${default}), 10)"""
      case DecType(_, _, _, _, _, _) =>
        s"""ROUND(COALESCE($finalAlias, 0), 10)"""
      case IntType(_,sm,df,_,_) =>
        if (sm.isDefined) {
          s"""COALESCE(CAST($finalAlias as varchar), 'NA')"""
        } else {
          s"""COALESCE(CAST($finalAlias as bigint), ${df.getOrElse(0)})"""
        }
      case DateType(_) => s"""getFormattedDate($finalAlias)"""
      case StrType(_, sm, df) =>
        val defaultValue = df.getOrElse("NA")
        s"""COALESCE(CAST($finalAlias as VARCHAR), '$defaultValue')"""
      case _ => s"""COALESCE(cast($finalAlias as VARCHAR), 'NA')"""
    }
    if (column.annotations.contains(EscapingRequired)) {
      s"""getCsvEscapedString(CAST(COALESCE($finalAlias, '') AS VARCHAR))"""
    } else {
      renderedCol
    }
  }

  def generateDimJoinQuery(queryBuilderContext: QueryBuilderContext, dimBundle: DimensionBundle, fact: Fact, requestModel: RequestModel, factViewAlias: String) : String = {

    /**
      *  render fact/dim columns with derived/rollup expression
      */
    def renderColumn(column: Column, alias: String): String = {
      val name = column.alias.getOrElse(column.name)
      column match {
        case DimCol(_, dt, _, _, _, _) =>
          name
        case PrestoDerDimCol(_, dt, _, de, _, _, _) =>
          s"""${de.render(name, Map.empty)}"""
        case other => throw new IllegalArgumentException(s"Unhandled column type for dimension cols : $other")
      }
    }

    val requestDimCols = dimBundle.fields
    val publicDimName = dimBundle.publicDim.name
    val dimTableName = dimBundle.dim.underlyingTableName.getOrElse(dimBundle.dim.name)
    val dimFilters = dimBundle.filters
    val fkColName = fact.publicDimToForeignKeyMap(publicDimName)
    val fkCol = fact.columnsByNameMap(fkColName)
    val pkColName = dimBundle.dim.primaryKey
    val dimAlias = queryBuilderContext.getAliasForTable(publicDimName)

    val dimCols = requestDimCols map {
      colAlias =>
        if (dimBundle.publicDim.isPrimaryKeyAlias(colAlias)) {
          s"""$pkColName ${getPkFinalAliasForDim(queryBuilderContext, dimBundle)}"""
        } else {
          val colName = dimBundle.publicDim.aliasToNameMapFull(colAlias)
          val column = dimBundle.dim.dimensionColumnsByNameMap(colName)
          val finalAlias : String = queryBuilderContext.getDimensionColNameForAlias(colAlias)
          s"${renderColumn(column, finalAlias)} AS $finalAlias"
        }
    }

    val aliasToNameMapFull = dimBundle.publicDim.aliasToNameMapFull
    val columnsByNameMap = dimBundle.dim.columnsByNameMap

    val wheres = dimFilters map {
      filter =>
        FilterSql.renderFilter(
          filter,
          aliasToNameMapFull,
          Map.empty,
          columnsByNameMap,
          PrestoEngine,
          prestoLiteralMapper
        ).filter
    }

    val partitionFilters = partitionColumnRenderer.renderDim(requestModel, dimBundle, prestoLiteralMapper, PrestoEngine)
    val renderedFactFk = renderColumn(fkCol, "")

    val dimWhere = s"""WHERE ${RenderedAndFilter(wheres + partitionFilters).toString}"""

    val joinType = if (requestModel.anyDimHasNonFKNonForceFilter) {
      "JOIN"
    } else {
      "LEFT OUTER JOIN"
    }

    // Columns on which join is done must be of the same type. Add explicit CAST otherwise.
    val joinCondition = {
      require(dimBundle.dim.columnsByNameMap.contains(pkColName), s"Dim: ${dimBundle.dim.name} does not contain $pkColName")
      if (fkCol.dataType.getClass.equals(dimBundle.dim.columnsByNameMap(pkColName).dataType.getClass)) {
        s"$factViewAlias.$renderedFactFk = $dimAlias.${dimAlias}_$pkColName"
      } else {
        s"CAST($factViewAlias.$renderedFactFk AS VARCHAR) = CAST($dimAlias.${dimAlias}_$pkColName AS VARCHAR)"
      }
    }

    s"""$joinType (
       |SELECT ${dimCols.mkString(", ")}
       |FROM $dimTableName
       |$dimWhere
       |)
       |$dimAlias
       |ON
       |$joinCondition
       """.stripMargin

  }
}
