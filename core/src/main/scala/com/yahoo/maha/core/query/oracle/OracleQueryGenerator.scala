// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.oracle

import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension._

import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.query._
import grizzled.slf4j.Logging
import org.apache.commons.lang3.StringUtils

import scala.collection.{mutable, SortedSet}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * Created by hiral on 11/13/15.
 */
case class WhereClause(filters: CombiningFilter) {
  override def toString: String = {
    if (filters.isEmpty) {
      ""
    } else {
      s"WHERE ${filters.toString}"
    }
  }
}

case class RenderedDimension(dimAlias: String, sql: String, onCondition: Option[String], supportingRenderedDimension: Option[RenderedDimension] = None, hasPagination: Boolean, hasTotalRows: Boolean)

case class DimensionSql(drivingDimensionSql: String, multiDimensionJoinSql: Option[String], hasPagination: Boolean, hasTotalRows: Boolean)

class OracleQueryGenerator(partitionColumnRenderer:PartitionColumnRenderer, literalMapper: OracleLiteralMapper = new OracleLiteralMapper) extends BaseQueryGenerator[WithOracleEngine] with Logging {

  override val engine: Engine = OracleEngine

  final private[this] val MAX_SNAPSHOT_TS_ALIAS: String = "max_snapshot_ts_"
  final private[this] val ADDITIONAL_PAGINATION_COLUMN: IndexedSeq[String] = IndexedSeq(OracleQueryGenerator.ROW_COUNT_ALIAS)
  final private[this] val PAGINATION_ROW_COUNT: String = s"""Count(*) OVER() ${OracleQueryGenerator.ROW_COUNT_ALIAS}"""
  final private[this] val supportingDimPostfix: String = "_indexed"
  final private[this] val PAGINATION_WRAPPER: String = "SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (%s) %s) D ) WHERE %s"
  final private[this] val OUTER_PAGINATION_WRAPPER: String = "%s WHERE %s"
  final private[this] val OUTER_PAGINATION_WRAPPER_WITH_FILTERS: String = "%s AND %s"
  final private[this] val PAGINATION_WRAPPER_UNION: String = "SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (%s) D )"
  final private[this] val PAGINATION_ROW_COUNT_COL = ColumnContext.withColumnContext { implicit cc =>
    DimCol(OracleQueryGenerator.ROW_COUNT_ALIAS, IntType())
  }

  private[this] val factAlias: String = "FactAlias"

  override def generate(queryContext: QueryContext): Query = {
    queryContext match {
      case context: DimQueryContext =>
        generateDimOnlyQuery(context)
      case context: CombinedQueryContext =>
        generateDimFactQuery(context)
      case FactQueryContext(factBestCandidate, model, indexAliasOption, attributes) =>
        generateDimFactQuery(CombinedQueryContext(SortedSet.empty, factBestCandidate, model, attributes))
      case a => throw new IllegalArgumentException(s"Unhandled query context : $a")
    }
  }

  private[this] def generateDimensionSql(queryContext: QueryContext, queryBuilderContext: QueryBuilderContext, includePagination: Boolean): DimensionSql = {
    queryContext match {
      case DimQueryContext(dims, requestModel, indexAliasOption, queryAttributes) => generateDimensionSql(dims, requestModel, queryBuilderContext, true, None, includePagination)
      case CombinedQueryContext(dims, fact, requestModel, queryAttributes) => generateDimensionSql(dims, requestModel, queryBuilderContext, false, Option(fact), includePagination)
      case any => throw new UnsupportedOperationException(s"query context not supported : ${any.getClass.getSimpleName}")
    }
  }

  def generateSubqueryFilter(primaryTableFkCol: String, primaryTableFilters: SortedSet[Filter], subqueryBundle: DimensionBundle) : (String, Boolean) = {

    var escaped = false
    val aliasToNameMapFull = subqueryBundle.publicDim.aliasToNameMapFull
    val columnsByNameMap = subqueryBundle.dim.columnsByNameMap

    val dimSelect = subqueryBundle.dim.primaryKey
    val primaryBundleFiltersToInclude = primaryTableFilters.view.filter {
      filter =>
        val alias = filter.field
        subqueryBundle.publicDim.columnsByAlias(alias)
    }.map {
      filter =>
        val f = FilterSql.renderFilter(filter, aliasToNameMapFull, columnsByNameMap, OracleEngine, literalMapper)
        escaped |= f.escaped
        f.filter
    }

    val subqueryFilters = generateInSubqueryFilters(subqueryBundle)
    val dimWhere = WhereClause(AndFilter(subqueryFilters ++ primaryBundleFiltersToInclude))
    (s"""$primaryTableFkCol IN (SELECT $dimSelect FROM ${subqueryBundle.dim.name} $dimWhere)""", escaped)
  }

  private[this] def generateDimensionSql(dims: SortedSet[DimensionBundle]
                                         , requestModel: RequestModel
                                         , queryBuilderContext: QueryBuilderContext
                                         , isDimOnly: Boolean
                                         , factOption: Option[FactBestCandidate]
                                         , includePagination: Boolean
                                          ): DimensionSql = {
    /*
Given all the dimension bundles, and request model do the following
1.  For each dimension bundle, generate RenderedDimension
a. Generate dimension alias
b. Render the dimension fields while updating the query builder context
c. Render the filters
2.  Determine fact driven or dim driven
a. Fact Driven
  1. Determine join type
    a. No fact filter use Left Outer Join
    b. With fact filter use Join
  2. Generate full sql by combining the individual dimensions
b. Dim Driven
  1. Single dimensions query
  2. Multiple dimensions query
    a. Generate full sql by combining the individual dimensions
*/
    def generateWhereClause(dimBundle: DimensionBundle, subqueryBundles: Set[DimensionBundle]): WhereClause = {

      val dimBundleFilters = new mutable.LinkedHashSet[String]
      var escaped = false
      val primaryDimLevel = dimBundle.dim.dimLevel

      //add subquery filters
      subqueryBundles.foreach {
        subqueryBundle =>
          require(primaryDimLevel > subqueryBundle.dim.dimLevel,
            s"level of primary dimension must be greater than subquery dimension, dim=${dimBundle.publicDim.name}, subquery dim=${subqueryBundle.publicDim.name}")
          require(dimBundle.publicDim.columnsByAlias(subqueryBundle.publicDim.primaryKeyByAlias),
            s"subquery dim primary key not found in primary dimension, dim=${dimBundle.publicDim.name}, subquery dim=${subqueryBundle.publicDim.name}")

          val (sql, wasEscaped) = generateSubqueryFilter(dimBundle.dim.publicDimToForeignKeyMap(subqueryBundle.publicDim.name), dimBundle.filters, subqueryBundle)
          escaped |= wasEscaped
          dimBundleFilters += sql
      }
      val aliasToNameMapFull = dimBundle.publicDim.aliasToNameMapFull
      val columnsByNameMap = dimBundle.dim.columnsByNameMap


      dimBundle.filters.foreach {
        filter =>
          if (!requestModel.forceDimDriven
          || dimBundle.isDrivingDimension
          //TODO: add check that not include filter predicate if it is push down only if that field is partition key
          || requestModel.hasNonDrivingDimSortOrFilter && !dimBundle.isDrivingDimension) {
            val f = FilterSql.renderFilter(filter, aliasToNameMapFull, columnsByNameMap, OracleEngine, literalMapper)
            escaped |= f.escaped
            dimBundleFilters += f.filter
          }
      }

      WhereClause(AndFilter(dimBundleFilters))
    }

    def generateRenderedDimension(dimBundle: DimensionBundle,
                                  subqueryBundles: Set[DimensionBundle],
                                  requestModel: RequestModel, nonPrimaryBundleHasFilters: Boolean, isDimOnly: Boolean): RenderedDimension = {
      val isHashPartitioningSupported = dimBundle.dim.annotations(OracleAdvertiserHashPartitioning)

      val dimAlias = queryBuilderContext.getAliasForTable(dimBundle.dim.name)


      val prevDimOption: Option[Dimension] = dimBundle.upperCandidates.headOption

      val dimSelectSet = new mutable.LinkedHashSet[String]

      val orderByIndex: ArrayBuffer[String] = new ArrayBuffer[String](dimBundle.fields.size)

      val dimSortMap = requestModel.dimSortByMap

      var colIndex = 1

      val shouldSelfJoinWithSupportingDrivingDim = {
        if (!requestModel.isFactDriven && requestModel.hasFactSortBy == false
          && requestModel.dimSortByMap.size == 1 && dimBundle.isDrivingDimension && dimBundle.publicDim.partitionColumn.nonEmpty
          && dimBundle.hasNonFKOrForcedFilters == false) {
          if (requestModel.dimSortByMap.contains(dimBundle.publicDim.primaryKeyByAlias)) {
            true
          }
          else {
            false
          }
        } else {
          false
        }
      }


      val supportingRenderedDimension: Option[RenderedDimension] = {
        val dimPKIndex : Option[PKCompositeIndex] = getDimOptionalPkIndex(dimBundle.dim)

        if (shouldSelfJoinWithSupportingDrivingDim && dimPKIndex.isDefined) {
          val innerFields = dimBundle.fields.filter(f => f.equals(dimBundle.publicDim.primaryKeyByAlias) ||
            dimBundle.publicDim.partitionColumn.contains(f))
          // Rewriting dimension candidate for supporting dimension
          val supportingDim = dimBundle.publicDim.forColumns(OracleEngine,requestModel.schema, innerFields)
          require(supportingDim.isDefined,s"Failed to find Supporting Dimension for $innerFields")
          val copyDimBundle = dimBundle.copy(dim = supportingDim.get, fields = innerFields)
          Option(generateSupportingRenderedDimension(copyDimBundle, dimBundle, requestModel, dimPKIndex.get.indexName))
        } else {
          None
        }
      }
      val sortColsFirst = new mutable.LinkedHashSet[String] ++ requestModel.requestSortByCols.filter(col=>dimBundle.fields.contains(col.alias)).map(_.alias) ++ dimBundle.fields

      sortColsFirst.foreach {
        alias =>
          val name = {
            if (dimBundle.publicDim.primaryKeyByAlias == alias) {
              dimBundle.dim.primaryKey
            } else {
              dimBundle.publicDim.aliasToNameMap(alias)
            }
          }
          val column = dimBundle.dim.dimensionColumnsByNameMap(name)
          val nameOrAlias = column.alias.getOrElse(name)
          val finalAlias = column match {
            case OracleDerDimCol(_,_,_,exp, _,_,_) =>
              dimSelectSet += s"""${exp.render(nameOrAlias, Map.empty)} AS "${nameOrAlias}""""
              s"""${dimAlias}."${nameOrAlias}""""
            case DimCol(_, _, _, _, _, _) | OraclePartDimCol(_, _, _, _, _,_) =>
              dimSelectSet += s"$nameOrAlias"
              s"""${dimAlias}.${nameOrAlias}"""
            case any =>
              throw new UnsupportedOperationException(s"Found non dim column : $any")
          }
          if (isDimOnly && requestModel.hasNonDrivingDimNonFKNonPKFilter) {
            queryBuilderContext.setDimensionColAliasForDimOnlyQuery(alias, finalAlias, column, dimBundle.publicDim)
          } else {
            queryBuilderContext.setDimensionColAlias(alias, finalAlias, column, dimBundle.publicDim)
          }

          colIndex = dimSelectSet.size

          if ((requestModel.isDimDriven || isDimOnly) && dimSortMap.contains(alias) && dimBundle.isDrivingDimension) {
            val nullsLast = if (column.isKey) "" else "NULLS LAST"
            orderByIndex += s"""$colIndex ${dimSortMap(alias)} $nullsLast"""
          } else if(isDimOnly && requestModel.factSortByMap.contains(alias)){
            val nullsLast = if (column.isKey) "" else "NULLS LAST"
            orderByIndex += s"""$colIndex ${requestModel.factSortByMap(alias)} $nullsLast"""
          }
      }

      if (dimBundle.dim.partitionColumns.nonEmpty) {
        dimBundle.dim.partitionColumns.foreach {
          d => dimSelectSet += d.name
        }
      }

      var hasTotalRows = false

      if (requestModel.includeRowCount && requestModel.isDimDriven && dimBundle.isDrivingDimension
        && ((!nonPrimaryBundleHasFilters && isDimOnly) || !isDimOnly)) {
        dimSelectSet += PAGINATION_ROW_COUNT
        hasTotalRows = true
      }

      val dimSelect = dimSelectSet.mkString(", ")
      val dimWhere = generateWhereClause(dimBundle, subqueryBundles)

      val dimOrderBy = {
        if ((requestModel.isDimDriven || isDimOnly) && orderByIndex.size > 0) {
          val sql = orderByIndex.mkString(", ")
          s"""ORDER BY $sql"""
        } else {
          ""
        }
      }

      val onCondition: Option[String] = for {
        prevDim <- prevDimOption
        foreignKeyColumnName <- prevDim.publicDimToForeignKeyMap.get(dimBundle.publicDim.name)
        foreignKeyColumn = prevDim.columnsByNameMap(foreignKeyColumnName)
        foreignKeyColumnNameOrAlias = foreignKeyColumn.alias.getOrElse(foreignKeyColumnName)
        prevDimAlias = queryBuilderContext.getAliasForTable(prevDim.name)
        prevPublicDim = dimBundle.publicUpperCandidatesMap(prevDim.name)
      } yield {
        val idJoin = s"$prevDimAlias.${foreignKeyColumnNameOrAlias} = $dimAlias.${dimBundle.dim.primaryKey}"
        val partitionKeyConditions = new mutable.LinkedHashSet[String]()
        dimBundle.dim.partitionColumns.map {
          partCol =>
            val name = partCol.name
            val alias = dimBundle.publicDim.keyColumnToAliasMap(name)
            val prevName = prevPublicDim.aliasToNameMapFull(alias)
            if (prevName.nonEmpty) {
              partitionKeyConditions += s"$prevDimAlias.$prevName = $dimAlias.$name"
            }
        }
        partitionKeyConditions += idJoin
        String.format("ON( %s )", partitionKeyConditions.mkString(" AND "))
      }

      val optionalHint = getDimensionOptionalHint(dimBundle.dim).map(toComment).getOrElse("")
      val dimension = dimBundle.dim

      if (!isHashPartitioningSupported) {
        require(dimension.singletonColumn.isDefined,
          s"No singleton column defined for non hash partitioned dimension : dim=${dimension.name}")
        require(dimension.singletonColumn.exists(_.annotations(OracleSnapshotTimestamp)),
          s"No snapshot column find for non hash partitioned dimension : dim=${dimension.name}")

        val snapshotColumnName = dimension.singletonColumn.get.name
        val maxSnapshotColumnExpression = renderSnapshotExpression(snapshotColumnName, dimension.primaryKey)
        val innerSql =
          s"""SELECT $optionalHint $maxSnapshotColumnExpression $MAX_SNAPSHOT_TS_ALIAS, $snapshotColumnName, $dimSelect
            FROM ${dimension.name}
            $dimWhere"""
        RenderedDimension(dimAlias,
          s"""SELECT $optionalHint $dimSelect
            FROM ( $innerSql )
            WHERE $MAX_SNAPSHOT_TS_ALIAS = $snapshotColumnName"""
          , None, None, hasPagination = false, hasTotalRows = hasTotalRows)
      } else {
        if (supportingRenderedDimension.isDefined) {
          RenderedDimension(dimAlias,
            s"""SELECT $optionalHint $dimSelect
            FROM ${dimension.name} INNER JOIN ( ${supportingRenderedDimension.get.sql} ) ${supportingRenderedDimension.get.dimAlias}
            ${supportingRenderedDimension.get.onCondition.get}
            $dimWhere
            $dimOrderBy """, onCondition, supportingRenderedDimension, hasPagination = false, hasTotalRows = hasTotalRows)
        } else {
          RenderedDimension(dimAlias,
            s"""SELECT $optionalHint $dimSelect
            FROM ${dimension.name}
            $dimWhere
            $dimOrderBy """, onCondition, supportingRenderedDimension, hasPagination = false, hasTotalRows = hasTotalRows)
        }
      }
    }

    def generateSupportingRenderedDimension(dimBundle: DimensionBundle,
                                            mainDimBundle: DimensionBundle,
                                            requestModel: RequestModel,
                                            dimPKIndex: String): RenderedDimension = {
      val isHashPartitioningSupported = dimBundle.dim.annotations(OracleAdvertiserHashPartitioning)

      require(isHashPartitioningSupported, "We do not support inner join with supporting dimensions for non hash partitioned tables")

      val dimAlias = queryBuilderContext.getAliasForTable(s"${dimBundle.dim.name}$supportingDimPostfix")

      val prevDim = mainDimBundle.publicDim

      val dimSelectSet = new mutable.LinkedHashSet[String]

      val orderByIndex: ArrayBuffer[String] = new ArrayBuffer[String](dimBundle.fields.size)

      val dimSortMap = requestModel.dimSortByMap

      var colIndex = 1

      if (dimBundle.dim.partitionColumns.nonEmpty) {
        dimBundle.dim.partitionColumns.foreach {
          d =>
            val fieldAlias = queryBuilderContext.getAliasForField(dimBundle.dim.name, d.name)
            dimSelectSet += s"${d.name} $fieldAlias"
        }
      }


      dimBundle.fields.foreach {
        alias =>
          val name = {
            if (dimBundle.publicDim.primaryKeyByAlias == alias) {
              dimBundle.dim.primaryKey
            } else {
              dimBundle.publicDim.aliasToNameMap(alias)
            }
          }
          val column = dimBundle.dim.dimensionColumnsByNameMap(name)

          val fieldAlias = queryBuilderContext.getAliasForField(dimBundle.dim.name, name)
          dimSelectSet += s"$name $fieldAlias"

          colIndex = dimSelectSet.size

          if (requestModel.isDimDriven && dimSortMap.contains(alias) && dimBundle.isDrivingDimension) {
            val nullsLast = if (column.isKey) "" else "NULLS LAST"
            orderByIndex += s"""$colIndex ${dimSortMap(alias)} $nullsLast"""
          }
      }

      if (requestModel.includeRowCount && requestModel.isDimDriven && dimBundle.isDrivingDimension) {
        dimSelectSet += PAGINATION_ROW_COUNT
      }

      val dimSelect = dimSelectSet.mkString(", ")
      val dimWhere = generateWhereClause(dimBundle, Set.empty)

      val dimOrderBy = {
        if (requestModel.isDimDriven && orderByIndex.size > 0) {
          val sql = orderByIndex.mkString(", ")
          s"""ORDER BY $sql"""
        } else {
          ""
        }
      }

      val onCondition: Option[String] = {
        val pkIdFieldAlias = queryBuilderContext.getAliasForField(dimBundle.dim.name,dimBundle.dim.primaryKey)
        val idJoin = s"${mainDimBundle.dim.name}.${prevDim.aliasToNameMapFull(prevDim.primaryKeyByAlias)} = $dimAlias.${pkIdFieldAlias}"
        val partitionKeyConditions = new mutable.LinkedHashSet[String]()
        dimBundle.dim.partitionColumns.map {
          partCol =>
            val name = partCol.name
            val partColFieldAlias = queryBuilderContext.getAliasForField(dimBundle.dim.name, name)
            val alias = dimBundle.publicDim.keyColumnToAliasMap(name)
            val prevName = prevDim.aliasToNameMapFull(alias)
            if (prevName.nonEmpty) {
              partitionKeyConditions += s"${mainDimBundle.dim.name}.$prevName = $dimAlias.$partColFieldAlias"
            }
        }
        partitionKeyConditions += idJoin
        Option(String.format("ON( %s )", partitionKeyConditions.mkString(" AND ")))
      }

      val dimension = dimBundle.dim
      val optionalHint = toComment(s" INDEX(${dimension.name} $dimPKIndex) ")

      val sql =
        s"""SELECT $optionalHint $dimSelect
            FROM ${dimension.name}
            $dimWhere
            $dimOrderBy """
      RenderedDimension(dimAlias, addPaginationWrapper(sql, requestModel.maxRows, requestModel.startIndex, includePagination),
        onCondition, hasPagination = includePagination, hasTotalRows = false)
    }

    if (dims.isEmpty) {
      DimensionSql(StringUtils.EMPTY, None, false, false)
    } else {
      require(factOption.isDefined || isDimOnly, "Fact is not defined when doing a combined query!")

      if (!isDimOnly && requestModel.isFactDriven) {
        val factCandidate = factOption.get
        val factAlias = queryBuilderContext.getAliasForTable(factCandidate.fact.name)
        val renderedDimensions: Map[String, RenderedDimension] = dims.map {
          dimBundle =>
            dimBundle.dim.name -> generateRenderedDimension(dimBundle, Set.empty, requestModel, false, isDimOnly)
        }.toMap

        def getJoinType(dimBundle: DimensionBundle): String = {
          if (factCandidate.schemaRequiredAliases.forall(dimBundle.publicDim.columnsByAlias)) {
            if (requestModel.isDebugEnabled) {
              info(s"dimBundle.dim.isDerivedDimension: ${dimBundle.dim.name} ${dimBundle.dim.isDerivedDimension} hasNonPushDownFilters: ${dimBundle.hasNonPushDownFilters}")
            }
            if (dimBundle.dim.isDerivedDimension) {
              if (dimBundle.hasNonPushDownFilters) { // If derived dim has filter, then use inner join, otherwise use left outer join
                "INNER JOIN"
              } else {
                "LEFT OUTER JOIN"
              }
            } else {
              "RIGHT OUTER JOIN"
            }
          } else {
            "LEFT OUTER JOIN"
          }
        }

        val factHasSchemaRequiredFields: Boolean = factCandidate
          .schemaRequiredAliases.forall(factCandidate.publicFact.columnsByAlias.apply)

        val hasAllDimsNonFKNonForceFilterAsync = requestModel.isAsyncRequest && requestModel.hasAllDimsNonFKNonForceFilter


        val defaultJoinType: Option[String] = {
          if (requestModel.forceDimDriven) {
            Option("RIGHT OUTER JOIN")
          } else {
            if (hasAllDimsNonFKNonForceFilterAsync) {
              Option("INNER JOIN")
            } else
            if (factHasSchemaRequiredFields) {
              Option("LEFT OUTER JOIN")
            } else {
              None
            }
          }
        }

        val sqlBuilder = new StringBuilder
        var hasPagination = false
        var hasTotalRows = false
        dims.foreach {
          dimBundle =>
            val renderedDim = renderedDimensions(dimBundle.dim.name)
            hasPagination = hasPagination || renderedDim.hasPagination
            hasTotalRows = hasTotalRows ||  renderedDim.hasTotalRows
            /*
          1. joinType {} dimAlias ON (factAlias.fk = dimAlias.pk)
           */
            val dimAlias = renderedDim.dimAlias
            val fkCol = factCandidate.fact.publicDimToForeignKeyColMap(dimBundle.publicDim.name)
            val fk = fkCol.alias.getOrElse(fkCol.name)

            val pk = dimBundle.dim.primaryKey
            val joinType = defaultJoinType.getOrElse(getJoinType(dimBundle))

            sqlBuilder.append(
              s"""           $joinType
           (${renderedDim.sql})
           $dimAlias ON ($factAlias.$fk = $dimAlias.$pk)""")
            sqlBuilder.append("\n")

        }

        val resultSql = sqlBuilder.result()

        //if fact does not have all schema require fields, there must be a right outer join with at least one dim
        require(factHasSchemaRequiredFields || resultSql.contains("RIGHT OUTER JOIN") || resultSql.contains("INNER JOIN") ,
          s"Failed to construct valid query when fact does not have all schema required fields : ${factCandidate.schemaRequiredAliases}"
        )

        DimensionSql(resultSql, None, hasPagination = hasPagination, hasTotalRows = hasTotalRows)
      } else {
        // Sync Query Gen Block
        var hasPagination = false
        var hasTotalRows = false
        val primaryBundle = dims.last
        val subqueryBundles = dims.dropRight(1)
        val isSubqueryOnlyQuery = subqueryBundles.nonEmpty && subqueryBundles.forall {
          db => db.fields.filterNot(db.publicDim.isPrimaryKeyAlias).isEmpty
        }
        val nonPrimaryBundleHasFilters = subqueryBundles.exists(_.hasNonPushDownFilters)
        val renderedDimensionsList = new mutable.ArrayBuffer[(String, RenderedDimension, DimensionBundle)](dims.size)

        if (isSubqueryOnlyQuery) {
          val renderedDim = generateRenderedDimension(primaryBundle, subqueryBundles.toSet, requestModel, false, isDimOnly)
          renderedDimensionsList += new Tuple3(primaryBundle.dim.name, renderedDim, primaryBundle)
          hasPagination = hasPagination || renderedDim.hasPagination
          hasTotalRows = hasTotalRows ||  renderedDim.hasTotalRows
        } else {
          dims.toList.foreach {
            dimBundle =>
              val renderedDim = generateRenderedDimension(dimBundle, Set.empty, requestModel, nonPrimaryBundleHasFilters, isDimOnly)
              renderedDimensionsList += new Tuple3(dimBundle.dim.name, renderedDim, dimBundle)
              hasPagination = hasPagination || renderedDim.hasPagination
              hasTotalRows = hasTotalRows ||  renderedDim.hasTotalRows
          }
        }
        val parentJoinsLOJBuilder = new StringBuilder
        val parentJoinType = {
          if(requestModel.hasNonDrivingDimNonFKNonPKFilter) {
            "RIGHT OUTER JOIN"
          } else  {
            "LEFT OUTER JOIN"
          }
        }
        val (_, renderedPrimaryDim, _) = renderedDimensionsList.reduceRight {
          (renderedDim, b) =>
            require(renderedDim._2.onCondition.isDefined,
              s"Failed to determine join condition between ${primaryBundle.dim.name} and ${renderedDim._1}")
            if (requestModel.isDebugEnabled) {
              info(s"renderedDim: ${renderedDim._3.dim.name} renderedDim._3.dim.isDerivedDimension: ${renderedDim._3.dim.isDerivedDimension} hasNonPushDownFilters : ${renderedDim._3.hasNonPushDownFilters}")
              info(s"b: ${b._3.dim.name} b._3.dim.isDerivedDimension: ${b._3.dim.isDerivedDimension} hasNonPushDownFilters : ${b._3.hasNonPushDownFilters}")
            }
            val joinType = if((renderedDim._3.dim.isDerivedDimension && renderedDim._3.hasNonPushDownFilters)|| (b._3.dim.isDerivedDimension && b._3.hasNonPushDownFilters)) {
                "INNER JOIN"
            } else if (renderedDim._3.dim.isDerivedDimension || b._3.dim.isDerivedDimension){
              "LEFT OUTER JOIN"
            } else parentJoinType

            parentJoinsLOJBuilder.append(
              s""" $joinType
            (${renderedDim._2.sql}) ${renderedDim._2.dimAlias}
              ${renderedDim._2.onCondition.get}
              """
            )
            b
        }
        val dimAlias = renderedPrimaryDim.dimAlias
        val pk = primaryBundle.dim.primaryKey

        val sqlBuilder = new StringBuilder
        val factCondition: String = if (!isDimOnly) {
          val factCandidate = factOption.get
          val factAlias = queryBuilderContext.getAliasForTable(factCandidate.fact.name)
          val fkObj = factCandidate.fact.publicDimToForeignKeyColMap(primaryBundle.publicDim.name)
          val fkName = fkObj.alias.getOrElse(fkObj.name)
          s""" ON ($factAlias.$fkName = $dimAlias.$pk)"""
        } else StringUtils.EMPTY
        if (!isDimOnly) {
          sqlBuilder.append( s"""           RIGHT OUTER JOIN""")
        }

        val dimJoinsTemplate = " (%s) %s \n" +
          "         %s "

        val wrapDimJoinsTemplate = s"""(%s)"""

        if (requestModel.hasFactSortBy && !isDimOnly) {
          val dimJoins = {
            if (subqueryBundles.size > 0) {
              String.format(wrapDimJoinsTemplate,
                String.format(dimJoinsTemplate, renderedPrimaryDim.sql, dimAlias, parentJoinsLOJBuilder.result()))
            } else {
              String.format(dimJoinsTemplate, renderedPrimaryDim.sql, dimAlias, parentJoinsLOJBuilder.result())
            }
          }
          sqlBuilder.append(
            s"""
               $dimJoins $factCondition
              """)
          sqlBuilder.append("\n")
          DimensionSql(sqlBuilder.result(), None, hasPagination = hasPagination, hasTotalRows = hasTotalRows)
        } else {
          val dimJoins = {
            if (subqueryBundles.size > 0) {
              val shouldIncludePagination = (includePagination && !nonPrimaryBundleHasFilters) || (includePagination && !isDimOnly)
              hasPagination = hasPagination || shouldIncludePagination
              String.format(wrapDimJoinsTemplate
                , String.format(dimJoinsTemplate
                  , addPaginationWrapper(renderedPrimaryDim.sql, requestModel.maxRows, requestModel.startIndex, shouldIncludePagination)
                  , dimAlias
                  , parentJoinsLOJBuilder.result()
                )
              )
            } else {
              hasPagination = true
              String.format(dimJoinsTemplate
                , addPaginationWrapper(renderedPrimaryDim.sql, requestModel.maxRows, requestModel.startIndex, includePagination)
                , dimAlias
                , parentJoinsLOJBuilder.result()
              )
            }
          }
          sqlBuilder.append(
            s"""
               $dimJoins $factCondition""")
          sqlBuilder.append("\n")
          DimensionSql(sqlBuilder.result(), None, hasPagination = hasPagination, hasTotalRows = hasTotalRows)
        }
      }
    }
  }

  private[this] def renderSnapshotExpression(snapshotColumnName: String, pkName: String): String = {
    s"MAX($snapshotColumnName) OVER (PARTITION BY $pkName)"
  }

  private[this] def addPaginationWrapper(queryString: String, mr: Int, si: Int, includePagination: Boolean): String = {
    if(includePagination) {
      val paginationPredicates: ListBuffer[String] = new ListBuffer[String]()
      val minPosition: Int = if (si < 0) 1 else si + 1
      paginationPredicates += ("ROW_NUMBER >= " + minPosition)
      val stopKeyPredicate: String =  {
        if (mr > 0) {
          val maxPosition: Int = if (si <= 0) mr else minPosition - 1 + mr
          paginationPredicates += ("ROW_NUMBER <= " + maxPosition)
          s"WHERE ROWNUM <= $maxPosition"
        } else  s""
      }
      String.format(PAGINATION_WRAPPER, queryString, stopKeyPredicate, paginationPredicates.toList.mkString(" AND "))
    } else {
      queryString
    }
  }

  private[this] def addOuterPaginationWrapper(queryString: String, mr: Int, si: Int, includePagination: Boolean, outerFiltersPresent: Boolean): String = {
    if(includePagination) {
      val paginationPredicates: ListBuffer[String] = new ListBuffer[String]()
      val minPosition: Int = if (si < 0) 1 else si + 1
      paginationPredicates += ("ROWNUM >= " + minPosition)
      if (mr > 0) {
        val maxPosition: Int = if (si <= 0) mr else minPosition - 1 + mr
        paginationPredicates += ("ROWNUM <= " + maxPosition)
      }
      if (outerFiltersPresent)
        String.format(OUTER_PAGINATION_WRAPPER_WITH_FILTERS, queryString, paginationPredicates.toList.mkString(" AND "))
      else
        String.format(OUTER_PAGINATION_WRAPPER, queryString, paginationPredicates.toList.mkString(" AND "))
    } else {
      queryString
    }
  }

  private[this] def toComment(hint: String): String = {
    s"/*+ $hint */"
  }

  private[this] def getDimensionOptionalHint(dimension: Dimension): Option[String] = {
    dimension.annotations.collect {
      case hint if hint.isInstanceOf[DimensionOracleStaticHint] =>
        hint.asInstanceOf[DimensionOracleStaticHint].hint
    }.headOption
  }

  private[this] def dimOnlyInjectFilter(dbSet: SortedSet[DimensionBundle], injectFilter: ValuesFilter) : SortedSet[DimensionBundle] = {
    val injectFilterSet = Set(injectFilter)
    dbSet.map {
      db =>
        if(db.publicDim.columnsByAliasMap.contains(injectFilter.field)) {
          info("publicDim contains injectFilter.field: " + injectFilter.field)
          db.copy(filters = db.filters ++ injectFilterSet)
        } else {
          val fkSources = db.publicDim.foreignKeySources
          val fkDimBundles = dbSet.find(b => fkSources.contains(b.publicDim.name) && b.publicDim.isPrimaryKeyAlias(injectFilter.field))
          info("fkDimBundles: " + fkDimBundles)
          fkDimBundles.fold(db){
            fkDimBundle =>
              db.dim.sourceToForeignKeyColumnMap.get(fkDimBundle.publicDim.name).map {
                col =>
                  db.copy(filters = db.filters ++ Set(injectFilter.renameField(db.publicDim.keyColumnToAliasMap(col.name))))
              }.getOrElse(db)
          }
        }
    }
  }

  private[this] def generateOuterWhereClause(queryContext: QueryContext, queryBuilderContext: QueryBuilderContext) : WhereClause = {
    val outerFilters = new mutable.LinkedHashSet[String]
    val requestModel =  queryContext.requestModel
    requestModel.outerFilters.foreach {
      filter =>
          val f = FilterSql.renderOuterFilter(filter, queryBuilderContext.aliasColumnMap, OracleEngine, literalMapper)
          outerFilters += f.filter
    }

    WhereClause(AndFilter(outerFilters))
  }

  private[this] def generateDimOnlyQuery(queryContext: QueryContext): Query = {
    val paramBuilder = new QueryParameterBuilder

    val outerColumns = new mutable.LinkedHashSet[String]

    val requestModel = queryContext.requestModel

    val dimDrivenFirstRowOptimization = if(!requestModel.isRequestingDistict && requestModel.isDimDriven && requestModel.maxRows == 1) { """/*+ FIRST_ROWS */"""
    } else ""

    val queryStringTemplate = {
      if(!requestModel.isRequestingDistict) {
        s"""SELECT $dimDrivenFirstRowOptimization *
      FROM (SELECT %s
            FROM %s
           )
            %s"""
      } else {
        s"""SELECT *
      FROM (SELECT DISTINCT %s
            FROM %s
           )
            %s"""
      }
    }

    val includePagination = queryContext.requestModel.isSyncRequest
    val aliasColumnMapOfRequestCols = new mutable.HashMap[String, Column]()

    val dimOnlyQueryContext = queryContext.asInstanceOf[DimQueryContext]
    val injectedIdInFiltersOption = dimOnlyQueryContext.queryAttributes.getAttributeOption(QueryAttributes.injectedDimINFilter)
    val injectedIdNotInFiltersOption = dimOnlyQueryContext.queryAttributes.getAttributeOption(QueryAttributes.injectedDimNOTINFilter)

    val unionQueryOption: Option[String] = for {
      injectedIdInFiltersAttribute <- injectedIdInFiltersOption
      if injectedIdInFiltersAttribute.isInstanceOf[InjectedDimFilterAttribute]
    } yield {
      val queryBuilderContext = new QueryBuilderContext

      val injectedIdInFilters = injectedIdInFiltersAttribute.asInstanceOf[InjectedDimFilterAttribute]

      val inFilter = injectedIdInFilters
      val inFilterQueryContext = dimOnlyQueryContext.copy(dims = dimOnlyInjectFilter(dimOnlyQueryContext.dims, inFilter.value))
      val dimQueryIn = generateDimensionSql(inFilterQueryContext, queryBuilderContext, includePagination = false).drivingDimensionSql
      val inFilterValuesSize = inFilter.value.values.size
      val maxRows = Math.max(inFilterValuesSize, requestModel.maxRows)

      val dimQueryNotInOption = for {
        injectedIdNotInFiltersAttribute <- injectedIdNotInFiltersOption
        if injectedIdNotInFiltersAttribute.isInstanceOf[InjectedDimFilterAttribute]

      } yield {
        val queryBuilderContext = new QueryBuilderContext
        val injectedIdNotInFilters = injectedIdNotInFiltersAttribute.asInstanceOf[InjectedDimFilterAttribute]
        val notInFilter = injectedIdNotInFilters
        val notInFilterQueryContext = dimOnlyQueryContext.copy(dims = dimOnlyInjectFilter(dimOnlyQueryContext.dims, notInFilter.value))
        generateDimensionSql(notInFilterQueryContext, queryBuilderContext, includePagination = false).drivingDimensionSql
      }

      val outerWhereClause = generateOuterWhereClause(queryContext, queryBuilderContext)
      val aliasColumnMap = queryBuilderContext.aliasColumnMap

      queryContext.requestModel.requestCols.map {
        ci =>
          if (aliasColumnMap.contains(ci.alias)) {
            aliasColumnMapOfRequestCols += (ci.alias -> aliasColumnMap(ci.alias))
            outerColumns += renderOuterColumn(ci, queryBuilderContext, Map.empty, false, true, queryContext)
          } else if (ci.isInstanceOf[ConstantColumnInfo]) {
            outerColumns += renderOuterColumn(ci, queryBuilderContext, Map.empty, false, true, queryContext)
          }
      }

      if (queryContext.requestModel.includeRowCount && !queryContext.requestModel.hasFactSortBy) {
        outerColumns += OracleQueryGenerator.ROW_COUNT_ALIAS
      }

      dimQueryNotInOption.fold {
        addPaginationWrapper(String.format(queryStringTemplate, outerColumns.mkString(", "), dimQueryIn, outerWhereClause), maxRows, 0, includePagination)
      } {
        dimQueryNotIn =>
          val unionTemplate = s" (%s) UNION ALL (%s) "
          String.format(unionTemplate
            , String.format(PAGINATION_WRAPPER_UNION
              , String.format(queryStringTemplate, outerColumns.mkString(", "), dimQueryIn, outerWhereClause)
            )
            , addPaginationWrapper(String.format(queryStringTemplate,outerColumns.mkString(", "),dimQueryNotIn, outerWhereClause),maxRows, 0, includePagination)
          )
      }
    }

    if(unionQueryOption.isDefined) {
      new OracleQuery(
        queryContext,
        unionQueryOption.get,
        paramBuilder.build(),
        aliasColumnMapOfRequestCols.toMap,
        additionalColumns(queryContext)
      )
    } else {

      val queryBuilderContext = new QueryBuilderContext

      //TODO: figure out what to do with multi dim sql, but we shouldnt have any here, maybe throw error
      val dimensionSql = generateDimensionSql(queryContext, queryBuilderContext, includePagination)
      val dimQueryString = dimensionSql.drivingDimensionSql
      val aliasColumnMap = queryBuilderContext.aliasColumnMap

      queryContext.requestModel.requestCols.foreach {
        ci =>
          if (aliasColumnMap.contains(ci.alias)) {
            aliasColumnMapOfRequestCols += (ci.alias -> aliasColumnMap(ci.alias))
            outerColumns += renderOuterColumn(ci, queryBuilderContext, Map.empty, false, true, queryContext)
          } else if (ci.isInstanceOf[ConstantColumnInfo]) {
            outerColumns += renderOuterColumn(ci, queryBuilderContext, Map.empty, false, true, queryContext)
          }
      }

      aliasColumnMap.foreach {
        case (alias, column) =>
          if((!aliasColumnMapOfRequestCols.contains(alias)) && queryContext.indexAliasOption.contains(alias)) {
            val ci = DimColumnInfo(alias)
            aliasColumnMapOfRequestCols += (ci.alias -> aliasColumnMap(ci.alias))
            outerColumns += renderOuterColumn(ci, queryBuilderContext, Map.empty, false, true, queryContext)
          }
      }
      val outerWhereClause = generateOuterWhereClause(queryContext, queryBuilderContext)

      if (queryContext.requestModel.includeRowCount && !queryContext.requestModel.hasFactSortBy) {
        if(dimensionSql.hasTotalRows) {
          outerColumns += OracleQueryGenerator.ROW_COUNT_ALIAS
        } else {
          outerColumns += PAGINATION_ROW_COUNT
        }
        aliasColumnMapOfRequestCols += (OracleQueryGenerator.ROW_COUNT_ALIAS -> PAGINATION_ROW_COUNT_COL)
      }
      val finalQueryString = String.format(queryStringTemplate, outerColumns.mkString(", "), dimQueryString, outerWhereClause)
      val queryString = if(dimensionSql.hasPagination) finalQueryString else {
        addOuterPaginationWrapper(finalQueryString, queryContext.requestModel.maxRows, queryContext.requestModel.startIndex, includePagination,
          requestModel.outerFilters.size > 0)
      }
      new OracleQuery(
        queryContext,
        queryString,
        paramBuilder.build(),
        aliasColumnMapOfRequestCols.toMap,
        additionalColumns(queryContext)
      )
    }
  }

  private[this] def applyDataTypeCleanup(alias: String, column: Column, queryContext: QueryContext): String = {

    def round(alias: String): String = s"ROUND($alias, 10)"
    def roundWithScale(alias: String, scale:Int): String = {
      val scaleDerived = {
        if(scale == 0)
          10
        else scale
      }
      s"ROUND($alias, $scaleDerived)"
    }
    def coalesce(alias: String, default: String): String = s"""coalesce($alias, $default)"""
    def toChar(alias: String): String = s"""to_char($alias)"""

    column match {
      case col if col.isInstanceOf[FactColumn] =>
        col.dataType match {

          case DecType(_, scale, Some(default), Some(min), Some(max), _)
            if col.isInstanceOf[DerivedColumn] || col.asInstanceOf[FactColumn].rollupExpression.isInstanceOf[CustomRollup] =>
            val minMaxClause = s"CASE WHEN (($alias >= $min) AND ($alias <= $max)) THEN $alias ELSE $default END"
            coalesce(roundWithScale(minMaxClause, scale), default.toString())
          case IntType(_, _, Some(default), Some(min), Some(max))
            if col.isInstanceOf[DerivedColumn] || col.asInstanceOf[FactColumn].rollupExpression.isInstanceOf[CustomRollup] =>
            val minMaxClause = s"CASE WHEN (($alias >= $min) AND ($alias <= $max)) THEN $alias ELSE $default END"
            coalesce(round(minMaxClause), default.toString)
          case IntType(_, _, default, _, _) if default.isDefined => coalesce(alias, default.get.toString)
          case DecType(_, _, default, _, _, _) if default.isDefined => coalesce(round(alias), default.get.toString())
          case DecType(_, _, _, _, _, _) => round(alias)
          case _ => alias
        }
      case col if col.isInstanceOf[DimensionColumn] =>
        col.dataType match {
          case IntType(_, sm, _, _, _) if sm.isDefined && queryContext.requestModel.isDimDriven =>
            val defaultValue = sm.get.default
            s"""COALESCE(${alias}, '$defaultValue')"""
          case StrType(_, sm, _) if sm.isDefined && queryContext.requestModel.isDimDriven =>
            val defaultValue = sm.get.default
            s"""COALESCE(${alias}, '$defaultValue')"""
          case IntType(_, _, _, _, _) | DecType(_, _, _, _, _, _) => toChar(alias)
          case DateType(fmt) if fmt.isDefined => s"to_char($alias, '${fmt.get}')"
          case _ => alias
        }
      case col =>
        throw new UnsupportedOperationException(s"Unhandled column type : $col")
    }
  }

  private[this] def renderOuterColumn(columnInfo: ColumnInfo, queryBuilderContext: QueryBuilderContext, duplicateAliasMapping: Map[String, Set[String]], isFactOnlyQuery: Boolean, isDimOnly: Boolean, queryContext: QueryContext): String = {

    def renderFactCol(alias: String, finalAlias: String, col: Column): String = {
      val postFilterAlias = applyDataTypeCleanup(finalAlias, col, queryContext)
      s"""$postFilterAlias "$alias""""
    }

    def handleFactColumn(alias: String) : String = {
      if (queryBuilderContext.containsFactColNameForAlias(alias)) {
        val col = queryBuilderContext.getFactColByAlias(alias)
        val finalAlias = queryBuilderContext.getFactColNameForAlias(alias)
        renderFactCol(alias, finalAlias, col)
      } else if (duplicateAliasMapping.contains(alias)) {
        val duplicateAliases = duplicateAliasMapping(alias)
        val renderedDuplicateAlias = duplicateAliases.collectFirst {
          case duplicateAlias if queryBuilderContext.containsFactColNameForAlias(duplicateAlias) =>
            val col = queryBuilderContext.getFactColByAlias(duplicateAlias)
            val finalAlias = queryBuilderContext.getFactColNameForAlias(duplicateAlias)
            renderFactCol(alias, finalAlias, col)

        }
        require(renderedDuplicateAlias.isDefined, s"Failed to render column : $alias")
        renderedDuplicateAlias.get
      } else {
        throw new IllegalArgumentException(s"Could not find inner alias for outer column : $alias")
      }
    }

    columnInfo match {
      case FactColumnInfo(alias) if isDimOnly =>
        val col = queryBuilderContext.getDimensionColByAlias(alias)
        val finalAlias = queryBuilderContext.getDimensionColNameForAlias(alias)
        val postFilterAlias = applyDataTypeCleanup(finalAlias, col, queryContext)
        s"""$postFilterAlias "$alias""""
      case FactColumnInfo(alias) =>
        handleFactColumn(alias)
      case DimColumnInfo(alias) if isFactOnlyQuery =>
        handleFactColumn(alias)
      case DimColumnInfo(alias) =>
        val col = queryBuilderContext.getDimensionColByAlias(alias)
        val finalAlias = queryBuilderContext.getDimensionColNameForAlias(alias)
        val postFilterAlias = applyDataTypeCleanup(finalAlias, col, queryContext)
        s"""$postFilterAlias "$alias""""
      case ConstantColumnInfo(alias, value) =>
        s"""'$value' AS "$alias""""
      case _ => throw new UnsupportedOperationException("Unsupported Column Type")
    }
  }


  private[this] def renderSortByColumn(columnInfo: SortByColumnInfo, queryBuilderContext: QueryBuilderContext): String = {
    columnInfo match {
      case FactSortByColumnInfo(alias, order) =>
        s""""${columnInfo.alias}" ${columnInfo.order.toString} NULLS LAST"""
      case DimSortByColumnInfo(alias, order) =>
        val column = queryBuilderContext.getDimensionColByAlias(alias)
        if (column.isKey) {
          s""""${columnInfo.alias}" ${columnInfo.order.toString}"""
        } else {
          s""""${columnInfo.alias}" ${columnInfo.order.toString} NULLS LAST"""
        }
      case _ => throw new UnsupportedOperationException("Unsupported Sort By Column Type")
    }
  }

  private[this] def renderRollupExpression(expression: String, rollupExpression: RollupExpression, renderedColExp: Option[String] = None): String = {
    rollupExpression match {
      case SumRollup => s"SUM(${renderedColExp.getOrElse(expression)})"
      case MaxRollup => s"MAX(${renderedColExp.getOrElse(expression)})"
      case MinRollup => s"MIN(${renderedColExp.getOrElse(expression)})"
      case AverageRollup => s"AVG(${renderedColExp.getOrElse(expression)})"
      case OracleCustomRollup(exp) => s"(${exp.render(expression, Map.empty, renderedColExp)})"
      case NoopRollup => s"(${renderedColExp.getOrElse(expression)})"
      case any => throw new UnsupportedOperationException(s"Unhandled rollup expression : $any")
    }
  }

  private[this] def generateInSubqueryFilters(bundle: DimensionBundle): Set[String] = {
    val filters = new collection.mutable.TreeSet[String]
    val aliasToNameMapFull = bundle.publicDim.aliasToNameMapFull
    val columnsByNameMap = bundle.dim.columnsByNameMap
    bundle.filters.foreach {
      filter =>
        filters += FilterSql.renderFilter(
          filter,
          aliasToNameMapFull,
          columnsByNameMap,
          OracleEngine,
          literalMapper).filter
    }
    filters.toSet
  }

  private[this] def generateDimFactQuery(queryContext: CombinedQueryContext): Query = {
    val paramBuilder = new QueryParameterBuilder
    val queryBuilderContext = new QueryBuilderContext
    val queryBuilder: QueryBuilder = new QueryBuilder(
      queryContext.requestModel.requestCols.size + 5
      , queryContext.requestModel.requestSortByCols.size + 1)
    val requestModel = queryContext.requestModel
    val optionalHint: String = getFactOptionalHint(queryContext.factBestCandidate.fact, requestModel).map(toComment).getOrElse("")
    val aliasColumnMapOfRequestCols = new mutable.HashMap[String, Column]()
    val isFactOnlyQuery = requestModel.isFactDriven && queryContext.dims.forall {
      db => (db.fields.filterNot(db.publicDim.isPrimaryKeyAlias).isEmpty && !db.hasNonFKSortBy
        && queryContext.factBestCandidate.publicFact.foreignKeyAliases(db.publicDim.primaryKeyByAlias))
    }
    val requestColAliasesSet = requestModel.requestCols.map(_.alias).toSet
    val factOnlySubqueryFields : Set[String] = if(isFactOnlyQuery) {
      queryContext.dims.view.map(_.publicDim.primaryKeyByAlias).filterNot(requestColAliasesSet).toSet
    } else Set.empty

    def generateDimJoin(): Unit = {
      if (queryContext.dims.nonEmpty) {
        val dsql = generateDimensionSql(queryContext, queryBuilderContext, true)
        queryBuilder.addDimensionJoin(dsql.drivingDimensionSql)
        //TODO: add support for optimal mutli dimension sort by metric query
        //TODO: right now it just does join with driving table
        dsql.multiDimensionJoinSql.foreach(queryBuilder.addMultiDimensionJoin)
      }
    }

    def generateOrderBy(): Unit = {
      if(requestModel.isDimDriven && requestModel.hasDrivingDimNonFKNonPKSortBy) {
        // In Dim driven case, if driving dimension has orderBy then we do not want to orderBy again in the outer as it mess up the order
      } else {
        requestModel.requestSortByCols.foreach {
          ci =>
            queryBuilder.addOrderBy(renderSortByColumn(ci, queryBuilderContext))
        }
      }
    }

    def generateWhereAndHavingClause(): Unit = {
      // inner fact where clauses
      val fact = queryContext.factBestCandidate.fact
      val publicFact = queryContext.factBestCandidate.publicFact
      val filters = queryContext.factBestCandidate.filters
      val allFilters = publicFact.forcedFilters //++ filters  need to append regular filters or pass in
      val whereFilters = new mutable.LinkedHashSet[String]
      val havingFilters = new mutable.LinkedHashSet[String]
      var escaped = false
      val hasPartitioningScheme = fact.annotations.contains(OracleQueryGenerator.ANY_PARTITIONING_SCHEME)

      //add subquery
      if(isFactOnlyQuery) {
        queryContext.dims.foreach {
          subqueryBundle =>
            val factFKCol = fact.publicDimToForeignKeyMap(subqueryBundle.publicDim.name)
            val factFkColAlias = {
              if (fact.columnsByNameMap.contains(factFKCol)) {
                fact.columnsByNameMap.get(factFKCol).get.alias
              } else {
                None
              }
            }
            val (sql, wasEscaped) = generateSubqueryFilter(factFkColAlias.getOrElse(factFKCol), filters, subqueryBundle)
            escaped |= wasEscaped
            whereFilters += sql
        }
      }

      if (requestModel.isFactDriven || requestModel.dimensionsCandidates.isEmpty || requestModel.hasNonFKFactFilters || requestModel.hasFactSortBy || fact.forceFilters.nonEmpty) {
        val unique_filters = removeDuplicateIfForced( filters.toSeq, allFilters.toSeq, queryContext )
        unique_filters.sorted.foreach {
          filter =>
            val name = publicFact.aliasToNameColumnMap(filter.field)
            if (fact.dimColMap.contains(name)) {
              val f = FilterSql.renderFilter(
                filter,
                queryContext.factBestCandidate.publicFact.aliasToNameColumnMap,
                fact.columnsByNameMap,
                OracleEngine,
                literalMapper)
              escaped |= f.escaped
              whereFilters += f.filter
            } else if (fact.factColMap.contains(name)) {
              val column = fact.columnsByNameMap(name)
              val alias = queryContext.factBestCandidate.factColMapping(name)
              val exp = column match {
                case FactCol(_, dt, cc, rollup, _, annotations, _) =>
                  s"""${renderRollupExpression(name, rollup)}"""
                case OracleDerFactCol(_, _, dt, cc, de, annotations, rollup, _) =>
                  s"""${renderRollupExpression(de.render(name, Map.empty), rollup)}"""
                case any =>
                  throw new UnsupportedOperationException(s"Found non fact column : $any")
              }
              val f = FilterSql.renderFilter(
                filter,
                queryContext.factBestCandidate.publicFact.aliasToNameColumnMap,
                fact.columnsByNameMap,
                OracleEngine,
                literalMapper,
                Option(exp)
              )
              escaped |= f.escaped
              havingFilters += f.filter
            } else {
              throw new IllegalArgumentException(
                s"Unknown fact column: publicFact=${publicFact.name}, fact=${fact.name} alias=${filter.field}, name=$name")
            }
        }
      }
      val dayFilter = FilterSql.renderFilter(
        requestModel.localTimeDayFilter,
        queryContext.factBestCandidate.publicFact.aliasToNameColumnMap,
        fact.columnsByNameMap,
        OracleEngine,
        literalMapper).filter

      val combinedQueriedFilters = {
        if (hasPartitioningScheme) {
          val partitionFilters = new mutable.LinkedHashSet[String]
          val partitionFilterOption = partitionColumnRenderer.renderFact(queryContext, literalMapper, OracleEngine)
          if(partitionFilterOption.isDefined) {
            partitionFilters += partitionFilterOption.get
            AndFilter(partitionFilters ++ whereFilters)
          } else {
            AndFilter(whereFilters + dayFilter)
          }
        } else {
          AndFilter(whereFilters + dayFilter)
        }
      }

      val whereClauseExpression = s"""WHERE ${combinedQueriedFilters.toString} """
      queryBuilder.setWhereClause(whereClauseExpression)

      if (havingFilters.nonEmpty) {
        val havingAndFilters = AndFilter(havingFilters.toSet)
        val havingClauseExpression = s"""HAVING ${havingAndFilters.toString}"""
        queryBuilder.setHavingClause(havingClauseExpression)
      }

    }


    def generateFactViewColumns(): Unit = {
      val factTableAlias = queryBuilderContext.getAliasForTable(queryContext.factBestCandidate.fact.name)
      val fact = queryContext.factBestCandidate.fact

      def renderColumnName(column: Column): String = {
        //column.alias.fold(column.name)(alias => s"""$alias AS ${column.name}""")
        column.alias.getOrElse(column.name)
      }

      def renderStaticMappedDimension(column: Column) : String = {
        val nameOrAlias = renderColumnName(column)
        column.dataType match {
          case IntType(_, sm, _, _, _) if sm.isDefined =>
            val defaultValue = sm.get.default
            val whenClauses = sm.get.tToStringMap.map {
              case (from, to) => s"WHEN (${nameOrAlias} IN ($from)) THEN '$to'"
            }
            s"CASE ${whenClauses.mkString(" ")} ELSE '$defaultValue' END"
          case StrType(_, sm, _) if sm.isDefined =>
            val defaultValue = sm.get.default
            val decodeValues = sm.get.tToStringMap.map {
              case (from, to) => s"'$from', '$to'"
            }
            s"""DECODE(${nameOrAlias}, ${decodeValues.mkString(", ")}, '$defaultValue')"""
          case _ =>
            nameOrAlias
        }
      }

      def renderColumnWithAlias(fact: Fact, column: Column, alias: String, requiredInnerCols: Set[String]): Unit = {
        val name = column.alias.getOrElse(column.name)
        val exp = column match {
          case any if queryBuilderContext.containsColByName(name) =>
            //do nothing, we've already processed it
            ""
          case DimCol(_, dt, cc, _, annotations, _) if dt.hasStaticMapping =>
            queryBuilderContext.setFactColAlias(alias, s"""$factTableAlias.$name""", column)
            s"${renderStaticMappedDimension(column)} $name"
          case DimCol(_, dt, cc, _, annotations, _) =>
            queryBuilderContext.setFactColAlias(alias, s"""$factTableAlias.$name""", column)
            renderColumnName(column)
          case OraclePartDimCol(_, dt, cc, _, annotations, _) =>
            queryBuilderContext.setFactColAlias(alias, s"""$factTableAlias.$name""", column)
            renderColumnName(column)
          case OracleDerDimCol(_, dt, cc, de, _, annotations, _) =>
            val renderedAlias = s""""$alias""""
            queryBuilderContext.setFactColAlias(alias, s"""$factTableAlias.$renderedAlias""", column)
            s"""${de.render(name, Map.empty)} AS $renderedAlias"""
          case FactCol(_, dt, cc, rollup, _, annotations, _) =>
            val nameOrAlias = column.alias.getOrElse(name)
            column.dataType match {
              case DecType(_, _, Some(default), Some(min), Some(max), _) =>
                val renderedAlias = s""""$name""""
                val minMaxClause = s"CASE WHEN (($nameOrAlias >= $min) AND ($nameOrAlias <= $max)) THEN $nameOrAlias ELSE $default END"
                queryBuilderContext.setFactColAlias(alias, s"""$factTableAlias.$renderedAlias""", column)
                s"${renderRollupExpression(nameOrAlias, rollup, Option(minMaxClause))} AS $renderedAlias"
              case IntType(_, _, Some(default), Some(min), Some(max)) =>
                val renderedAlias = s""""$name""""
                val minMaxClause = s"CASE WHEN (($nameOrAlias >= $min) AND ($nameOrAlias <= $max)) THEN $nameOrAlias ELSE $default END"
                queryBuilderContext.setFactColAlias(alias, s"""$factTableAlias.$renderedAlias""", column)
                s"${renderRollupExpression(nameOrAlias, rollup, Option(minMaxClause))} AS $renderedAlias"
              case _ =>
                val renderedAlias = s""""$name""""
                queryBuilderContext.setFactColAlias(alias, s"""$factTableAlias.$renderedAlias""", column)
                s"""${renderRollupExpression(nameOrAlias, rollup)} AS $renderedAlias"""
            }
          case OracleDerFactCol(_, _, dt, cc, de, annotations, rollup, _)
            if queryContext.factBestCandidate.filterCols.contains(name) || de.expression.hasRollupExpression || requiredInnerCols(name) =>
            val renderedAlias = s""""$alias""""
            queryBuilderContext.setFactColAlias(alias, s"""$factTableAlias.$renderedAlias""", column)
            s"""${renderRollupExpression(de.render(name, Map.empty), rollup)} AS $renderedAlias"""
          case OracleDerFactCol(_, _, dt, cc, de, annotations, _, _) =>
            //means no fact operation on this column, push expression outside
            de.sourceColumns.foreach {
              case src if src != name =>
                val sourceCol = fact.columnsByNameMap(src)
                renderColumnWithAlias(fact, sourceCol, sourceCol.name, requiredInnerCols)
              case _ => //do nothing if we reference ourselves
            }
            queryBuilderContext.setFactColAlias(alias, s"""(${de.render(name
              , queryBuilderContext.getColAliasToFactColNameMap
              , columnPrefix = Option(s"$factTableAlias.")
              , expandDerivedExpression = false)})""", column)
            ""
          case any =>
            throw new UnsupportedOperationException(s"Found non unhandled column : $any")
        }
        queryBuilder.addFactViewColumn(exp)
      }

      val dimCols = queryContext.factBestCandidate.dimColMapping.toList.collect {
        case (dimCol, alias) if queryContext.factBestCandidate.requestCols(dimCol) =>
          val column = fact.columnsByNameMap(dimCol)
          (column, alias)
      }

      //render derived columns last
      val groupDimCols = dimCols.groupBy(_._1.isDerivedColumn)
      groupDimCols.toList.sortBy(_._1).foreach {
        case (_, list) => list.foreach {
          case (column, alias) =>
            val name = column.name
            val nameOrAlias = column.alias.getOrElse(name)
            if(!factOnlySubqueryFields(alias)) {
              renderColumnWithAlias(fact, column, alias, Set.empty)
            }
            if (column.isDerivedColumn) {
              val derivedExpressionExpanded: String = column.asInstanceOf[DerivedDimensionColumn].derivedExpression.render(name, Map.empty).asInstanceOf[String]
              queryBuilder.addGroupBy( s"""$derivedExpressionExpanded""")
            } else {
              if(!factOnlySubqueryFields(alias)) {
                if(column.dataType.hasStaticMapping) {
                  queryBuilder.addGroupBy(renderStaticMappedDimension(column))
                } else {
                  queryBuilder.addGroupBy(nameOrAlias)
                }
              }
            }
        }
      }

      val factCols = queryContext.factBestCandidate.factColMapping.toList.collect {
        case (nonFkCol, alias) if queryContext.factBestCandidate.requestCols(nonFkCol) =>
          (fact.columnsByNameMap(nonFkCol), alias)
      }

      val groupedFactCols = factCols.groupBy(_._1.isDerivedColumn)
      //render non derived columns first
      groupedFactCols.get(false).foreach { nonDerivedCols =>
        nonDerivedCols.foreach {
          case (column, alias) =>
            val renderedAlias = s""""$alias""""
            renderColumnWithAlias(fact, column, alias, Set.empty)
        }
      }

      //render derived columns last
      groupedFactCols.get(true).foreach { derivedCols =>
        val requiredInnerCols: Set[String] =
          derivedCols.view.map(_._1.asInstanceOf[DerivedColumn]).flatMap(dc => dc.derivedExpression.sourceColumns).toSet
        derivedCols.foreach {
          case (column, alias) =>
            val renderedAlias = s""""$alias""""
            renderColumnWithAlias(fact, column, alias, requiredInnerCols)
        }
      }

      if (requestModel.includeRowCount && requestModel.isFactDriven) {
        queryBuilder.addFactViewColumn(PAGINATION_ROW_COUNT)
      }
    }

    def generateOuterColumns(): Unit = {
      // add requested dim and fact columns, this should include constants
      queryContext.requestModel.requestCols foreach {
        columnInfo =>
          if (!columnInfo.isInstanceOf[ConstantColumnInfo] && queryBuilderContext.aliasColumnMap.contains(columnInfo.alias)) {
            aliasColumnMapOfRequestCols += (columnInfo.alias -> queryBuilderContext.aliasColumnMap(columnInfo.alias))
          } else if (queryContext.factBestCandidate.duplicateAliasMapping.contains(columnInfo.alias)) {
            val sourceAliases = queryContext.factBestCandidate.duplicateAliasMapping(columnInfo.alias)
            val sourceAlias = sourceAliases.find(queryBuilderContext.aliasColumnMap.contains)
            require(sourceAlias.isDefined
              , s"Failed to find source column for duplicate alias mapping : ${queryContext.factBestCandidate.duplicateAliasMapping(columnInfo.alias)}")
            aliasColumnMapOfRequestCols += (columnInfo.alias -> queryBuilderContext.aliasColumnMap(sourceAlias.get))
          }
            queryBuilder.addOuterColumn(renderOuterColumn(columnInfo, queryBuilderContext, queryContext.factBestCandidate.duplicateAliasMapping, isFactOnlyQuery, false, queryContext))
      }

      if (queryContext.requestModel.includeRowCount) {
        queryBuilder.addOuterColumn(OracleQueryGenerator.ROW_COUNT_ALIAS)
        aliasColumnMapOfRequestCols += (OracleQueryGenerator.ROW_COUNT_ALIAS -> PAGINATION_ROW_COUNT_COL)
      }

      val outerWhereClause = generateOuterWhereClause(queryContext, queryBuilderContext)
      queryBuilder.setOuterWhereClause(outerWhereClause.toString)
    }

    /*
     1. generate query builder
     */
    generateFactViewColumns()
    generateWhereAndHavingClause()
    //only generate dim if we are not fact only
    if(!isFactOnlyQuery) {
      generateDimJoin()
    }
    generateOuterColumns()
    generateOrderBy()
    /*
     2. query parameters
     */

    val orderByClause = {
      if (requestModel.requestSortByCols.nonEmpty) {
        queryBuilder.getOrderByClause
      } else StringUtils.EMPTY
    }

    val queryStringPaginated = {
      // fill out query string
      val queryString =
        s"""SELECT *
FROM (SELECT ${queryBuilder.getOuterColumns}
      FROM (SELECT $optionalHint
                   ${queryBuilder.getFactViewColumns}
            FROM ${getFactAlias(queryContext.factBestCandidate.fact.name, queryContext.dims.map(_.dim).toSet)}
            ${queryBuilder.getWhereClause}
            ${queryBuilder.getGroupByClause}
            ${queryBuilder.getHavingClause}
           ) ${queryBuilderContext.getAliasForTable(queryContext.factBestCandidate.fact.name)}
${queryBuilder.getJoinExpressions}
) ${queryBuilder.getOuterWhereClause}
   $orderByClause"""

      if (requestModel.isSyncRequest && (requestModel.isFactDriven || requestModel.hasFactSortBy)) {
        addPaginationWrapper(queryString, queryContext.requestModel.maxRows, queryContext.requestModel.startIndex, true)
      } else {
        queryString
      }
    }

    new OracleQuery(
      queryContext,
      queryStringPaginated,
      paramBuilder.build(),
      aliasColumnMapOfRequestCols.toMap,
      additionalColumns(queryContext)
    )
  }

  private[this] def getFactAlias(name: String, dims: Set[Dimension]): String = {
    // if hash partition supported
    if (dims.exists(_.annotations.contains(OracleAdvertiserHashPartitioning))) {
      s"$name $factAlias"
    } else {
      s"$name"
    }
  }

  private[this] def getDimOptionalPkIndex(dim: Dimension): Option[PKCompositeIndex] = {
    dim.annotations.find(_.isInstanceOf[PKCompositeIndex]).map(_.asInstanceOf[PKCompositeIndex])
  }

  private[this] def getFactOptionalHint(fact: Fact, requestModel: RequestModel): Option[String] = {
    fact.annotations.foldLeft(Option.empty[String]) {
      (optionalHint, annotation) =>
        if (annotation.isInstanceOf[OracleFactDimDrivenHint] && requestModel.isDimDriven) {
          Option(annotation.asInstanceOf[OracleFactDimDrivenHint].hint)
        } else {
          if (annotation.isInstanceOf[OracleFactStaticHint] && optionalHint.isEmpty) {
            Option(annotation.asInstanceOf[OracleFactStaticHint].hint)
          } else {
            optionalHint
          }
        }
    }
  }

  private[this] def additionalColumns(queryContext: QueryContext): IndexedSeq[String] = {
    if (queryContext.requestModel.includeRowCount) {
      ADDITIONAL_PAGINATION_COLUMN
    } else {
      IndexedSeq.empty[String]
    }
  }
}

object OracleQueryGenerator extends Logging {
  val ROW_COUNT_ALIAS: String = "TOTALROWS"
  val ANY_PARTITIONING_SCHEME = OraclePartitioningScheme("") //no name needed since class name hashcode

  def register(queryGeneratorRegistry: QueryGeneratorRegistry, partitionColumnRenderer:PartitionColumnRenderer) = {
    if (!queryGeneratorRegistry.isEngineRegistered(OracleEngine)) {
      val generator = new OracleQueryGenerator(partitionColumnRenderer)
      queryGeneratorRegistry.register(OracleEngine, generator)
    } else {
      queryGeneratorRegistry.getGenerator(OracleEngine).foreach {
        qg =>
          if (!qg.isInstanceOf[OracleQueryGenerator]) {
            warn(s"Another query generator registered for OracleEngine : ${qg.getClass.getCanonicalName}")
          }
      }
    }
  }
}
