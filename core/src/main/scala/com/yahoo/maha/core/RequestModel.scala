// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import com.yahoo.maha.core
import com.yahoo.maha.core.MetaType.MetaType
import com.yahoo.maha.core.bucketing.{BucketParams, BucketSelector, CubeBucketSelected}
import com.yahoo.maha.core.dimension.PublicDimension
import com.yahoo.maha.core.fact.{BestCandidates, PublicFact, PublicFactCol, PublicFactColumn}
import com.yahoo.maha.core.registry.{FactRowsCostEstimate, Registry}
import com.yahoo.maha.core.request.Parameter.Distinct
import com.yahoo.maha.core.request._
import com.yahoo.maha.core.error._
import com.yahoo.maha.core.query.{InnerJoin, JoinType, LeftOuterJoin, RightOuterJoin}
import com.yahoo.maha.utils.DaysUtils
import grizzled.slf4j.Logging
import org.slf4j.LoggerFactory

import scala.collection.{SortedSet, mutable}
import scala.util.{Failure, Success, Try}

/**
 * Created by jians on 10/5/15.
 */
case class DimensionCandidate(dim: PublicDimension
                              , fields: Set[String]
                              , filters: SortedSet[Filter]
                              , upperCandidates: List[PublicDimension]
                              , lowerCandidates: List[PublicDimension]
                              , isDrivingDimension: Boolean
                              , hasNonFKOrForcedFilters: Boolean
                              , hasNonFKNonForceFilters: Boolean
                              , hasNonFKSortBy: Boolean
                              , hasNonFKNonPKSortBy: Boolean
                              , hasLowCardinalityFilter: Boolean
                              , hasPKRequested : Boolean
                              , hasNonPushDownFilters : Boolean
                             ) {

  def debugString : String = {
    s"""
       fields=$fields
       filters=${filters.map(_.field)}
       upperCandidates=${upperCandidates.map(_.name)}
       lowerCandidates=${lowerCandidates.map(_.name)}
       isDrivingDimension=$isDrivingDimension
       hasNonFKOrForcedFilters=$hasNonFKOrForcedFilters
       hasNonFKNonForceFilters=$hasNonFKNonForceFilters
       hasNonFKSortBy=$hasNonFKSortBy
       hasNonFKNonPKSortBy=$hasNonFKNonPKSortBy
       hasLowCardinalityFilter=$hasLowCardinalityFilter
       hasPKRequested=$hasPKRequested
       hasNonPushDownFilters=$hasNonPushDownFilters
     """
  }
}
case class DimensionRelations(relations: Map[(String, String), Boolean]) {
  val hasUnrelatedDimensions: Boolean = relations.exists(!_._2)
}

object DimensionCandidate {
  implicit val ordering: Ordering[DimensionCandidate] = Ordering.by(dc => s"${dc.dim.dimLevel.level}-${dc.dim.name}")
}

sealed trait ColumnInfo {
  def alias: String
}
case class FactColumnInfo(alias: String) extends ColumnInfo
case class DimColumnInfo(alias: String) extends ColumnInfo
case class ConstantColumnInfo(alias: String, value: String) extends ColumnInfo

sealed trait RequestCol {
  def alias: String
  def isKey: Boolean
  def isJoinKey: Boolean
  final override def hashCode: Int = alias.hashCode
  final override def equals(other: Any) : Boolean = {
    if(other == null) return false
    other match {
      case rc: RequestCol =>
        alias.equals(rc.alias)
      case _ => false
    }
  }
  def toName(name: String) : RequestCol
}
object RequestCol {
  implicit val ordering: Ordering[RequestCol] = Ordering.by(_.alias)
}

object BaseRequestCol {
  def apply(s: String) : BaseRequestCol = new BaseRequestCol(s)
}
class BaseRequestCol(val alias: String) extends RequestCol {
  def isKey : Boolean = false
  def isJoinKey: Boolean = false
  def toName(name: String) : RequestCol = new BaseRequestCol(name)
}
class JoinKeyCol(val alias: String) extends RequestCol {
  def isKey : Boolean = true
  def isJoinKey: Boolean = true
  def toName(name: String) : RequestCol = new JoinKeyCol(name)
}

trait SortByColumnInfo {
  def alias: String
  def order: Order
}

case class DimSortByColumnInfo(alias: String, order: Order) extends SortByColumnInfo
case class FactSortByColumnInfo(alias: String, order: Order) extends SortByColumnInfo

case class RequestModel(cube: String
                        , bestCandidates: Option[BestCandidates]
                        , factFilters: SortedSet[Filter]
                        , dimensionsCandidates: SortedSet[DimensionCandidate]
                        , requestCols: IndexedSeq[ColumnInfo]
                        , requestSortByCols: IndexedSeq[SortByColumnInfo]
                        , dimColumnAliases: Set[String]
                        , dimCardinalityEstimate: Option[Long]
                        , factCost: Map[(String, Engine), FactRowsCostEstimate]
                        , factSortByMap : Map[String, Order]
                        , dimSortByMap : Map[String, Order]
                        , hasFactFilters: Boolean
                        , hasMetricFilters: Boolean
                        , hasNonFKFactFilters: Boolean
                        , hasDimFilters: Boolean
                        , hasNonFKDimFilters: Boolean
                        , hasFactSortBy: Boolean
                        , hasDimSortBy: Boolean
                        , isFactDriven: Boolean
                        , forceDimDriven: Boolean
                        , forceFactDriven: Boolean
                        , hasNonDrivingDimSortOrFilter: Boolean
                        , hasDrivingDimNonFKNonPKSortBy: Boolean
                        , hasNonDrivingDimNonFKNonPKFilter: Boolean
                        , anyDimHasNonFKNonForceFilter: Boolean
                        , schema: Schema
                        , utcTimeDayFilter: Filter
                        , localTimeDayFilter: Filter
                        , utcTimeHourFilter: Option[Filter]
                        , localTimeHourFilter: Option[Filter]
                        , utcTimeMinuteFilter: Option[Filter]
                        , localTimeMinuteFilter: Option[Filter]
                        , requestType: RequestType
                        , startIndex: Int
                        , maxRows: Int
                        , includeRowCount: Boolean
                        , isDebugEnabled: Boolean
                        , additionalParameters : Map[Parameter, Any]
                        , factSchemaRequiredAliasesMap: Map[String, Set[String]]
                        , reportingRequest: ReportingRequest
                        , queryGrain: Option[Grain]=Option.apply(DailyGrain)
                        , isRequestingDistict:Boolean
                        , hasLowCardinalityDimFilters: Boolean
                        , requestedDaysWindow: Int
                        , requestedDaysLookBack: Int
                        , outerFilters: SortedSet[Filter]
                        , requestedFkAliasToPublicDimensionMap: Map[String, PublicDimension]
                        , orFilterMeta: Set[OrFilterMeta]
                        , dimensionRelations: DimensionRelations
  ) {

  val requestColsSet: Set[String] = requestCols.map(_.alias).toSet
  lazy val dimFilters: SortedSet[Filter] = dimensionsCandidates.flatMap(_.filters)

  def isDimDriven: Boolean = !isFactDriven
  def hasDimAndFactOperations: Boolean = (hasNonFKDimFilters || hasDimSortBy) && (hasNonFKFactFilters || hasFactSortBy)
  def isTimeSeries: Boolean = requestCols.exists(ci => Grain.grainFields(ci.alias))
  def isAsyncRequest : Boolean = requestType == AsyncRequest
  def isSyncRequest : Boolean = requestType == SyncRequest
  def forceQueryEngine: Option[Engine] = additionalParameters.get(Parameter.QueryEngine).map(_.asInstanceOf[QueryEngineValue].value)

  /*
  Map to store the dimension name to JoinType associated with the given dimension based on the different constraints.
  defaultJoinType has higher preference than the joinType associated with the dimensions.
   */
  val publicDimToJoinTypeMap : Map[String, JoinType]  = {
    //dim driven query
    //1. fact ROJ driving dim (filter or no filter)
    //2. fact ROJ driving dim (filter or no filter) LOJ parent dim LOJ parent dim
    //3. fact ROJ driving dim IJ parent dim IJ parent dim
    //4. fact IJ driving dim [IJ parent dim IJ parent dim] (metric filter)
    //fact driven query
    //1. fact LOJ driving dim (no filter)
    //2. fact LOJ driving dim (no filter) LOJ parent dim (no filter) LOJ parent dim (no filter)
    //3. fact IJ driving dim (filter on anything)
    //4. fact IJ driving dim IJ parent dim IJ parent dim

    val schema: Schema = reportingRequest.schema
    val anyDimsHasSchemaRequiredNonKeyField: Boolean = dimensionsCandidates.exists(
      _.dim.schemaRequiredAlias(schema).exists(!_.isKey))
    dimensionsCandidates.map {
      dc =>
        //driving dim case
        if(dc.isDrivingDimension) {
          val joinType = if(forceDimDriven) {
            if(hasMetricFilters) {
              InnerJoin
            } else {
              RightOuterJoin
            }
          } else {
            if(anyDimHasNonFKNonForceFilter || anyDimsHasSchemaRequiredNonKeyField) {
              InnerJoin
            } else {
              LeftOuterJoin
            }

          }
          dc.dim.name -> joinType
        } else {
          //non driving dim case
          val joinType = if(forceDimDriven) {
              InnerJoin
          } else {
            if(anyDimHasNonFKNonForceFilter || anyDimsHasSchemaRequiredNonKeyField) {
              InnerJoin
            } else {
              LeftOuterJoin
            }
          }
          dc.dim.name -> joinType
        }
    }.toMap
  }

  utcTimeDayFilter match {
    case BetweenFilter(field, from, to) =>
      require(!DaysUtils.isFutureDate(from), FutureDateNotSupportedError(from))
    case EqualityFilter(field, date, _, _) =>
      require(!DaysUtils.isFutureDate(date), FutureDateNotSupportedError(date))
    case InFilter(field, dates, _, _) =>
      require(!dates.forall(date => DaysUtils.isFutureDate(date)), FutureDateNotSupportedError(dates.mkString(", ")))
    case a =>
      throw new IllegalArgumentException(s"Filter operation not supported. Day filter can be between, in, equality filter : $a")
  }

  def getMostRecentRequestedDate() : String = {
    val mostRecentDate: String = {
      utcTimeDayFilter match {
        case EqualityFilter(field, date, _, _) => date
        case BetweenFilter(field, from, to) => to
        case InFilter(field, dates, _, _) =>
          var minDiff : Integer = scala.Int.MaxValue
          var answer : String = null
          dates.foreach { date =>
            val curDiff = DailyGrain.getDaysFromNow(date)
            if (curDiff < minDiff) {
              minDiff = curDiff
              answer = date
            }
          }
          answer
        case _ =>
          throw new IllegalArgumentException(s"Filter operation not supported. Day filter can be between, in, equality filter.")
      }
    }
    mostRecentDate
  }

  def debugString : String = {
    s"""
       cube=$cube
       requestCols=$requestCols
       requestSortByCols=$requestSortByCols
       dimColumnAliases=$dimColumnAliases
       dimCardinalityEstimate=$dimCardinalityEstimate
       factCost=$factCost
       factSortByMap=$factSortByMap
       dimSortByMap=$dimSortByMap
       hasFactFilters=$hasFactFilters
       hasMetricFilters=$hasMetricFilters
       hasNonFKFactFilters=$hasNonFKFactFilters
       hasDimFilters=$hasDimFilters
       hasNonFKDimFilters=$hasNonFKDimFilters
       hasFactSortBy=$hasFactSortBy
       hasDimSortBy=$hasDimSortBy
       hasNonDrivingDimSortFilter=$hasNonDrivingDimSortOrFilter
       hasDrivingDimNonFKNonPKSortBy=$hasDrivingDimNonFKNonPKSortBy
       hasNonDrivingDimNonFKNonPKFilter=$hasNonDrivingDimNonFKNonPKFilter
       anyDimHasNonFKNonForceFilter=$anyDimHasNonFKNonForceFilter
       hasLowCardinalityDimFilters=$hasLowCardinalityDimFilters
       isFactDriven=$isFactDriven
       forceDimDriven=$forceDimDriven
       forceFactDriven=$forceFactDriven
       schema=$schema
       utcTimeDayFilter=$utcTimeDayFilter
       localTimeDayFilter=$localTimeDayFilter
       requestType=$requestType
       startIndex=$startIndex
       maxRows=$maxRows
       includeRowCount=$includeRowCount
       hasDimAndFactOperations=$hasDimAndFactOperations
       isTimeSeries=$isTimeSeries
       isDebugEnabled=$isDebugEnabled
       additionalParameters=$additionalParameters
       factSchemaRequiredAliasesMap=$factSchemaRequiredAliasesMap
       queryGrain=$queryGrain
       isRequestingDistict=$isRequestingDistict
       publicDimToJoinTypeMap=$publicDimToJoinTypeMap
       dimensionsCandidates=${dimensionsCandidates.map(_.debugString)}
     """
  }
}

object RequestModel extends Logging {
  private[this] val MAX_ALLOWED_STR_LEN = 3999: Int
  def max_allowed_str_len: Int = MAX_ALLOWED_STR_LEN

  val Logger = LoggerFactory.getLogger(classOf[RequestModel])

  def from(request: ReportingRequest, registry: Registry, utcTimeProvider: UTCTimeProvider = PassThroughUTCTimeProvider, revision: Option[Int] = None) : Try[RequestModel] = {
    Try {
      registry.getFact(request.cube, revision) match {
        case None =>
          throw new IllegalArgumentException(s"cube does not exist : ${request.cube}")
        case Some(publicFact) =>
          val fieldMap = new mutable.HashMap[String, Field]()
          // all non-constant fields from request
          val allRequestedAliases = new mutable.TreeSet[String]()
          val allRequestedFactAliases = new mutable.TreeSet[String]()
          val allRequestedFactJoinAliases = new mutable.TreeSet[String]()
          val allRequestedDimensionPrimaryKeyAliases = new mutable.TreeSet[String]()
          val allRequestedNonFactAliases = new mutable.TreeSet[String]()
          val allDependentColumns = new mutable.TreeSet[String]()
          val allProjectedAliases = request.selectFields.map(f=> f.field).toSet

          // populate all requested fields into allRequestedAliases
          request.selectFields.view.filter(field => field.value.isEmpty).foreach { field =>
            allRequestedAliases += field.field
            fieldMap.put(field.field, field)
            //add dependent column
            if(publicFact.dependentColumns(field.field)) {
              allDependentColumns += field.field
            }
          }

          val queryGrain:Option[Grain]= if(fieldMap.contains(HourlyGrain.HOUR_FILTER_FIELD)){
            Option.apply(HourlyGrain)
          } else {
            Option.apply(DailyGrain)
          }
          val isDebugEnabled = request.isDebugEnabled
          val localTimeMinuteFilter = request.minuteFilter
          val localTimeHourFilter = request.hourFilter
          val localTimeDayFilter = request.dayFilter
          val maxDaysWindowOption = publicFact.maxDaysWindow.get(request.requestType, queryGrain.getOrElse(DailyGrain))
          val maxDaysLookBackOption = publicFact.maxDaysLookBack.get(request.requestType, queryGrain.getOrElse(DailyGrain))
          require(maxDaysLookBackOption.isDefined && maxDaysWindowOption.isDefined
            , GranularityNotSupportedError(request.cube, request.requestType, queryGrain.getOrElse(DailyGrain)))
          val maxDaysWindow = maxDaysWindowOption.get
          val maxDaysLookBack = maxDaysLookBackOption.get

          // validating max lookback againt public fact a
          val (requestedDaysWindow, requestedDaysLookBack) = validateMaxLookBackWindow(localTimeDayFilter, publicFact.name, maxDaysWindow, maxDaysLookBack)
          val isAsyncFactDrivenQuery = request.requestType == AsyncRequest && !request.forceDimensionDriven
          val isSyncFactDrivenQuery = request.requestType == SyncRequest && request.forceFactDriven

          val constantValueFields: Set[String] = request.selectFields
            .filter(field => field.value.isDefined)
            .map(field => field.alias.getOrElse(field.field))
            .toSet

          val allRequestedFkAliasToPublicDimMap =
            publicFact.foreignKeyAliases.filter(allProjectedAliases.contains(_)).map {
              case fkAlias =>
               val dimensionOption = registry.getDimensionByPrimaryKeyAlias(fkAlias, revision)
                require(dimensionOption.isDefined, s"Can not find the dimension for Foreign Key Alias $fkAlias in public fact ${publicFact.name}")
                fkAlias -> dimensionOption.get
            }.toMap

          // Check for duplicate aliases/fields
          // User is allowed to ask same column with different alias names
          val requestedAliasList: IndexedSeq[String] = request.selectFields.map(field => field.alias.getOrElse(field.field))
          val duplicateAliases: StringBuilder = new StringBuilder
          requestedAliasList.diff(requestedAliasList.distinct).distinct.foreach(alias => duplicateAliases.append(alias).append(","))

          require(requestedAliasList.distinct.size == requestedAliasList.size,
            s"Duplicate fields/aliases found: cube=${publicFact.name}, duplicate fields are $duplicateAliases")

          /* For fields
          1. Identify all fact cols, and all remaining cols
          2. Check all remaining cols if they have primary key alias's
          3. If any remaining column has no primary key, fail
          4. For all primary key's identified, check if fact has them, if not, fail
          5. If all primary key's found, add them to set of all requested fields (all fields + all primary key alias's),
             and all requested fact fields (all fact cols + all primary key alias's) and all requested dimensions
          */

          /* For filters
          1. Identify all fact filters, and all remaining filters
          2. Check all remaining filters if they have primary key
          3. If any remaining filter has no primary key, fail
          4. For all primary key's identified, check if fact has them, if not, fail
          5. If all primary key's found, add them to set of all requested fields,  requested fact fields,
             and all requested dimensions
           */

          /* For order by
          1. Check all order by fields are in all requested fields, if not, fail
           */

          /* Generate Best Candidates
          1. Given all requested fact cols and optional schema, generate best candidates
          2. Check all fact cols, there must be at least 1 FactCol
          3. If none found, fail
           */

          /* Generate Dimension Candidates
          1. Given all requested dimensions, generate dimension candidates
           */

          /* Check if it is dim driven
          1. If there's no fact filter or fact ordering, then it is dimDriven
           */

          //for dim driven only, list of primary keys
          val dimDrivenRequestedDimensionPrimaryKeyAliases = new mutable.TreeSet[String]()


          //check required aliases
          publicFact.requiredAliases.foreach {
            alias =>
              require(fieldMap.contains(alias), s"Missing required field: cube=${publicFact.name}, field=$alias")
          }

          //check dependent columns
          allDependentColumns.foreach {
            alias =>
              publicFact.columnsByAliasMap(alias).dependsOnColumns.foreach {
                dependentAlias =>
                  require(fieldMap.contains(dependentAlias),
                    s"Missing dependent column : cube=${publicFact.name}, field=$alias, depensOnColumn=$dependentAlias")
              }
          }


          val requestedDimAliasesToPublicDimMap: Map[String, PublicDimension] =
            requestedAliasList
            .filter(reqCol => registry.getDimColIdentity(reqCol).isDefined && registry.dimMap.contains(registry.getDimColIdentity(reqCol).get.publcDimName, publicFact.dimRevision))
            .map(reqCol => reqCol -> registry.dimMap(registry.getDimColIdentity(reqCol).get.publcDimName, publicFact.dimRevision))
            .toMap

          val colsWithRestrictedSchema: IndexedSeq[String] = requestedAliasList.collect {
            case reqCol if (publicFact.restrictedSchemasMap.contains(reqCol)
              && !publicFact.restrictedSchemasMap(reqCol)(request.schema))
              => reqCol
            case dimReqCol if (requestedDimAliasesToPublicDimMap.contains(dimReqCol)
              && requestedDimAliasesToPublicDimMap(dimReqCol).restrictedSchemasMap.contains(dimReqCol)
              && !requestedDimAliasesToPublicDimMap(dimReqCol).restrictedSchemasMap(dimReqCol)(request.schema)
              )
            => dimReqCol
          }
          require(colsWithRestrictedSchema.isEmpty, RestrictedSchemaError(colsWithRestrictedSchema, request.schema.entryName, publicFact.name))

          // separate into fact cols and all remaining non fact cols
          allRequestedAliases.foreach { field =>
            if(publicFact.columnsByAlias(field)) {
              if(publicFact.foreignKeyAliases(field) && !isAsyncFactDrivenQuery) {
                dimDrivenRequestedDimensionPrimaryKeyAliases += field
                allRequestedNonFactAliases += field
              }
              allRequestedFactAliases += field
            } else {
              allRequestedNonFactAliases += field
            }
          }

          // get all primary key aliases for non fact cols
          allRequestedNonFactAliases.foreach { field =>
            if(registry.isPrimaryKeyAlias(field) && publicFact.columnsByAlias(field)) {
              //it's a key column we already have, no op
            } else {
              val primaryKeyAliasOption = registry.getPrimaryKeyAlias(publicFact.name, revision, field)
              require(primaryKeyAliasOption.isDefined, UnknownFieldNameError(field))
              require(publicFact.columnsByAlias(primaryKeyAliasOption.get),
                NoRelationWithPrimaryKeyError(request.cube, primaryKeyAliasOption.get, Option(field)))
              allRequestedAliases += primaryKeyAliasOption.get
              allRequestedFactAliases += primaryKeyAliasOption.get
              allRequestedFactJoinAliases += primaryKeyAliasOption.get
              allRequestedDimensionPrimaryKeyAliases += primaryKeyAliasOption.get
            }
          }

          publicFact.incompatibleColumns.foreach {
            case (alias, incompatibleColumns) =>
              require(!(allRequestedFactAliases.contains(alias)  && incompatibleColumns.intersect(allRequestedAliases).nonEmpty),
                InCompatibleColumnError(alias, incompatibleColumns))
          }

          //keep map from alias to filter for final map back to Set[Filter]
          val filterMap = new mutable.HashMap[String, Filter]()
          val pushDownFilterMap = new mutable.HashMap[String, PushDownFilter]()
          val allFilterAliases = new mutable.TreeSet[String]()
          val allOrFilters = new mutable.TreeSet[Filter]()
          val allFactFilters = new mutable.TreeSet[Filter]()
          val allNonFactFilterAliases = new mutable.TreeSet[String]()
          val allOuterFilters = mutable.TreeSet[Filter]()
          val allOrFilterMeta = mutable.Set[OrFilterMeta]()

          // populate all filters into allFilterAliases
          request.filterExpressions.foreach { filter =>
            if (filter.isInstanceOf[OuterFilter]) {
              val outerFilters = filter.asInstanceOf[OuterFilter].filters.to[mutable.TreeSet]
              outerFilters.foreach( of => require(allRequestedAliases.contains(of.field) == true, s"OuterFilter ${of.field} is not in selected column list"))
              allOuterFilters ++= outerFilters
            } else if (filter.isInstanceOf[OrFilter]) {
              /**
                * Split the current Filters into a series of OrFilters,
                * categorized by Column type which then get rendered separately.
                */

              val orFilter = filter.asInstanceOf[OrFilter]
              val mapForOrFilterSplitting: mutable.HashMap[MetaType.Value, mutable.SortedSet[Filter]] = mutable.HashMap[MetaType.Value, mutable.SortedSet[Filter]]()
                orFilter.filters.foreach{
                filter =>
                  val containsField = publicFact.columnsByAliasMap.contains(filter.field)
                  if (!containsField) if (mapForOrFilterSplitting.contains(MetaType.DimType)) mapForOrFilterSplitting(MetaType.DimType) += filter else mapForOrFilterSplitting.put(MetaType.DimType, mutable.SortedSet(filter))
                  else {
                    val isPubFactCol = publicFact.columnsByAliasMap(filter.field).isInstanceOf[PublicFactCol]
                    if(!isPubFactCol) if(mapForOrFilterSplitting.contains(MetaType.FactType)) mapForOrFilterSplitting(MetaType.FactType) += filter else mapForOrFilterSplitting.put(MetaType.FactType, mutable.SortedSet(filter))
                    else if(mapForOrFilterSplitting.contains(MetaType.MetricType)) mapForOrFilterSplitting(MetaType.MetricType) += filter else mapForOrFilterSplitting.put(MetaType.MetricType, mutable.SortedSet(filter))
                  }
              }

              allOrFilterMeta ++= mapForOrFilterSplitting.map(typeAndFilters => OrFilterMeta(OrFilter(typeAndFilters._2.toList), typeAndFilters._1))
              //require(orFilterMap.size == 1, s"Or filter cannot have combination of fact and dim filters, factFilters=${orFilterMap.get(true)} dimFilters=${orFilterMap.get(false)}")
              //allOrFilterMeta ++= brokenOrFilters.map(meta => OrFilterMeta(meta._2, meta._1))

              for(filter <- orFilter.filters) {
                allFilterAliases += filter.field
                allOrFilters += filter
                val attemptedReverseMappedFilter = tryCreateReverseMappedFilter(filter, publicFact)
                filterMap.put(filter.field, attemptedReverseMappedFilter)
              }
            }
            else {
              allFilterAliases+=filter.field
              val attemptedReverseMappedFilter = tryCreateReverseMappedFilter(filter, publicFact)
              filterMap.put(filter.field, attemptedReverseMappedFilter)
            }
          }

          //check required filter aliases
          publicFact.requiredFilterAliases.foreach {
            alias =>
              require(filterMap.contains(alias), s"Missing required filter: cube=${publicFact.name}, field=$alias")
          }

          // populate all forced filters from fact
          publicFact.forcedFilters.foreach { filter =>
            if(!allFilterAliases(filter.field)) {
              allFilterAliases += filter.field
              filterMap.put(filter.field, filter)
            }
          }

          //list of fk filters
          val filterPostProcess = new mutable.TreeSet[String]
          // separate into fact filters and all remaining non fact filters, except fact filters which are foreign keys
          allFilterAliases.foreach { filter =>
            if(publicFact.columnsByAlias(filter)) {
              if(publicFact.foreignKeyAliases(filter)) {
                //we want to process these after all non foreign keys have been processed
                filterPostProcess += filter
              }
              allFactFilters += filterMap(filter)
            } else {
              allNonFactFilterAliases += filter
            }
          }

          val allFactSortBy = new mutable.HashMap[String, Order]
          val allDimSortBy = new mutable.HashMap[String, Order]

          var orderingPostProcess = List.empty[SortBy]
          //process all non foreign key / primary key sort by's
          request.sortBy.foreach {
            ordering =>
              val primaryKeyAlias = registry.getPrimaryKeyAlias(publicFact.name, ordering.field)
              if(primaryKeyAlias.isDefined) {
                allDimSortBy.put(ordering.field, ordering.order)
              } else {
                require(publicFact.columnsByAlias(ordering.field), s"Failed to determine dim or fact source for ordering by ${ordering.field}")
                if(publicFact.foreignKeyAliases(ordering.field)) {
                  //skip as we want to process these after all non foreign keys have been processed
                  orderingPostProcess ::= ordering
                } else {
                  allFactSortBy.put(ordering.field, ordering.order)
                }
              }
          }

          //primary key alias in the allNonFactFilterAliases should never occur unless does not exist in public fact
          allNonFactFilterAliases.foreach { filter =>
            if(registry.isPrimaryKeyAlias(filter)) {
              require(publicFact.columnsByAlias(filter),
                NoRelationWithPrimaryKeyError(request.cube, filter))
            } else {
              val primaryKeyAliasOption = registry.getPrimaryKeyAlias(publicFact.name, revision, filter)
              require(primaryKeyAliasOption.isDefined, UnknownFieldNameError(filter))
              require(publicFact.columnsByAlias(primaryKeyAliasOption.get),
                NoRelationWithPrimaryKeyError(request.cube, primaryKeyAliasOption.get, Option(filter)))
              allRequestedAliases += primaryKeyAliasOption.get
              allRequestedFactAliases += primaryKeyAliasOption.get
              allRequestedFactJoinAliases += primaryKeyAliasOption.get
              allRequestedDimensionPrimaryKeyAliases += primaryKeyAliasOption.get
            }
          }

          // ordering fields must be in requested fields
          request.sortBy.foreach {
            ordering => require(fieldMap.contains(ordering.field), s"Ordering fields must be in requested fields : ${ordering.field}")
          }

          //if all fact aliases and fact filters are all pk aliases, then it must be dim only query
          if(allRequestedFactAliases.forall(a => allRequestedNonFactAliases(a) || registry.isPrimaryKeyAlias(a)) && allFactFilters.forall(f => registry.isPrimaryKeyAlias(f.field))) {
            //clear fact aliases
            allRequestedFactAliases.clear()
            allRequestedFactJoinAliases.clear()
            //clear fact filters
            allFactFilters.clear()
          }

          val bestCandidatesOption: Option[BestCandidates] = if(allRequestedFactAliases.nonEmpty || allFactFilters.nonEmpty) {
            for {
              bestCandidates <- publicFact.getCandidatesFor(request.schema, request.requestType, allRequestedFactAliases.toSet, allRequestedFactJoinAliases.toSet, allFactFilters.map(f => f.field -> f.operator).toMap, requestedDaysWindow, requestedDaysLookBack, localTimeDayFilter)
            } yield bestCandidates
          } else None

          //if there are no fact cols or filters, we don't need best candidate, otherwise we do
          require((allRequestedFactAliases.isEmpty && allFactFilters.isEmpty)
            || (bestCandidatesOption.isDefined && bestCandidatesOption.get.facts.nonEmpty) , s"No fact best candidates found for request, fact cols : $allRequestedAliases, fact filters : ${allFactFilters.map(_.field)}")

          //val bestCandidates = bestCandidatesOption.get

          //keep entitySet for cost estimation for schema specific entities
          val factToEntitySetMap : mutable.Map[(String, Engine), Set[String]] = new mutable.HashMap
          val entityPublicDimSet = mutable.TreeSet[PublicDimension]()

          val factSchemaRequiredAliasesMap = new mutable.HashMap[String, Set[String]]
          bestCandidatesOption.foreach(_.facts.values.foreach {
            factCandidate =>
              val fact = factCandidate.fact
              //check schema required aliases for facts
              val schemaRequiredFilterAliases = registry.getSchemaRequiredFilterAliasesForFact(fact.name, request.schema, publicFact.name)
              val entitySet = schemaRequiredFilterAliases.map(f => registry.getDimensionByPrimaryKeyAlias(f, Option.apply(publicFact.dimRevision))).flatten.map {
                publicDim =>
                  entityPublicDimSet += publicDim
                  publicDim.name
              }

              val missingFields = schemaRequiredFilterAliases.filterNot(allFilterAliases.apply)
              require(missingFields.isEmpty,
                s"required filter for cube=${publicFact.name}, schema=${request.schema}, fact=${fact.name} not found = $missingFields , found = $allFilterAliases")
              factToEntitySetMap.put(fact.name -> fact.engine, entitySet)
              factSchemaRequiredAliasesMap.put(fact.name, schemaRequiredFilterAliases)
          })

          val timezone = if(bestCandidatesOption.isDefined && bestCandidatesOption.get.publicFact.enableUTCTimeConversion) {
            utcTimeProvider.getTimezone(request)
          } else None

          val (utcTimeDayFilter, utcTimeHourFilter, utcTimeMinuteFilter) = utcTimeProvider.getUTCDayHourMinuteFilter(localTimeDayFilter, localTimeHourFilter, localTimeMinuteFilter, timezone, isDebugEnabled)

          //set fact flags
          //we don't count fk filters here
          val hasNonFKFactFilters = allFactFilters.filterNot(f => filterPostProcess(f.field)).nonEmpty
          val hasFactFilters = allFactFilters.nonEmpty
          val hasMetricFilters = if(bestCandidatesOption.isDefined) {
            val bestCandidates = bestCandidatesOption.get
            val publicFact = bestCandidates.publicFact
            allFactFilters.exists(f =>  publicFact.columnsByAliasMap.contains(f.field) && publicFact.columnsByAliasMap(f.field).isInstanceOf[PublicFactColumn])
          } else false

          //we have to post process since the order of the sort by item could impact if conditions
          //let's add fact sort by's first
          orderingPostProcess.foreach {
            ordering =>
              //if we are fact driven, add to fact sort by else add to dim sort by
              if (allFactSortBy.nonEmpty || (isAsyncFactDrivenQuery && allRequestedDimensionPrimaryKeyAliases.isEmpty)) {
                allFactSortBy.put(ordering.field, ordering.order)
              } else {
                if(allRequestedDimensionPrimaryKeyAliases.contains(ordering.field)
                  || (request.requestType == SyncRequest && !request.forceFactDriven)
                  || (request.requestType == AsyncRequest && request.forceDimensionDriven)) {
                  allDimSortBy.put(ordering.field, ordering.order)
                } else if(allRequestedFactAliases.contains(ordering.field)) {
                  allFactSortBy.put(ordering.field, ordering.order)
                } else {
                  throw new IllegalArgumentException(s"Cannot determine if key is fact or dim for ordering : $ordering")
                }
              }
          }

          val hasFactSortBy = allFactSortBy.nonEmpty
          val isFactDriven: Boolean = {
            val primaryCheck: Boolean =
              (isAsyncFactDrivenQuery
                || isSyncFactDrivenQuery
                || (!request.forceDimensionDriven &&
                ((hasFactFilters && !allFactFilters.forall(f => bestCandidatesOption.get.dimColAliases(f.field)))
                  || hasFactSortBy)))

            val secondaryCheck: Boolean =
              !request.forceDimensionDriven && allDimSortBy.isEmpty && allRequestedDimensionPrimaryKeyAliases.isEmpty && allNonFactFilterAliases.isEmpty
            primaryCheck || secondaryCheck
          }

          //validate filter operation on fact filter field
          allFactFilters.foreach {
            filter =>
              val pubCol = publicFact.columnsByAliasMap(filter.field)
              require(pubCol.filters.contains(filter.operator),
                s"Unsupported filter operation : cube=${publicFact.name}, col=${filter.field}, operation=${filter.operator}")
              filter match {
                //For multiFieldForcedFilter, compare both column types & check filter list on compareTo.
                case multiFieldFilter: MultiFieldForcedFilter =>
                  require(publicFact.columnsByAliasMap.contains(multiFieldFilter.compareTo), IncomparableColumnError(multiFieldFilter.field, multiFieldFilter.compareTo))
                  val secondCol = publicFact.columnsByAliasMap(multiFieldFilter.compareTo)
                  require(secondCol.filters.contains(multiFieldFilter.operator),
                    s"Unsupported filter operation : cube=${publicFact.name}, col=${multiFieldFilter.compareTo}, operation=${multiFieldFilter.operator}")
                  validateFieldsInMultiFieldForcedFilter(publicFact, multiFieldFilter)
                //For field, value filters check length of the value.
                case _ =>
                  val (isValidFilter, length) = validateLengthForFilterValue(publicFact, filter)
                  require(isValidFilter, s"Value for ${filter.field} exceeds max length of $length characters.")
              }
          }

          //if we are dim driven, add primary key of highest level dim
          if(dimDrivenRequestedDimensionPrimaryKeyAliases.nonEmpty && !isFactDriven) {
            val dimDrivenHighestLevelDim =
              dimDrivenRequestedDimensionPrimaryKeyAliases
                .map(pk => registry.getDimensionByPrimaryKeyAlias(pk, Option.apply(publicFact.dimRevision)).get) //we can do .get since we already checked above
                .to[SortedSet]
                .lastKey

            val addDim = {
              if(allRequestedDimensionPrimaryKeyAliases.nonEmpty) {
                val requestedDims = allRequestedDimensionPrimaryKeyAliases
                  .map(pk => registry.getDimensionByPrimaryKeyAlias(pk, Option.apply(publicFact.dimRevision)).get)
                  .to[SortedSet]
                val allRequestedPKAlreadyExist =
                  dimDrivenRequestedDimensionPrimaryKeyAliases.forall(pk => requestedDims.exists(_.columnsByAlias(pk)))
                !allRequestedPKAlreadyExist
              } else {
                true
              }
            }
            if(addDim) {
              allRequestedDimensionPrimaryKeyAliases += dimDrivenHighestLevelDim.primaryKeyByAlias
              dimDrivenRequestedDimensionPrimaryKeyAliases.foreach { pk =>
                if(dimDrivenHighestLevelDim.columnsByAlias(pk) || dimDrivenHighestLevelDim.primaryKeyByAlias == pk) {
                  //do nothing, we've got this pk covered
                } else {
                  //uncovered pk, we need to do join
                  allRequestedDimensionPrimaryKeyAliases += pk
                }
              }
            }
          }
          val finalAllRequestedDimensionPrimaryKeyAliases = allRequestedDimensionPrimaryKeyAliases.toSet

          val finalAllRequestedDimsMap = finalAllRequestedDimensionPrimaryKeyAliases
            .map(pk => pk -> registry.getDimensionByPrimaryKeyAlias(pk, Option.apply(publicFact.dimRevision)).get).toMap

          //produce dim candidates
          val allRequestedDimAliases = new mutable.TreeSet[String]()
          var dimOrder : Int = 0
          val dimensionCandidates: SortedSet[DimensionCandidate] = {
            val intermediateCandidates = new mutable.TreeSet[DimensionCandidate]()
            val upperJoinCandidates = new mutable.TreeSet[PublicDimension]()
            finalAllRequestedDimensionPrimaryKeyAliases
              .flatMap(f => registry.getDimensionByPrimaryKeyAlias(f, Option.apply(publicFact.dimRevision)))
              .toIndexedSeq
              .sortWith((a, b) => b.dimLevel < a.dimLevel)
              .foreach {
                publicDimOption =>
                  //used to identify the highest level dimension
                  dimOrder += 1
                  // publicDimOption should always be defined for primary key alias because it is checked above
                  val publicDim = publicDimOption
                  val colAliases = publicDim.columnsByAlias
                  val isDrivingDimension : Boolean = dimOrder == 1

                  val filters = new mutable.TreeSet[Filter]()
                  //all non foreign key based filters
                  val hasNonFKFilters =  allNonFactFilterAliases.foldLeft(false) {
                    (b, filter) =>
                      val result = if (colAliases.contains(filter) || filter == publicDim.primaryKeyByAlias) {
                        filters += filterMap(filter)
                        true
                      } else false
                      b || result
                  }

                  // populate all forced filters from dim
                  val hasForcedFilters = publicDim.forcedFilters.foldLeft(false) {
                    (b, filter) =>
                      val result = if(!allNonFactFilterAliases(filter.field) && !filterPostProcess(filter.field)) {
                        filters += filter
                        true
                      } else false
                      b || result
                  }

                  val fields = allRequestedNonFactAliases.filter {
                    fd =>
                      (dimOrder == 1 && colAliases.contains(fd)) ||
                        (colAliases.contains(fd) && !(allRequestedFactAliases(fd) && !allNonFactFilterAliases(fd) && !allDimSortBy.contains(fd)))
                  }.toSet

                  if(fields.nonEmpty || filters.nonEmpty || !isFactDriven) {
                    //push down all key based filters
                    filterPostProcess.foreach {
                      filter =>
                        if (colAliases(filter) || publicDim.primaryKeyByAlias == filter) {
                          if(pushDownFilterMap.contains(filter)) {
                            filters += pushDownFilterMap(filter)
                          } else {
                            val pushDownFilter = PushDownFilter(filterMap(filter))
                            pushDownFilterMap.put(filter, pushDownFilter)
                            filters += pushDownFilter
                          }
                        }
                    }

                    //validate filter operation on dim filters
                    filters.foreach {
                      filter =>
                        val pubCol = publicDim.columnsByAliasMap(filter.field)
                        require(pubCol.filters.contains(filter.operator),
                          s"Unsupported filter operation : dimension=${publicDim.name}, col=${filter.field}, operation=${filter.operator}, expected=${pubCol.filters}")
                        filter match {
                          //For multiFieldForcedFilter, compare both column types & check filter list on compareTo.
                          case multiFieldFilter: MultiFieldForcedFilter =>
                            require(publicDim.columnsByAliasMap.contains(multiFieldFilter.compareTo), IncomparableColumnError(multiFieldFilter.field, multiFieldFilter.compareTo))
                            val secondCol = publicDim.columnsByAliasMap(multiFieldFilter.compareTo)
                            require(secondCol.filters.contains(multiFieldFilter.operator),
                              s"Unsupported filter operation : cube=${publicDim.name}, col=${multiFieldFilter.compareTo}, operation=${multiFieldFilter.operator}")
                            validateFieldsInMultiFieldForcedFilter(publicDim, multiFieldFilter)
                          //For field, value filters check length of the value.
                          case _ =>
                            val (isValidFilter, length) = validateLengthForFilterValue(publicDim, filter)
                            require(isValidFilter, s"Value for ${filter.field} exceeds max length of $length characters.")
                        }
                    }

                    val hasNonFKSortBy = allDimSortBy.exists {
                      case (sortField, _) =>
                        publicDim.allColumnsByAlias.contains(sortField) && !publicDim.foreignKeyByAlias(sortField)
                    }
                    val hasNonFKNonPKSortBy = allDimSortBy.exists {
                      case (sortField, _) =>
                        publicDim.allColumnsByAlias.contains(sortField) && !publicDim.foreignKeyByAlias(sortField) && !publicDim.primaryKeyByAlias.equals(sortField)
                    }

                    //keep only one level higher
                    val aboveLevel = publicDim.dimLevel + 1
                    val prevLevel  = publicDim.dimLevel - 1

                    val (foreignkeyAlias: Set[String], lowerJoinCandidates: List[PublicDimension]) = {
                      if (finalAllRequestedDimsMap.size > 1) {
                        val foreignkeyAlias = new mutable.TreeSet[String]
                        val lowerJoinCandidates = new mutable.TreeSet[PublicDimension]
                        publicDim.foreignKeyByAlias.foreach {
                          alias =>
                            if (finalAllRequestedDimsMap.contains(alias)) {
                              foreignkeyAlias += alias
                              val pd = finalAllRequestedDimsMap(alias)
                              //only keep lower join candidates
                              if(pd.dimLevel != publicDim.dimLevel && pd.dimLevel <= prevLevel) {
                                lowerJoinCandidates += finalAllRequestedDimsMap(alias)
                              }
                            }
                        }
                        (foreignkeyAlias.toSet, lowerJoinCandidates.toList)
                      } else {
                        (Set.empty[String], List.empty[PublicDimension])
                      }
                    }


                    // always include primary key in dimension table for join
                    val requestedDimAliases = foreignkeyAlias ++ fields + publicDim.primaryKeyByAlias
                    val filteredUpper = upperJoinCandidates.filter(pd => pd.dimLevel != publicDim.dimLevel && pd.dimLevel >= aboveLevel)

                    // attempting to find the better upper candidate if exist
                    // ads->adgroup->campaign hierarchy, better upper candidate for campaign is ad
                    val filteredUpperTopList = {
                      val bestUpperCandidates = filteredUpper
                        .filter(pd => pd.foreignKeyByAlias.contains(publicDim.primaryKeyByAlias))
                      val bestUpperDerivedCandidate = bestUpperCandidates.find(pd => pd.getBaseDim.isDerivedDimension)
                      val bestUpperCandidate = if (bestUpperDerivedCandidate.isDefined) {
                        Set(bestUpperDerivedCandidate.get)
                      } else {
                        bestUpperCandidates.take(1)
                      }
                      if(bestUpperCandidate.isEmpty && upperJoinCandidates.nonEmpty &&
                        ((!publicFact.foreignKeyAliases(publicDim.primaryKeyByAlias) && isFactDriven) || !isFactDriven)) {
                        //inject upper candidates
                        val upper = upperJoinCandidates.last
                        val findDimensionPath = registry.findDimensionPath(publicDim, upper)
                        findDimensionPath.foreach {
                          injectDim =>
                            val injectFilters : SortedSet[Filter] = pushDownFilterMap.collect {
                              case (alias, filter) if injectDim.columnsByAlias.contains(alias) => filter.asInstanceOf[Filter]
                            }.to[SortedSet]
                            val injectFilteredUpper = upperJoinCandidates.filter(pd => pd.dimLevel != injectDim.dimLevel && pd.dimLevel >= aboveLevel)
                            val injectBestUpperCandidate = injectFilteredUpper
                              .filter(pd => pd.foreignKeyByAlias.contains(injectDim.primaryKeyByAlias)).takeRight(1)
                            val hasLowCardinalityFilter = injectFilters.view.filter(!_.isPushDown).exists {
                              filter =>
                                (colAliases(filter.field) || injectDim.columnsByAlias(filter.field)) &&
                                  !publicFact.columnsByAlias(filter.field) &&
                                  !(publicDim.containsHighCardinalityFilter(filter) || injectDim.containsHighCardinalityFilter(filter))
                            }
                            intermediateCandidates += new DimensionCandidate(
                              injectDim
                              , Set(injectDim.primaryKeyByAlias, publicDim.primaryKeyByAlias)
                              , injectFilters
                              , injectBestUpperCandidate.toList
                              , List(publicDim)
                              , false
                              , hasNonFKFilters || hasForcedFilters
                              , hasNonFKFilters // this does not include force Filters
                              , hasNonFKSortBy
                              , hasNonFKNonPKSortBy
                              , hasLowCardinalityFilter
                              , hasPKRequested = allProjectedAliases.contains(publicDim.primaryKeyByAlias)
                              , hasNonPushDownFilters = injectFilters.exists(filter => !filter.isPushDown)
                            )

                        }
                        val newFilteredUpper = findDimensionPath.filter(pd => pd.dimLevel != publicDim.dimLevel && pd.dimLevel >= aboveLevel)
                        newFilteredUpper.filter(pd => pd.foreignKeyByAlias.contains(publicDim.primaryKeyByAlias)).takeRight(1)
                      } else {
                        bestUpperCandidate
                      }
                    }

                    val filteredLowerTopList = lowerJoinCandidates.lastOption.fold(List.empty[PublicDimension])(List(_))
                    val hasLowCardinalityFilter = filters.view.filter(!_.isPushDown).exists {
                      filter => colAliases(filter.field) && !publicFact.columnsByAlias(filter.field) && !publicDim.containsHighCardinalityFilter(filter)
                    }

                    intermediateCandidates += new DimensionCandidate(
                      publicDim
                      , foreignkeyAlias ++ fields + publicDim.primaryKeyByAlias
                      , filters.to[SortedSet]
                      , filteredUpperTopList.toList
                      , filteredLowerTopList
                      , isDrivingDimension
                      , hasNonFKFilters || hasForcedFilters
                      , hasNonFKFilters // this does not include force Filters
                      , hasNonFKSortBy
                      , hasNonFKNonPKSortBy
                      , hasLowCardinalityFilter
                      , hasPKRequested = allProjectedAliases.contains(publicDim.primaryKeyByAlias)
                      , hasNonPushDownFilters = filters.exists(filter => !filter.isPushDown)
                    )
                    allRequestedDimAliases ++= requestedDimAliases
                    // Adding current dimension to uppper dimension candidates
                    upperJoinCandidates+=publicDim
                  }
              }
            intermediateCandidates.to[SortedSet]
          }

          /*UNUSED Feature
          //if we are dim driven, and we have no ordering, and we only have a single primary key alias in request fields
          //add default ordering by that primary key alias
          if(!isFactDriven && allDimSortBy.isEmpty && request.ordering.isEmpty) {
            val primaryKeyAliasesRequested = allRequestedDimensionPrimaryKeyAliases.filter(allRequestedAliases.apply)
            if(primaryKeyAliasesRequested.size == 1) {
              allDimSortBy.put(primaryKeyAliasesRequested.head, ASC)
            }
          }
          */

          //we don't count fk filters here
          val hasNonFKDimFilters = allNonFactFilterAliases.filterNot(filterPostProcess(_)).nonEmpty
          val hasDimFilters = allNonFactFilterAliases.nonEmpty
          val hasDimSortBy = allDimSortBy.nonEmpty
          val hasNonDrivingDimSortOrFilter = dimensionCandidates.exists(dc => !dc.isDrivingDimension && (dc.hasNonFKOrForcedFilters || dc.hasNonFKSortBy))
          val hasDrivingDimNonFKNonPKSortBy = dimensionCandidates.filter(dim => dim.isDrivingDimension && dim.hasNonFKNonPKSortBy).nonEmpty

          val hasNonDrivingDimNonFKNonPKFilter = dimensionCandidates.filter(dim => !dim.isDrivingDimension && dim.hasNonFKOrForcedFilters).nonEmpty

          val anyDimHasNonFKNonForceFilter = dimensionCandidates.exists(dim=> dim.hasNonFKNonForceFilters)

          //dimensionCandidates.filter(dim=> dim.isDrivingDimension && !dim.filters.intersect(filterMap.values.toSet).isEmpty


          val finalAllRequestedCols = {
            request.selectFields.map {
              case fd if allRequestedFactAliases(fd.field) &&
                ((isFactDriven && request.requestType == AsyncRequest)
                  || (!allRequestedNonFactAliases(fd.field) && !dimDrivenRequestedDimensionPrimaryKeyAliases(fd.field))) =>
                FactColumnInfo(fd.field)
              case fd if constantValueFields(fd.field) =>
                ConstantColumnInfo(fd.alias.getOrElse(fd.field), fd.value.get)
              case fd if allRequestedDimAliases(fd.field)=>
                DimColumnInfo(fd.field)
              case fd =>
                FactColumnInfo(fd.field)
            }
          }

          val isRequestingDistict = {
            val distinctValue =  request.additionalParameters.get(Distinct)
            if(distinctValue.isDefined) {
              if(distinctValue.get == DistinctValue(true)) {
                true
              } else false
            } else false
          }

          val finalAllSortByCols = {
            request.sortBy.map {
              case od if allFactSortBy.contains(od.field) =>
                FactSortByColumnInfo(od.field, od.order)
              case od if allDimSortBy.contains(od.field) =>
                DimSortByColumnInfo(od.field, od.order)
              case od =>
                throw new IllegalStateException(s"Failed to identify source for ordering col : $od")
            }
          }


          val includeRowCount = request.includeRowCount || (request.requestType == SyncRequest && request.paginationStartIndex < 0)

          val hasLowCardinalityDimFilters = dimensionCandidates.exists(_.hasLowCardinalityFilter)

          val dimensionRelations: DimensionRelations = {
            val publicDimsInRequest = dimensionCandidates.map(_.dim.name)
            var relations: Map[(String, String), Boolean] = Map.empty
            val seq = dimensionCandidates.toIndexedSeq

            var i = 0
            while(i < seq.size) {
              var j = i + 1
              while(j < seq.size) {
                val a = seq(i)
                val b = seq(j)
                val nonDirectRelations = registry.findDimensionPath(a.dim, b.dim)
                val allIndirectRelationsInRequest = nonDirectRelations.forall(pd => publicDimsInRequest(pd.name))
                val related = (a.dim.foreignKeySources.contains(b.dim.name)
                  || b.dim.foreignKeySources.contains(a.dim.name)
                  || (nonDirectRelations.nonEmpty && allIndirectRelationsInRequest)
                  )
                relations += ((a.dim.name, b.dim.name) -> related)
                j+=1
              }
              i+=1
            }
            DimensionRelations(relations)
          }

          val allFactWithoutOr = allFactFilters.filterNot(filter => allOrFilters.contains(filter))
          new RequestModel(request.cube, bestCandidatesOption, allFactWithoutOr.to[SortedSet], dimensionCandidates,
            finalAllRequestedCols, finalAllSortByCols, allRequestedNonFactAliases.toSet,
            registry.getDimCardinalityEstimate(dimensionCandidates, request, entityPublicDimSet.toSet, filterMap,isDebugEnabled),
            bestCandidatesOption.map(
              _.facts.values
                .map(f => (f.fact.name, f.fact.engine) -> registry.getFactRowsCostEstimate(dimensionCandidates,f, request, entityPublicDimSet.toSet, filterMap, isDebugEnabled)).toMap
            ).getOrElse(Map.empty),
            factSortByMap = allFactSortBy.toMap,
            dimSortByMap = allDimSortBy.toMap,
            isFactDriven = isFactDriven,
            hasFactFilters = hasFactFilters,
            hasMetricFilters = hasMetricFilters,
            hasNonFKFactFilters = hasNonFKFactFilters,
            hasFactSortBy = hasFactSortBy,
            hasDimFilters = hasDimFilters,
            hasNonFKDimFilters = hasNonFKDimFilters,
            hasDimSortBy = hasDimSortBy,
            forceDimDriven = request.forceDimensionDriven,
            forceFactDriven = request.forceFactDriven,
            hasNonDrivingDimSortOrFilter = hasNonDrivingDimSortOrFilter,
            hasDrivingDimNonFKNonPKSortBy = hasDrivingDimNonFKNonPKSortBy,
            hasNonDrivingDimNonFKNonPKFilter =  hasNonDrivingDimNonFKNonPKFilter,
            anyDimHasNonFKNonForceFilter = anyDimHasNonFKNonForceFilter,
            schema = request.schema,
            requestType = request.requestType,
            localTimeDayFilter = localTimeDayFilter,
            localTimeHourFilter = localTimeHourFilter,
            localTimeMinuteFilter = localTimeMinuteFilter,
            utcTimeDayFilter = utcTimeDayFilter,
            utcTimeHourFilter = utcTimeHourFilter,
            utcTimeMinuteFilter = utcTimeMinuteFilter,
            startIndex = request.paginationStartIndex,
            maxRows = request.rowsPerPage,
            includeRowCount = includeRowCount,
            isDebugEnabled = isDebugEnabled,
            additionalParameters = request.additionalParameters,
            factSchemaRequiredAliasesMap = factSchemaRequiredAliasesMap.toMap,
            reportingRequest = request,
            queryGrain = queryGrain,
            isRequestingDistict = isRequestingDistict,
            hasLowCardinalityDimFilters = hasLowCardinalityDimFilters,
            requestedDaysLookBack = requestedDaysLookBack,
            requestedDaysWindow = requestedDaysWindow,
            outerFilters = allOuterFilters,
            requestedFkAliasToPublicDimensionMap = allRequestedFkAliasToPublicDimMap,
            orFilterMeta = allOrFilterMeta.toSet,
            dimensionRelations = dimensionRelations
           )
      }
    }
  }

  def validateMaxLookBackWindow(localTimeDayFilter:Filter, factName:String, maxDaysWindow:Int, maxDaysLookBack:Int): (Int, Int) = {
    localTimeDayFilter match {
      case BetweenFilter(_,from,to) =>
        val requestedDaysWindow = DailyGrain.getDaysBetween(from, to)
        val requestedDaysLookBack = DailyGrain.getDaysFromNow(from)
        require(requestedDaysWindow <= maxDaysWindow,
          MaxWindowExceededError(maxDaysWindow, DailyGrain.getDaysBetween(from, to), factName))
        require(requestedDaysLookBack <= maxDaysLookBack,
          MaxLookBackExceededError(maxDaysLookBack, DailyGrain.getDaysFromNow(from), factName))
        (requestedDaysWindow, requestedDaysLookBack)

      case InFilter(_,dates, _, _) =>
        val requestedDaysWindow = dates.size
        require(dates.size < maxDaysWindow,
          MaxWindowExceededError(maxDaysWindow, dates.size, factName))
        var maxDiff: Integer = scala.Int.MinValue
        dates.foreach {
          date => {
            val curDiff = DailyGrain.getDaysFromNow(date)
            if(curDiff > maxDiff) maxDiff = curDiff
            require(curDiff <= maxDaysLookBack,
              MaxLookBackExceededError(maxDaysLookBack, curDiff, factName))
          }
        }
        (requestedDaysWindow, maxDiff)

      case EqualityFilter(_,date, _, _) =>
        val requestedDaysLookBack = DailyGrain.getDaysFromNow(date)
        require(requestedDaysLookBack <= maxDaysLookBack,
          MaxLookBackExceededError(maxDaysLookBack, requestedDaysLookBack, factName))
        (1, requestedDaysLookBack)
      case a =>
        throw new IllegalArgumentException(s"Filter operation not supported. Day filter can be between, in, equality filter : $a")
    }
  }

  def validateFieldsInMultiFieldForcedFilter(publicTable: PublicTable, filter: MultiFieldForcedFilter): Unit = {
    publicTable match {
      case publicDim: PublicDimension =>
        val firstDataType: DataType = publicDim.nameToDataTypeMap(publicDim.columnsByAliasMap(filter.field).name)
        val compareToDataType: DataType = publicDim.nameToDataTypeMap(publicDim.columnsByAliasMap(filter.compareTo).name)
        require(firstDataType.jsonDataType == compareToDataType.jsonDataType, "Both fields being compared must be the same Data Type.")
      case publicFact: PublicFact =>
        val firstDataType: DataType = publicFact.dataTypeForAlias(publicFact.columnsByAliasMap(filter.field).alias)
        val compareToDataType: DataType = publicFact.dataTypeForAlias(publicFact.columnsByAliasMap(filter.compareTo).alias)
        require(firstDataType.jsonDataType == compareToDataType.jsonDataType, "Both fields being compared must be the same Data Type.")
      case _ => None
    }
  }

  def validateLengthForFilterValue(publicTable: PublicTable, filter: Filter): (Boolean, Int) = {
    val dataType = {
      publicTable match {
        case publicDim: PublicDimension => publicDim.nameToDataTypeMap(publicDim.columnsByAliasMap(filter.field).name)
        case publicFact: PublicFact => publicFact.dataTypeForAlias(publicFact.columnsByAliasMap(filter.field).alias)
        case _ => None
      }
    }

    def validateLength(values : List[String], maxLength:Int) : (Boolean, Int) = {
      val expectedLength = if (maxLength == 0) MAX_ALLOWED_STR_LEN else maxLength
      if (values.forall(_.length <= expectedLength))
        (true, expectedLength)
      else
        (false, expectedLength)
    }

    dataType match {
      case None => throw new IllegalArgumentException(s"Unable to find expected PublicTable as PublicFact or PublicDimension.")
      case StrType(length, _, _) => filter match {
        case InFilter(_, values, _, _) => validateLength(values, length)
        case NotInFilter(_, values, _, _) => validateLength(values, length)
        case EqualityFilter(_, value, _, _) => validateLength(List(value), length)
        case FieldEqualityFilter(_, value, _, _) => validateLength(List(value), length)
        case NotEqualToFilter(_, value, _, _) => validateLength(List(value), length)
        case LikeFilter(_, value, _, _) => validateLength(List(value), length)
        case BetweenFilter(_, from, to) => validateLength(List(from, to), length)
        case IsNullFilter(_, _, _) | IsNotNullFilter(_, _, _) | PushDownFilter(_) | OuterFilter(_) | OrFilter(_) => (true, MAX_ALLOWED_STR_LEN)
        case _ => throw new Exception(s"Unhandled FilterOperation $filter.")
      }
      case _ => (true, MAX_ALLOWED_STR_LEN)
    }
  }

  /**
    * Attempt to Statically Map the current filter, if applicable.
    * @param filter     - Filter to check against Statically Mapped Columns
    * @param publicFact - Source of Statically Mapped Column checking.
    * @return           - Reverse Statically Mapped filter.
    */
  def tryCreateReverseMappedFilter(filter: Filter
                                   , publicFact: PublicFact): Filter = {
    if (publicFact.aliasToReverseStaticMapping.contains(filter.field)) {
      val reverseMapping = publicFact.aliasToReverseStaticMapping(filter.field)
      filter match {
        case BetweenFilter(field, from, to) =>
          require(reverseMapping.contains(from), s"Unknown filter from value for field=$field, from=$from")
          require(reverseMapping.contains(to), s"Unknown filter to value for field=$field, to=$to")
          val fromSet = reverseMapping(from)
          val toSet = reverseMapping(to)
          require(fromSet.size == 1 && toSet.size == 1,
            s"Cannot perform between filter, the column has static mapping which maps to multiple values, from=$from maps to fromSet=$fromSet, to=$to maps to toSet=$toSet"
          )
          BetweenFilter(field, fromSet.head, toSet.head)
        case EqualityFilter(field, value, _, _) =>
          require(reverseMapping.contains(value), s"Unknown filter value for field=$field, value=$value")
          val valueSet = reverseMapping(value)
          if (valueSet.size > 1) {
            InFilter(field, valueSet.toList)
          } else {
            EqualityFilter(field, valueSet.head)
          }
        case InFilter(field, values, _, _) =>
          val mapped = values.map {
            value =>
              require(reverseMapping.contains(value), s"Unknown filter value for field=$field, value=$value")
              reverseMapping(value)
          }
          InFilter(field, mapped.flatten)
        case NotInFilter(field, values, _, _) =>
          val mapped = values.map {
            value =>
              require(reverseMapping.contains(value), s"Unknown filter value for field=$field, value=$value")
              reverseMapping(value)
          }
          NotInFilter(field, mapped.flatten)
        case NotEqualToFilter(field, value, _, _) =>
          require(reverseMapping.contains(value), s"Unknown filter value for field=$field, value=$value")
          val valueSet = reverseMapping(value)
          if (valueSet.size > 1) {
            NotInFilter(field, valueSet.toList)
          } else {
            NotEqualToFilter(field, valueSet.head)
          }
        case f =>
          throw new IllegalArgumentException(s"Unsupported filter operation on statically mapped field : $f")
      }
    } else filter
  }

}

case class RequestModelResult(model: RequestModel, dryRunModelTry: Option[Try[RequestModel]])

object RequestModelFactory extends Logging {
  // If no revision is specified, return a Tuple of RequestModels 1-To serve the response 2-Optional dryrun to test new fact revisions
  def fromBucketSelector(request: ReportingRequest, bucketParams: BucketParams, registry: Registry, bucketSelector: BucketSelector, utcTimeProvider: UTCTimeProvider = PassThroughUTCTimeProvider) : Try[RequestModelResult] = {
    val selectedBucketsTry: Try[CubeBucketSelected] = bucketSelector.selectBucketsForCube(request.cube, bucketParams)
    selectedBucketsTry match {
      case Success(buckets: CubeBucketSelected) =>
        for {
          defaultRequestModel <- RequestModel.from(request, registry, utcTimeProvider, Some(buckets.revision))
        } yield {
          val dryRunModel: Option[Try[RequestModel]] = if (buckets.dryRunRevision.isDefined) {
            Option(Try {
              val updatedRequest = if (buckets.dryRunEngine.isDefined) {
                buckets.dryRunEngine.get match {
                  case DruidEngine =>
                    ReportingRequest.forceDruid(request)
                  case OracleEngine =>
                    ReportingRequest.forceOracle(request)
                  case HiveEngine =>
                    ReportingRequest.forceHive(request)
                  case PrestoEngine =>
                    ReportingRequest.forcePresto(request)
                  case PostgresEngine =>
                    ReportingRequest.forcePostgres(request)
                  case a =>
                    throw new IllegalArgumentException(s"Unknown engine: $a")
                }
              } else request
              RequestModel.from(updatedRequest, registry, utcTimeProvider, buckets.dryRunRevision)
            }.flatten)
          } else None
          RequestModelResult(defaultRequestModel, dryRunModel)
        }

      case Failure(t) =>
        warn("Failed to compute bucketing info, will use default revision to return response", t)
        RequestModel.from(request, registry, utcTimeProvider).map(model => RequestModelResult(model, None))
    }
  }
}
