// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.druid

import java.util.UUID

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.google.common.collect.{Lists, Maps}
import com.yahoo.maha.core.DruidDerivedFunction._
import com.yahoo.maha.core.DruidPostResultFunction.{START_OF_THE_MONTH, START_OF_THE_WEEK}
import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.request._
import com.yahoo.maha.maha_druid_lookups.query.lookup.{DecodeConfig, MahaRegisteredLookupExtractionFn}
import com.yahoo.maha.query.aggregation.{RoundingDoubleSumAggregatorFactory, RoundingDoubleSumDruidModule}
import grizzled.slf4j.Logging
import io.druid.jackson.DefaultObjectMapper
import io.druid.java.util.common.granularity.{GranularityType, PeriodGranularity}
import io.druid.js.JavaScriptConfig
import io.druid.math.expr.ExprMacroTable
import io.druid.query.aggregation._
import io.druid.query.aggregation.datasketches.theta.{SketchMergeAggregatorFactory, SketchModule}
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory
import io.druid.query.aggregation.post.{ArithmeticPostAggregator, FieldAccessPostAggregator}
import io.druid.query.dimension.{DefaultDimensionSpec, DimensionSpec, ExtractionDimensionSpec}
import io.druid.query.extraction._
import io.druid.query.filter.{AndDimFilter, DimFilter, SelectorDimFilter}
import io.druid.query.groupby.GroupByQuery
import io.druid.query.groupby.GroupByQuery.Builder
import io.druid.query.groupby.having.{AndHavingSpec, HavingSpec}
import io.druid.query.groupby.orderby.{DefaultLimitSpec, NoopLimitSpec, OrderByColumnSpec}
import io.druid.query.lookup.LookupExtractionFn
import io.druid.query.ordering.{StringComparator, StringComparators}
import io.druid.query.select.{PagingSpec, SelectResultValue}
import io.druid.query.spec.{MultipleIntervalSegmentSpec, QuerySegmentSpec}
import io.druid.query.timeseries.TimeseriesResultValue
import io.druid.query.topn.{InvertedTopNMetricSpec, NumericTopNMetricSpec, TopNQueryBuilder, TopNResultValue}
import io.druid.query.{Druids, Result}
import io.druid.segment.column.ValueType
import org.joda.time.{DateTime, DateTimeZone, Interval, Period}
import org.json4s.{DefaultFormats, JValue}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{SortedSet, mutable}
import scala.util.Try

/**
  * Created by hiral on 12/11/15.
  */

object DruidQueryOptimizer {
  val GROUP_BY_IS_SINGLE_THREADED = "groupByIsSingleThreaded"
  val CHUNK_PERIOD = "chunkPeriod"
  val QUERY_PRIORITY = "priority"
  val GROUP_BY_STRATEGY = "groupByStrategy"
  val UNCOVERED_INTERVALS_LIMIT = "uncoveredIntervalsLimit"
  val UNCOVERED_INTERVALS_LIMIT_VALUE = 1.asInstanceOf[AnyRef]
  val APPLY_LIMIT_PUSH_DOWN = "applyLimitPushDown"
  val ASYNC_QUERY_PRIORITY = -1
  val TIMEOUT = "timeout"

}

trait DruidQueryOptimizer {
  def optimize(queryContext: FactQueryContext, context: java.util.Map[String, AnyRef]): Unit
}

class SyncDruidQueryOptimizer(maxSingleThreadedDimCardinality: Long = DruidQueryGenerator.defaultMaxSingleThreadedDimCardinality
                              , maxNoChunkCost: Long = DruidQueryGenerator.defaultMaxNoChunkCost
                              , maxChunks: Int = DruidQueryGenerator.defaultMaxChunks
                              , timeout: Int = DruidQueryGenerator.defaultTimeout
                             ) extends DruidQueryOptimizer {

  import DruidQueryOptimizer._

  private[this] def groupByIsSingleThreaded(dimCardinalityEstimate: Long): Boolean = {
    !(dimCardinalityEstimate > maxSingleThreadedDimCardinality)
  }

  private[this] def chunkPeriod(queryContext: FactQueryContext): Option[String] = {
    if (!queryContext.factBestCandidate.fact.annotations.contains(DruidGroupByStrategyV2)
      && queryContext.factBestCandidate.factCost > maxNoChunkCost) {
      val period: Int = math.ceil((queryContext.requestModel.reportingRequest.numDays + 1D) / maxChunks).toInt
      if (period > 0) {
        Option(s"P${period}D")
      } else {
        None
      }
    } else {
      None
    }
  }

  def optimize(queryContext: FactQueryContext, context: java.util.Map[String, AnyRef]): Unit = {
    val isGroupByQuery = (queryContext.requestModel.reportingRequest.queryType == com.yahoo.maha.core.request.GroupByQuery
      || queryContext.requestModel.reportingRequest.queryType == com.yahoo.maha.core.request.RowCountQuery)
    queryContext.factBestCandidate.fact.annotations.foreach {
      case DruidQueryPriority(priority) =>
        context.put(QUERY_PRIORITY, priority.asInstanceOf[AnyRef])
        if (priority == ASYNC_QUERY_PRIORITY) {
          if (!queryContext.factBestCandidate.isGrainOptimized || !queryContext.factBestCandidate.isIndexOptimized) {
            context.put(GROUP_BY_IS_SINGLE_THREADED, java.lang.Boolean.FALSE.asInstanceOf[AnyRef])
          }
        }
      case DruidGroupByStrategyV2 if isGroupByQuery =>
        context.put(GROUP_BY_STRATEGY, "v2")
      case DruidGroupByStrategyV1 if isGroupByQuery =>
        context.put(GROUP_BY_STRATEGY, "v1")
      case DruidGroupByIsSingleThreaded(isSingleThreaded) if isGroupByQuery =>
        context.put(GROUP_BY_IS_SINGLE_THREADED, isSingleThreaded.asInstanceOf[AnyRef])
        if (!isSingleThreaded) {
          chunkPeriod(queryContext).foreach(p => context.put(CHUNK_PERIOD, p))
        }
      case _ => //do nothing
    }

    if (!context.containsKey(GROUP_BY_IS_SINGLE_THREADED) && isGroupByQuery) {
      val dimCardinalityEstimate: Long = queryContext.requestModel.dimCardinalityEstimate.getOrElse(queryContext.factBestCandidate.fact.defaultCardinality.toLong)
      val groupBySingleThreadedBoolean = groupByIsSingleThreaded(dimCardinalityEstimate)
      context.put(GROUP_BY_IS_SINGLE_THREADED, groupBySingleThreadedBoolean.asInstanceOf[AnyRef])
      if (!groupBySingleThreadedBoolean) {
        chunkPeriod(queryContext).foreach(p => context.put(CHUNK_PERIOD, p))
      }
    }

    context.put(TIMEOUT, timeout.asInstanceOf[AnyRef])
    context.put(UNCOVERED_INTERVALS_LIMIT, UNCOVERED_INTERVALS_LIMIT_VALUE)
    context.put(APPLY_LIMIT_PUSH_DOWN, "false")
  }
}

class AsyncDruidQueryOptimizer(maxSingleThreadedDimCardinality: Long = DruidQueryGenerator.defaultMaxSingleThreadedDimCardinality
                               , maxNoChunkCost: Long = DruidQueryGenerator.defaultMaxNoChunkCost
                               , maxChunks: Int = DruidQueryGenerator.defaultMaxChunks
                               , timeout: Int = DruidQueryGenerator.defaultTimeout
                              ) extends SyncDruidQueryOptimizer(maxSingleThreadedDimCardinality, maxNoChunkCost, maxChunks, timeout) {

  import DruidQueryOptimizer._

  override def optimize(queryContext: FactQueryContext, context: java.util.Map[String, AnyRef]): Unit = {
    super.optimize(queryContext, context)
    queryContext.requestModel.requestType match {
      case AsyncRequest =>
        context.put(QUERY_PRIORITY, ASYNC_QUERY_PRIORITY.asInstanceOf[AnyRef])
      case a =>
      // Use default priority (0)
    }
  }
}

object DruidQueryGenerator extends Logging {
  val defaultMaxSingleThreadedDimCardinality: Long = 40000
  val defaultMaxNoChunkCost: Long = 280000
  val defaultMaxChunks: Int = 3
  val defaultTimeout: Int = 300000 //PT5M
  val defaultMaximumMaxRows: Int = 5000
  val defaultMaximumTopNMaxRows: Int = 400
  val defaultMaximumMaxRowsAsync: Int = 100000

  /*
   useCustomRoundingSumAggregator: RoundingDoubleSumAggregatorFactory used for rounding dec type up to n decimals places
   if you set to true, make sure that you have RoundingDoubleSumDruidModule registered in druid
   */
  def register(queryGeneratorRegistry: QueryGeneratorRegistry
               , queryOptimizer: DruidQueryOptimizer = new SyncDruidQueryOptimizer()
               , defaultDimCardinality: Long = defaultMaxSingleThreadedDimCardinality
               , maximumMaxRows: Int = defaultMaximumMaxRows
               , maximumTopNMaxRows: Int = defaultMaximumTopNMaxRows
               , maximumMaxRowsAsync: Int = defaultMaximumMaxRowsAsync
               , useCustomRoundingSumAggregator: Boolean  = false
              ) = {
    if (!queryGeneratorRegistry.isEngineRegistered(DruidEngine, Option(Version.DEFAULT))) {
      val generator = new DruidQueryGenerator(queryOptimizer, defaultDimCardinality, maximumMaxRows, maximumTopNMaxRows, maximumMaxRowsAsync, useCustomRoundingSumAggregator = useCustomRoundingSumAggregator)
      queryGeneratorRegistry.register(DruidEngine, generator)
    } else {
      queryGeneratorRegistry.getDefaultGenerator(DruidEngine).foreach {
        qg =>
          if (!qg.isInstanceOf[DruidQueryGenerator]) {
            warn(s"Another query generator registered for DruidEngine : ${qg.getClass.getCanonicalName}")
          }
      }

    }
  }
}

object DruidQuery {

  import scala.collection.JavaConverters._

  val mapper = new DefaultObjectMapper()
  mapper.setSerializationInclusion(Include.NON_NULL)
  val sketchModulesList = new SketchModule().getJacksonModules()
  sketchModulesList.asScala.foreach(module => mapper.registerModule(module))
  val roundingDoubleSUmModulesList = new RoundingDoubleSumDruidModule().getJacksonModules()
  roundingDoubleSUmModulesList.asScala.foreach(module => mapper.registerModule(module))

  def toJson(query: io.druid.query.Query[_]): String = {
    mapper.writeValueAsString(query)
  }

  val replaceMissingValueWith = "MAHA_LOOKUP_EMPTY"
}

abstract class DruidQuery[T] extends Query with WithDruidEngine {
  def query: io.druid.query.Query[T]

  def asString: String = DruidQuery.toJson(query)

  def maxRows: Int

  def isPaginated: Boolean

  def queryGenVersion: Option[Version] = None
}

case class TimeseriesDruidQuery(queryContext: QueryContext
                                , aliasColumnMap: Map[String, Column]
                                , query: io.druid.query.Query[Result[TimeseriesResultValue]]
                                , additionalColumns: IndexedSeq[String]
                                , maxRows: Int
                                , isPaginated: Boolean
                               ) extends DruidQuery[Result[TimeseriesResultValue]]

case class TopNDruidQuery(queryContext: QueryContext
                          , aliasColumnMap: Map[String, Column]
                          , query: io.druid.query.Query[Result[TopNResultValue]]
                          , additionalColumns: IndexedSeq[String]
                          , maxRows: Int
                          , isPaginated: Boolean
                         ) extends DruidQuery[Result[TopNResultValue]]

case class GroupByDruidQuery(queryContext: QueryContext
                             , aliasColumnMap: Map[String, Column]
                             , query: io.druid.query.Query[io.druid.data.input.Row]
                             , additionalColumns: IndexedSeq[String]
                             , override val ephemeralAliasColumnMap: Map[String, Column]
                             , maxRows: Int
                             , isPaginated: Boolean
                            ) extends DruidQuery[io.druid.data.input.Row]

case class SelectDruidQuery(queryContext: QueryContext
                            , aliasColumnMap: Map[String, Column]
                            , query: io.druid.query.Query[Result[SelectResultValue]]
                            , additionalColumns: IndexedSeq[String]
                            , maxRows: Int
                            , isPaginated: Boolean
                           ) extends DruidQuery[Result[SelectResultValue]]

class DruidQueryGenerator(queryOptimizer: DruidQueryOptimizer
                          , defaultDimCardinality: Long
                          , maximumMaxRows: Int = DruidQueryGenerator.defaultMaximumMaxRows
                          , maximumTopNMaxRows: Int = DruidQueryGenerator.defaultMaximumTopNMaxRows
                          , maximumMaxRowsAsync: Int = DruidQueryGenerator.defaultMaximumMaxRowsAsync
                          , shouldLimitInnerQueries: Boolean = true
                          , useCustomRoundingSumAggregator : Boolean = false) extends BaseQueryGenerator[WithDruidEngine] with Logging {

  import collection.JavaConverters._

  override val engine: Engine = DruidEngine

  private[this] val defaultMaxRows = 1000
  private[this] val DRUID_REQUEST_ID_CONTEXT = "queryId"
  private[this] val DRUID_USER_ID_CONTEXT = "userId"
  private[this] val MIN_TOPN_THRESHOLD = "minTopNThreshold"
  private[this] val DRUID_HOST_NAME_CONTEXT = "hostName"

  private[this] def findDirection(order: Order): OrderByColumnSpec.Direction = order match {
    case ASC => OrderByColumnSpec.Direction.ASCENDING
    case DESC => OrderByColumnSpec.Direction.DESCENDING
    case any => throw new UnsupportedOperationException(s"Sort order not supported : $any")
  }

  private[this] def findComparator(dataType: DataType): StringComparator = dataType match {
    case IntType(_, _, _, _, _) => StringComparators.NUMERIC
    case DecType(_, _, _, _, _, _) => StringComparators.NUMERIC
    case any => StringComparators.LEXICOGRAPHIC
  }

  override def generate(queryContext: QueryContext): Query = {
    queryContext match {
      case CombinedQueryContext(dims, factBestCandidate, requestModel, queryAttributes) =>
        generateFactQuery(dims, new FactQueryContext(factBestCandidate, requestModel, None, List.empty, queryAttributes))
      case FactQueryContext(factBestCandidate, requestModel, indexAliasOption, factGroupByKeys, queryAttributes, dimsOption) =>
        generateFactQuery(dimsOption.getOrElse(SortedSet.empty[DimensionBundle]).filter(f => f.dim.engine == DruidEngine), new FactQueryContext(factBestCandidate, requestModel, indexAliasOption, factGroupByKeys, queryAttributes))
      case any => throw new UnsupportedOperationException(s"query context not supported : $any")
    }
  }

  override def validateEngineConstraints(requestModel: RequestModel): Boolean = {
    return requestModel.startIndex < maximumMaxRows;
  }

  private[this] def generateFactQuery(dims: SortedSet[DimensionBundle], queryContext: FactQueryContext): Query = {

    val context: java.util.Map[String, AnyRef] = Maps.newHashMap[String, AnyRef]()
    val fact = queryContext.factBestCandidate.fact
    val dataSource = fact.underlyingTableName.getOrElse(fact.name)
    val model = queryContext.requestModel
    val requestIdValue = model.additionalParameters.getOrElse(Parameter.RequestId, RequestIdValue(UUID.randomUUID().toString))
    val requestId = requestIdValue.asInstanceOf[RequestIdValue].value
    context.put(DRUID_REQUEST_ID_CONTEXT, requestId)
    info(s"Druid requestId is set to $requestId")
    val userIdValue = model.additionalParameters.getOrElse(Parameter.UserId, UserIdValue(""))
    val userId = userIdValue.asInstanceOf[UserIdValue].value
    if (!userId.isEmpty) {
      debug(s"Druid userId is set to $userId")
      context.put(DRUID_USER_ID_CONTEXT, userId)
    }
    val hostNameValue = model.additionalParameters.getOrElse(Parameter.HostName, HostNameValue(""))
    val hostName = hostNameValue.asInstanceOf[HostNameValue].value
    if (!hostName.isEmpty) {
      debug(s"Hostname is set to $hostName")
      context.put(DRUID_HOST_NAME_CONTEXT, hostName)
    }
    queryOptimizer.optimize(queryContext, context)
    val dimCardinality = queryContext.requestModel.dimCardinalityEstimate.getOrElse(defaultDimCardinality)

    val isUsingDruidLookup: Boolean = dims.exists(_.dim.engine == DruidEngine)
    val factAliasColumnMap: Map[String, Column] = {
      val nameAliasMap = queryContext.factBestCandidate.dimColMapping ++ queryContext.factBestCandidate.factColMapping
      nameAliasMap.collect {
        case (name, alias) if !isUsingDruidLookup || (isUsingDruidLookup && model.requestColsSet(alias)) => alias -> queryContext.factBestCandidate.fact.columnsByNameMap(name)
      }
    }

    val dimAliasColumnMap: Map[String, Column] = {
      dims.map {
        db =>
          val aliasSet = db.fields.filterNot(f => f.equals(db.publicDim.primaryKeyByAlias) || db.publicDim.foreignKeyByAlias(f))
          aliasSet.map {
            alias =>
              val name = db.publicDim.aliasToNameMap(alias)
              val column = db.dim.dimensionColumnsByNameMap(name)
              alias -> column
          }
      }
    }.flatten.toMap

    val aliasColumnMap = factAliasColumnMap ++ dimAliasColumnMap

    val attemptToRenderSortByNames = {
      val sortByColAliases = queryContext.factBestCandidate.requestModel.requestSortByCols
      val intermediary = sortByColAliases.map(colInfo => colInfo.alias)
      intermediary.collect { case name if aliasColumnMap contains name => aliasColumnMap(name).name }
    }

    val factRequestCols: Set[String] = {
      if (queryContext.requestModel.dimensionsCandidates.nonEmpty && (queryContext.requestModel.forceDimDriven || queryContext.requestModel.isDimDriven)) {
        val joinCols: Set[String] = {
          val lastDimPrimaryKey = queryContext.factBestCandidate.publicFact.aliasToNameColumnMap.get(queryContext.requestModel.dimensionsCandidates.last.dim.primaryKeyByAlias)
          lastDimPrimaryKey.fold(queryContext.factBestCandidate.requestJoinCols) { pkey =>
            queryContext.factBestCandidate.requestJoinCols - pkey
          }
        }
        queryContext.factBestCandidate.requestCols.filterNot(joinCols.diff(attemptToRenderSortByNames.toSet))
      } else {
        queryContext.factBestCandidate.requestCols
      }
    }
    val maxRows = if (queryContext.requestModel.isAsyncRequest && queryContext.requestModel.maxRows <= 0) {
      maximumMaxRowsAsync
    } else if (queryContext.requestModel.maxRows < 1) {
      defaultMaxRows
    } else if (queryContext.requestModel.maxRows > maximumMaxRows) {
      maximumMaxRows
    } else {
      queryContext.requestModel.maxRows
    }

    queryContext.requestModel.reportingRequest.queryType match {
      case com.yahoo.maha.core.request.GroupByQuery | com.yahoo.maha.core.request.RowCountQuery =>
        val (dimFilterList, factFilterList) = getFilters(queryContext, dims)

        val (aggregatorList, postAggregatorList) = getAggregators(queryContext)
        val dimensionSpecTupleList: mutable.Buffer[(DimensionSpec, Option[DimensionSpec])] = getDimensions(queryContext, factRequestCols, dims, isUsingDruidLookup)

        require(queryContext.requestModel.startIndex < maximumMaxRows, s"startIndex can not exceed $maximumMaxRows")

        val maximumMaxRowsForReq = if (queryContext.requestModel.isAsyncRequest) maximumMaxRowsAsync else maximumMaxRows
        val maxRowsPlusStartIndex = maxRows + queryContext.requestModel.startIndex
        val threshold = if (maxRowsPlusStartIndex > maximumMaxRows) {
          maximumMaxRowsForReq
        } else if (queryContext.requestModel.hasNonFKDimFilters) {
          Math.min(2 * maxRows + queryContext.requestModel.startIndex, maximumMaxRowsForReq)
        } else {
          maxRowsPlusStartIndex
        }

        val haveFactDimCols = queryContext
          .factBestCandidate
          .dimColMapping
          .filterNot { case (name, alias) => Grain.grainFields(alias) }
          .keys
          .exists(factRequestCols)

        //if there is a single fact sort (and sort not on derived column), and single group by, use Top N
        if (queryContext.requestModel.factSortByMap.size == 1
          && factFilterList.isEmpty
          && dimensionSpecTupleList.size == 1
          && threshold <= maximumTopNMaxRows
          && queryContext.requestModel.factSortByMap.nonEmpty
          && queryContext.factBestCandidate.publicFact.aliasToNameColumnMap
          .get(queryContext.requestModel.factSortByMap.head._1)
          .flatMap(queryContext.factBestCandidate.fact.columnsByNameMap.get)
          .exists(!_.isDerivedColumn)
        ) {

          val (sortByAlias, order) = queryContext.requestModel.factSortByMap.head
          val metric = order match {
            case ASC =>
              new InvertedTopNMetricSpec(new NumericTopNMetricSpec(sortByAlias))
            case DESC =>
              new NumericTopNMetricSpec(sortByAlias)
          }

          if (queryContext.requestModel.dimCardinalityEstimate.isDefined) {
            context.put(MIN_TOPN_THRESHOLD, dimCardinality.asInstanceOf[AnyRef])
          }

          val builder: TopNQueryBuilder = new TopNQueryBuilder()
            .dataSource(dataSource)
            .intervals(getInterval(model))
            .granularity(getGranularity(queryContext))
            .metric(metric)
            .threshold(threshold)
            .context(context)

          if (dimensionSpecTupleList.nonEmpty)
            builder.dimension(dimensionSpecTupleList.head._1)

          val dimWithDateTimeFilterList = getDateTimeFilters(queryContext) ++ dimFilterList
          if (dimWithDateTimeFilterList.nonEmpty)
            builder.filters(new AndDimFilter(dimWithDateTimeFilterList.asJava))

          if (aggregatorList.nonEmpty)
            builder.aggregators(aggregatorList.asJava)

          if (postAggregatorList.nonEmpty)
            builder.postAggregators(postAggregatorList.asJava)

          new TopNDruidQuery(queryContext, aliasColumnMap, builder.build(), additionalColumns(queryContext), threshold, model.isSyncRequest)
        }
        //if there are no dimension cols in requested cols and no sorts, use time series request
        else if (!haveFactDimCols
          && factFilterList.isEmpty
          && !queryContext.factBestCandidate.publicFact.renderLocalTimeFilter
          //we ignore the grain fields from dimension spec list since timeseries already provides it
          //so we only expect grain fields in the dimension spec list, otherwise it's not timeseries
          && dimensionSpecTupleList.forall{ case (ds, ods) => Grain.grainFields(ds.getOutputName)}
          && !queryContext.requestModel.hasFactSortBy
          && queryContext.requestModel.isTimeSeries) {
          val builder = Druids.newTimeseriesQueryBuilder()
            .dataSource(dataSource)
            .intervals(getInterval(model))
            .granularity(getGranularity(queryContext))
            .context(context)

          val dimWithDateTimeFilterList = getDateTimeFilters(queryContext) ++ dimFilterList
          if (dimWithDateTimeFilterList.nonEmpty)
            builder.filters(new AndDimFilter(dimWithDateTimeFilterList.asJava))

          if (aggregatorList.nonEmpty)
            builder.aggregators(aggregatorList.asJava)

          if (postAggregatorList.nonEmpty)
            builder.postAggregators(postAggregatorList.asJava)

          new TimeseriesDruidQuery(queryContext, aliasColumnMap, builder.build(), additionalColumns(queryContext), threshold, model.isSyncRequest)
        }
        //else use group by
        else {
          val builder = GroupByQuery.builder()
            .setDataSource(dataSource)
            .setQuerySegmentSpec(getInterval(model))
            .setGranularity(getGranularity(queryContext))
            .setContext(context)

          if (dimensionSpecTupleList.nonEmpty)
            builder.setDimensions(dimensionSpecTupleList.map(_._1).asJava)

          val havingSpec: AndHavingSpec = if (factFilterList.nonEmpty) {
            new AndHavingSpec(factFilterList.asJava)
          } else {
            null
          }
          builder.setHavingSpec(havingSpec)

          if (aggregatorList.nonEmpty)
            builder.setAggregatorSpecs(aggregatorList.asJava)

          if (postAggregatorList.nonEmpty)
            builder.setPostAggregatorSpecs(postAggregatorList.asJava)

          val orderByColumnSpecList = queryContext.requestModel.requestSortByCols
            .view
            .filter(_.isInstanceOf[FactSortByColumnInfo])
            .map(_.asInstanceOf[FactSortByColumnInfo])
            .filter(fsc => !aliasColumnMap(fsc.alias).isInstanceOf[ConstFactCol] && !aliasColumnMap(fsc.alias).isInstanceOf[BaseConstDerivedFactCol])
            .map { fsc: FactSortByColumnInfo =>
              new OrderByColumnSpec(fsc.alias, findDirection(fsc.order), findComparator(aliasColumnMap(fsc.alias).dataType))
            }

          val limitSpec = if (orderByColumnSpecList.nonEmpty) {
            new DefaultLimitSpec(orderByColumnSpecList.asJava, threshold)
          } else {
            new DefaultLimitSpec(null, threshold)
          }

          builder.setLimitSpec(limitSpec)

          val ephemeralAliasColumns: Map[String, Column] = ephemeralAliasColumnMap(queryContext)

          generateGroupByQuery(dims, queryContext, dimensionSpecTupleList, dimFilterList, builder, havingSpec, limitSpec, context, ephemeralAliasColumns, aliasColumnMap, threshold)

        }

      case com.yahoo.maha.core.request.SelectQuery =>
        require(queryContext.requestModel.reportingRequest.sortBy.isEmpty
          , "druid select query type does not support sort by functionality!")
        require(!isUsingDruidLookup
          , "druid select query type does not support druid lookups!")
        queryContext.factBestCandidate.filterCols.foreach {
          colName =>
            require(!fact.columnsByNameMap(colName).isDerivedColumn
              , s"druid select query type does not support filter on derived columns: $colName")
        }
        val (dimFilterList, factFilterList) = getFilters(queryContext, dims)
        val maximumMaxRowsForReq = if (queryContext.requestModel.isAsyncRequest) maximumMaxRowsAsync else maximumMaxRows
        //since select query supports pagination, we don't need to add the offset to the threshold
        val threshold = maxRows

        val builder = Druids.newSelectQueryBuilder()
          .dataSource(dataSource)
          .intervals(getInterval(model))
          .context(context)

        queryContext.factBestCandidate.dimColMapping.collect {
          case (dimCol, alias) if factRequestCols(dimCol) =>
            val column = fact.columnsByNameMap(dimCol)
            require(!column.isDerivedColumn, s"druid select query type does not support derived columns : $alias")
        }
        val dimensionSpecTupleList: mutable.Buffer[(DimensionSpec, Option[DimensionSpec])] = getDimensions(queryContext, factRequestCols, dims, isUsingDruidLookup)
        builder.dimensionSpecs(dimensionSpecTupleList.map(_._1).asJava)

        val factCols = queryContext.factBestCandidate.factColMapping.toList.collect {
          case (nonFkCol, alias) if queryContext.factBestCandidate.requestCols(nonFkCol) =>
            (fact.columnsByNameMap(nonFkCol), alias)
        }

        //render derived columns last
        val groupedFactCols = factCols.groupBy(_._1.isDerivedColumn)

        if (groupedFactCols.contains(true)) {
          throw new UnsupportedOperationException(s"druid select query does not support derived columns : ${groupedFactCols(true).map(_._2).mkString(" ")}")
        }

        val dimWithDateTimeFilterList = getDateTimeFilters(queryContext) ++ dimFilterList
        if (dimWithDateTimeFilterList.nonEmpty)
          builder.filters(new AndDimFilter(dimWithDateTimeFilterList.asJava))

        groupedFactCols.get(false).foreach {
          nonDerivedColsList =>
            builder.metrics(nonDerivedColsList.map {
              case (col, alias) => col.alias.getOrElse(col.name)
            }.asJava)
        }

        val pagination: Option[JValue] = queryContext.requestModel.reportingRequest.pagination.config.get(DruidEngine)

        implicit val formats: org.json4s.Formats = DefaultFormats

        pagination.fold[Unit] {
          val spec = PagingSpec.newSpec(threshold)
          builder.pagingSpec(spec)
        } {
          jvalue =>
            jvalue.findField(_._1 == "pagingIdentifiers").foreach {
              case (_, identifiersJson) =>
                val identifiers: java.util.Map[String, java.lang.Integer] = identifiersJson.extract[Map[String, Int]].mapValues(new java.lang.Integer(_)).asJava
                val spec = new PagingSpec(identifiers, threshold)
                builder.pagingSpec(spec)
            }
        }

        new SelectDruidQuery(queryContext, aliasColumnMap, builder.build(), additionalColumns(queryContext), threshold, model.isSyncRequest)
      case other =>
        throw new UnsupportedOperationException(s"query type unsupported : $other")
    }
  }

  private[this] def generateGroupByQuery(dims: SortedSet[DimensionBundle],
                                         queryContext: FactQueryContext,
                                         dimensionSpecTupleList: mutable.Buffer[(DimensionSpec, Option[DimensionSpec])],
                                         dimFilterList: Seq[DimFilter],
                                         innerGroupByQueryBuilder: Builder,
                                         innerGroupByQueryHavingSpec: HavingSpec,
                                         innerGroupByQueryLimitSpec: DefaultLimitSpec,
                                         context: java.util.Map[String, AnyRef],
                                         ephemeralAliasColumns: Map[String, Column],
                                         aliasColumnMap: Map[String, Column],
                                         threshold: Int): GroupByDruidQuery = {

    // If there are DimFilters on lookup column then generate nested groupby query with dim filter pushed to outer query
    val hasDimFilterOnLookupColumn = dims.filter(p => p.dim.engine == DruidEngine).foldLeft(false) {
      (b, db) => {
        val result = db.filters.filterNot(f => f.field.equals(db.publicDim.primaryKeyByAlias) || db.publicDim.foreignKeyByAlias(f.field)).foldLeft(false) {
          (b, filter) => {
            val result = {
              val name = db.publicDim.aliasToNameMap(filter.field)
              val column = db.dim.dimensionColumnsByNameMap(name)
              column match {
                case DruidFuncDimCol(_, _, _, df, _, _, _) =>
                  df match {
                    case LOOKUP(_, _, _) => true
                    case LOOKUP_WITH_EMPTY_VALUE_OVERRIDE(_, _, _, _) => true
                    case LOOKUP_WITH_DECODE(_, _, _, args@_*) => true
                    case LOOKUP_WITH_DECODE_RETAIN_MISSING_VALUE(_, _, _, _, _, args@_*) => true
                    case LOOKUP_WITH_DECODE_ON_OTHER_COLUMN(_, _, _, _, _, _) => true
                    case LOOKUP_WITH_TIMEFORMATTER(_, _, _, _, _, _) => true
                    case LOOKUP_WITH_TIMESTAMP(_, _, _, _, _, _) => true
                    case _ => false
                  }
                case _ => false
              }
            }
            b || result
          }
        }
        b || result
      }
    }

    // If there are lookup with decode column then generate nested groupby query with dim decode pushed to outer query
    val hasLookupWithDecodeColumn = dims.filter(p => p.dim.engine == DruidEngine).foldLeft(false) {
      (b, db) => {
        val result = db.fields.filterNot(f => f.equals(db.publicDim.primaryKeyByAlias) || db.publicDim.foreignKeyByAlias(f)).foldLeft(false) {
          (b, field) => {
            val result = {
              val name = db.publicDim.aliasToNameMap(field)
              val column = db.dim.dimensionColumnsByNameMap(name)
              column match {
                case DruidFuncDimCol(_, _, _, df, _, _, _) =>
                  df match {
                    case LOOKUP_WITH_DECODE(_, _, _, args@_*) => true
                    case LOOKUP_WITH_DECODE_RETAIN_MISSING_VALUE(_, _, _, _, _, args@_*) => true
                    case LOOKUP_WITH_TIMEFORMATTER(_, _, _, _, _, _) => true
                    case LOOKUP_WITH_TIMESTAMP(_, _, _, _, _, _) => true
                    case _ => false
                  }
                case _ => false
              }
            }
            b || result
          }
        }
        b || result
      }
    }

    val hasExpensiveDateDimFilter = FilterDruid.isExpensiveDateDimFilter(queryContext.requestModel, queryContext.factBestCandidate.publicFact.aliasToNameColumnMap, queryContext.factBestCandidate.fact.columnsByNameMap)

    // check if it is a rowcount query from "RowCountCurator", overwrites the queryContext, aliasColumnMap
//    val isRowCountRequest = queryContext.requestModel.reportingRequest.curatorJsonConfigMap.contains("rowcount")
    val isRowCountRequest = queryContext.requestModel.reportingRequest.queryType == RowCountQuery

    val groupByQuery: GroupByQuery = if (hasDimFilterOnLookupColumn || hasLookupWithDecodeColumn || hasExpensiveDateDimFilter) {

      //this is nested groupBy query so reset limitSpec inside inner query
      innerGroupByQueryBuilder.setLimitSpec(NoopLimitSpec.INSTANCE)

      if(!hasExpensiveDateDimFilter) {
        val dimWithDateTimeFilterList = getDateTimeFilters(queryContext) ++ dimFilterList
        if (dimWithDateTimeFilterList.nonEmpty)
          innerGroupByQueryBuilder.setDimFilter(new AndDimFilter(dimWithDateTimeFilterList.asJava))
      } else if (dimFilterList.nonEmpty) {
        innerGroupByQueryBuilder.setDimFilter(new AndDimFilter(dimFilterList.asJava))
      }

      val outerQueryBuilder = GroupByQuery.builder()
        .setDataSource(innerGroupByQueryBuilder.build())
        .setQuerySegmentSpec(getInterval(queryContext.requestModel))
        .setGranularity(getGranularity(queryContext))
        .setContext(context)

      val outerQueryDimensionSpecList: mutable.Buffer[DimensionSpec] = new ArrayBuffer[DimensionSpec](queryContext.requestModel.requestCols.size)
      dimensionSpecTupleList.map {
        dimensionSpecTuple => {
          val outputName: String = dimensionSpecTuple._1.getOutputName
          queryContext.requestModel.requestCols.map {
            requestCol => {
              if (requestCol.alias == outputName) {
                outerQueryDimensionSpecList += dimensionSpecTuple._2.getOrElse(new DefaultDimensionSpec(outputName, outputName))
              }
            }
          }
          ephemeralAliasColumns.keySet.map {
            ephemeralColAlias => {


              if (ephemeralColAlias == outputName) {
                outerQueryDimensionSpecList += dimensionSpecTuple._2.getOrElse(new DefaultDimensionSpec(outputName, outputName))
              }
            }
          }
        }
      }

      if (outerQueryDimensionSpecList.nonEmpty)
        outerQueryBuilder.setDimensions(outerQueryDimensionSpecList.asJava)

      val cols = dims.flatMap(f => f.dim.columnsByNameMap).toMap ++ queryContext.factBestCandidate.fact.dimColMap
      val aliases = dims.flatMap(f => f.publicDim.aliasToNameMapFull) ++ queryContext.factBestCandidate.publicFact.aliasToNameColumnMap
      val outerQueryDimFilterList = new ArrayBuffer[DimFilter](queryContext.factBestCandidate.dimColMapping.size)
      dims.foreach {
        db => {
          val flatOrFilterMeta: Set[Filter] = queryContext.requestModel.orFilterMeta.flatMap(orFilterMeta => orFilterMeta.orFilter.filters)
          db.filters.filterNot(f => f.field.equals(db.publicDim.primaryKeyByAlias) || db.publicDim.foreignKeyByAlias(f.field) || flatOrFilterMeta.contains(f)).foreach {
            filter =>
              val grainOption = Option(queryContext.factBestCandidate.fact.grain)
              outerQueryDimFilterList += FilterDruid.renderFilterDim(
                filter,
                db.publicDim.aliasToNameMap,
                db.dim.dimensionColumnsByNameMap, grainOption, forOuterQuery = true)
          }
        }
      }

      //Add entire OR filter to outer query if any of them are dimType
      if(queryContext.requestModel.orFilterMeta.map(orFilterMeta => orFilterMeta.filterType).contains(MetaType.DimType)) {
        val flatOrFilterMetaFields: Set[Filter] = queryContext.requestModel.orFilterMeta.flatMap(orFilterMeta => orFilterMeta.orFilter.filters)
        outerQueryDimFilterList += FilterDruid.renderOrDimFilters(flatOrFilterMetaFields.toList, aliases.toMap, cols , Option.empty, true)
      }

      if(hasExpensiveDateDimFilter) {
        outerQueryDimFilterList ++= FilterDruid.renderDateDimFilters(queryContext.requestModel, queryContext.factBestCandidate.publicFact.aliasToNameColumnMap, queryContext.factBestCandidate.fact.columnsByNameMap, forOuterQuery = true)
      }

      if (outerQueryDimFilterList.nonEmpty) {
        outerQueryBuilder.setDimFilter(new AndDimFilter(outerQueryDimFilterList.asJava))
      }

      if (innerGroupByQueryHavingSpec != null) {
        outerQueryBuilder.setHavingSpec(innerGroupByQueryHavingSpec)
      }

      if (innerGroupByQueryLimitSpec != null && !isRowCountRequest) {
        outerQueryBuilder.setLimitSpec(innerGroupByQueryLimitSpec)
      }

      val (outerQueryAggregatorList, outerQueryPostAggregatorList) = getAggregatorsForOuterQuery(queryContext)

      if (outerQueryAggregatorList.nonEmpty)
        outerQueryBuilder.setAggregatorSpecs(outerQueryAggregatorList.asJava)

      if (outerQueryPostAggregatorList.nonEmpty)
        outerQueryBuilder.setPostAggregatorSpecs(outerQueryPostAggregatorList.asJava)

      outerQueryBuilder.build()

    } else {
      val dimWithDateTimeFilterList = getDateTimeFilters(queryContext) ++ dimFilterList
      if (dimWithDateTimeFilterList.nonEmpty)
        innerGroupByQueryBuilder.setDimFilter(new AndDimFilter(dimWithDateTimeFilterList.asJava))
      if (isRowCountRequest)
        innerGroupByQueryBuilder.setLimitSpec(NoopLimitSpec.INSTANCE)
      innerGroupByQueryBuilder.build()
    }

    val (finalQuery, finalQueryContext, finalAliasColumnMap) = if (isRowCountRequest) {
      //overwrite query
      val rowCountQueryBuilder = GroupByQuery.builder()
        .setDataSource(groupByQuery)
        .setQuerySegmentSpec(getInterval(queryContext.requestModel))
        .setGranularity(getGranularity(queryContext))
        .setContext(context)
      val countAggregatorFactory: AggregatorFactory = new CountAggregatorFactory(QueryRowList.ROW_COUNT_ALIAS)
      rowCountQueryBuilder.addAggregator(countAggregatorFactory)
      val rowCountGroupByQuery = rowCountQueryBuilder.build()

      // overwrite queryContext
      val copyRequestModel = queryContext.requestModel.copy(requestCols = IndexedSeq(FactColumnInfo(QueryRowList.ROW_COUNT_ALIAS)))
      val copyQueryContext = queryContext.copy(requestModel = copyRequestModel)

      // overwrite aliasColumnMap
      val copyAliasColumnMap = Map[String, Column](QueryRowList.ROW_COUNT_ALIAS -> FactCol(QueryRowList.ROW_COUNT_ALIAS, IntType())(new ColumnContext))

      (rowCountGroupByQuery, copyQueryContext, copyAliasColumnMap)
    } else {
      (groupByQuery, queryContext, aliasColumnMap)
    }

    new GroupByDruidQuery(finalQueryContext, finalAliasColumnMap, finalQuery, additionalColumns(queryContext), ephemeralAliasColumns, threshold, queryContext.requestModel.isSyncRequest)
  }

  private[this] def getBetweenDates(model: RequestModel): (DateTime, DateTime) = {
    val (dayFrom, dayTo) = {
      val (f, t) = FilterDruid.extractFromAndToDate(model.utcTimeDayFilter, DailyGrain)
      (f, t.plusDays(1))
    }

    val (dayWithHourFrom, dayWithHourTo) = model.utcTimeHourFilter.fold((dayFrom, dayTo)) {
      filter =>
        val (f, t) = FilterDruid.extractFromAndToDate(filter, HourlyGrain)
        (dayFrom.withHourOfDay(f.getHourOfDay), dayTo.minusDays(1).withHourOfDay(t.getHourOfDay).plusHours(1))
    }

    model.utcTimeMinuteFilter.fold((dayWithHourFrom, dayWithHourTo)) {
      filter =>
        val (f, t) = FilterDruid.extractFromAndToDate(filter, MinuteGrain)
        (dayWithHourFrom.withMinuteOfHour(f.getMinuteOfHour), dayWithHourTo.minusHours(1).withMinuteOfHour(t.getMinuteOfHour).plusMinutes(1))
    }
  }

  private[this] def getEqualityDates(model: RequestModel): (DateTime, DateTime) = {
    val (dayFrom, dayTo) = model.utcTimeDayFilter match {
      case EqualityFilter(_, value, _, _) =>
        val f = DailyGrain.fromFormattedString(value)
        (f, f.plusDays(1))
      case any => throw new UnsupportedOperationException(s"Only equality filter supported : $any")
    }
    val (dayWithHourFrom, dayWithHourTo) = model.utcTimeHourFilter.fold((dayFrom, dayTo)) {
      case EqualityFilter(_, value, _, _) =>
        val f = HourlyGrain.fromFormattedString(value)
        (dayFrom.withHourOfDay(f.getHourOfDay), dayTo.minusDays(1).withHourOfDay(f.getHourOfDay).plusHours(1))
      case any => throw new UnsupportedOperationException(s"Only equality filter supported : $any")
    }

    model.utcTimeMinuteFilter.fold((dayWithHourFrom, dayWithHourTo)) {
      case EqualityFilter(_, value, _, _) =>
        val f = MinuteGrain.fromFormattedString(value)
        (dayWithHourFrom.withMinuteOfHour(f.getMinuteOfHour), dayWithHourTo.minusHours(1).withMinuteOfHour(f.getMinuteOfHour).plusMinutes(1))
      case any => throw new UnsupportedOperationException(s"Only equality filter supported : $any")
    }
  }

  private[this] def getInterval(model: RequestModel): QuerySegmentSpec = {
    model.utcTimeDayFilter match {
      case BetweenFilter(_, from, to) =>
        val (f, t) = getBetweenDates(model)
        val interval = new Interval(f, t)
        new MultipleIntervalSegmentSpec(java.util.Arrays.asList(interval))
      case InFilter(_, values, _, _) =>
        val intervals = values.map {
          d =>
            val f = DailyGrain.fromFormattedString(d)
            val t = f.plusDays(1)
            new Interval(f, t)
        }
        new MultipleIntervalSegmentSpec(intervals.asJava)
      case EqualityFilter(_, value, _, _) =>
        val (f, t) = getEqualityDates(model)
        val interval = new Interval(f, t)
        new MultipleIntervalSegmentSpec(java.util.Arrays.asList(interval))
      case any =>
        throw new IllegalArgumentException(s"Unsupported day filter : $any")
    }
  }

  private[this] def getGranularity(queryContext: FactualQueryContext): io.druid.java.util.common.granularity.Granularity = {
    //for now, just do day
    if (queryContext.requestModel.isTimeSeries && !queryContext.factBestCandidate.publicFact.renderLocalTimeFilter) {
      GranularityType.DAY.getDefaultGranularity
    } else {
      GranularityType.ALL.getDefaultGranularity
    }
  }

  private[this] def getDimValueType(column: Column): ValueType = {
    ValueType.STRING
  }

  private[this] def getAggregatorsForOuterQuery(queryContext: FactQueryContext): (mutable.Buffer[AggregatorFactory], mutable.Buffer[PostAggregator]) = {
    getAggregators(queryContext, forOuterQuery = true)
  }

  private[this] def getAggregators(queryContext: FactQueryContext, forOuterQuery: Boolean = false): (mutable.Buffer[AggregatorFactory], mutable.Buffer[PostAggregator]) = {
    val fact = queryContext.factBestCandidate.fact

    val aggregatorAliasSet = new mutable.TreeSet[String]()
    val aggregatorNameAliasMap = new mutable.HashMap[String, String]()
    val aggregatorList = new ArrayBuffer[AggregatorFactory](2 * queryContext.factBestCandidate.factColMapping.size)
    val postAggregatorList = new ArrayBuffer[PostAggregator](2 * queryContext.factBestCandidate.factColMapping.size)

    def applyScaleCleanup(scale: Int): Int = {
      if (scale == 0) {
        10
      } else scale
    }

    def getSumAggregatorFactory(dataType: DataType, outputFieldName: String, inputFieldName: String): AggregatorFactory = {
      dataType match {
        case DecType(_, scale, Some(default), Some(min), Some(max), _) =>
          //TODO: fix min, max, and default value handling
          if(useCustomRoundingSumAggregator) {
            new RoundingDoubleSumAggregatorFactory(outputFieldName, inputFieldName, applyScaleCleanup(scale), null, ExprMacroTable.nil, true)
          } else {
            new DoubleSumAggregatorFactory(outputFieldName, inputFieldName)
          }
        case IntType(_, _, Some(default), Some(min), Some(max)) =>
          //TODO: fix min, max, and default value handling
          new LongSumAggregatorFactory(outputFieldName, inputFieldName)
        case DecType(_, scale, _, _, _, _) =>
          if(useCustomRoundingSumAggregator) {
            new RoundingDoubleSumAggregatorFactory(outputFieldName, inputFieldName, applyScaleCleanup(scale), null, ExprMacroTable.nil, true)
          } else {
            new DoubleSumAggregatorFactory(outputFieldName, inputFieldName)
          }
        case IntType(_, _, _, _, _) =>
          new LongSumAggregatorFactory(outputFieldName, inputFieldName)
        case any =>
          throw new UnsupportedOperationException(s"Unhandled data type $any")
      }
    }

    def getHyperUniqueAggregatorFactory(dataType: DataType, outputFieldName:String, fieldName: String): AggregatorFactory = {
      dataType match {
        case IntType(_, _, _, _, _) =>
          new HyperUniquesAggregatorFactory(outputFieldName, fieldName, false, true)
        case any =>
          throw new UnsupportedOperationException(s"Unhandled data type $any")
      }
    }

    def getMinAggregatorFactory(dataType: DataType, outputFieldName: String, inputFieldName: String): AggregatorFactory = {
      dataType match {
        case DecType(_, _, Some(default), Some(min), Some(max), _) =>
          //TODO: fix min, max, and default value handling
          new DoubleMinAggregatorFactory(outputFieldName, inputFieldName)
        case IntType(_, _, Some(default), Some(min), Some(max)) =>
          //TODO: fix min, max, and default value handling
          new LongMinAggregatorFactory(outputFieldName, inputFieldName)
        case DecType(_, _, _, _, _, _) =>
          new DoubleMinAggregatorFactory(outputFieldName, inputFieldName)
        case IntType(_, _, _, _, _) =>
          new LongMinAggregatorFactory(outputFieldName, inputFieldName)
        case any =>
          throw new UnsupportedOperationException(s"Unhandled data type $any")
      }
    }

    def getMaxAggregatorFactory(dataType: DataType, outputFieldName: String, inputFieldName: String): AggregatorFactory = {
      dataType match {
        case DecType(_, _, Some(default), Some(min), Some(max), _) =>
          //TODO: fix min, max, and default value handling
          new DoubleMaxAggregatorFactory(outputFieldName, inputFieldName)
        case IntType(_, _, Some(default), Some(min), Some(max)) =>
          //TODO: fix min, max, and default value handling
          new LongMaxAggregatorFactory(outputFieldName, inputFieldName)
        case DecType(_, _, _, _, _, _) =>
          new DoubleMaxAggregatorFactory(outputFieldName, inputFieldName)
        case IntType(_, _, _, _, _) =>
          new LongMaxAggregatorFactory(outputFieldName, inputFieldName)
        case any =>
          throw new UnsupportedOperationException(s"Unhandled data type $any")
      }
    }

    def getThetaSketchAggregatorFactory(outputFieldName: String, inputFieldName: String): AggregatorFactory = {
      new SketchMergeAggregatorFactory(outputFieldName, inputFieldName, null, null, false, null)
    }

    def getCountAggregatorFactory(dataType: DataType, outputFieldName: String): AggregatorFactory = {
      dataType match {
        case any =>
          new CountAggregatorFactory(outputFieldName)
      }
    }

    def getFilteredAggregatorFactory(dataType: DataType, rollup: RollupExpression, alias: String): AggregatorFactory = {
      val grainOption = Option(fact.grain)
      val druidFilteredRollup: DruidFilteredRollup = rollup.asInstanceOf[DruidFilteredRollup]

      val filter: Filter = druidFilteredRollup.delegateAggregatorRollupExpression match {
        case DruidThetaSketchRollup if queryContext.factBestCandidate.dimColMapping.contains(druidFilteredRollup.filter.field) => {
          val publicFactFilterName: String = queryContext.factBestCandidate.dimColMapping(druidFilteredRollup.filter.field)
          val publicFactFilter: Filter = queryContext.factBestCandidate.filters.filter(f => f.field.equals(publicFactFilterName)).head
          publicFactFilter
        }
        case any => {
          druidFilteredRollup.filter
        }
      }

      val dimFilter: DimFilter = FilterDruid.renderFilterDim(
        filter,
        queryContext.factBestCandidate.publicFact.aliasToNameColumnMap,
        fact.columnsByNameMap, grainOption)
      new FilteredAggregatorFactory(getAggregatorFactory(dataType, druidFilteredRollup.delegateAggregatorRollupExpression,
        alias, druidFilteredRollup.factCol.fieldNamePlaceHolder), dimFilter)
    }

    def getFilteredListAggregatorFactory(dataType: DataType, rollup: RollupExpression, alias: String): AggregatorFactory = {

      val druidFilteredListRollup: DruidFilteredListRollup = rollup.asInstanceOf[DruidFilteredListRollup]
      val filterList = druidFilteredListRollup.filter
      if (filterList.size < 2) {
        throw new UnsupportedOperationException(s"ToUse FilteredListAggregator filterList must have 2 or more filters, actualInput $filterList")
      } else {
        val dimFilterList: java.util.List[DimFilter] =
          filterList.map { (filter) =>
            FilterDruid.renderFilterDim(filter, fact.columnsByNameMap.map(e => e._1 -> e._2.name), fact.columnsByNameMap, Option(fact.grain))
          }.asJava

        val dimFilter: AndDimFilter = Druids.newAndDimFilterBuilder().fields(dimFilterList).build

        new FilteredAggregatorFactory(getAggregatorFactory(dataType, druidFilteredListRollup.delegateAggregatorRollupExpression,
          alias, druidFilteredListRollup.factCol.fieldNamePlaceHolder), dimFilter)
      }
    }

    def getAggregatorFactory(dataType: DataType, rollup: RollupExpression, alias: String, columnAlias: String): AggregatorFactory = {
      rollup match {
        case SumRollup =>
          getSumAggregatorFactory(dataType, alias, columnAlias)
        case MinRollup =>
          getMinAggregatorFactory(dataType, alias, columnAlias)
        case MaxRollup =>
          getMaxAggregatorFactory(dataType, alias, columnAlias)
        case CountRollup =>
          getCountAggregatorFactory(dataType, columnAlias)
        case DruidFilteredRollup(_, _, _) if (forOuterQuery) =>
          getAggregatorFactory(
            dataType,
            rollup.asInstanceOf[DruidFilteredRollup].delegateAggregatorRollupExpression,
            alias,
            columnAlias)
        case DruidFilteredRollup(_, _, _) =>
          getFilteredAggregatorFactory(dataType, rollup, alias)
        case DruidFilteredListRollup(_, _, _) if (forOuterQuery) =>
          getAggregatorFactory(
            dataType,
            rollup.asInstanceOf[DruidFilteredListRollup].delegateAggregatorRollupExpression,
            alias,
            columnAlias)
        case DruidFilteredListRollup(_, _, _) =>
          getFilteredListAggregatorFactory(dataType, rollup, alias)
        case DruidHyperUniqueRollup(fieldName) =>
          getHyperUniqueAggregatorFactory(dataType, alias, fieldName)
        case DruidThetaSketchRollup =>
          getThetaSketchAggregatorFactory(alias, columnAlias)
        case NoopRollup =>
          throw new UnsupportedOperationException(s"Unsupported rollup expression: NoopRollup")
        case any =>
          throw new UnsupportedOperationException(s"Unhandled rollup expression : $any")
      }
    }

    def addAggregatorFactory(dataType: DataType, rollup: RollupExpression, alias: String, columnAlias: String, columnName: String): Unit = {
      rollup match {
        case any if aggregatorAliasSet(columnAlias) =>
        //already added this column, ignore
        case AverageRollup =>
          val sumName = s"_sum_$columnAlias"
          val countName = s"_count_$columnAlias"
          aggregatorList += getSumAggregatorFactory(dataType, sumName, if (forOuterQuery) sumName else columnAlias)
          aggregatorAliasSet += sumName
          aggregatorNameAliasMap += (sumName -> sumName)
          aggregatorList += getCountAggregatorFactory(dataType, countName)
          aggregatorAliasSet += countName
          aggregatorNameAliasMap += (countName -> countName)
          val sumFieldAccess = new FieldAccessPostAggregator(sumName, sumName)
          val countFieldAccess = new FieldAccessPostAggregator(countName, countName)
          postAggregatorList += new ArithmeticPostAggregator(alias, "/", Lists.newArrayList(sumFieldAccess, countFieldAccess))
        case any =>
          aggregatorList += getAggregatorFactory(dataType, rollup, alias, if (forOuterQuery) alias else columnAlias)
          aggregatorAliasSet += columnAlias
          aggregatorNameAliasMap += (columnAlias -> alias)
          aggregatorNameAliasMap += (columnName -> alias)
      }
    }

    def renderColumnWithAlias(fact: Fact, column: Column, alias: String, forPostAggregator: Boolean = false): Unit = {
      column match {
        case FactCol(_, dt, cc, rollup, _, annotations, _) =>
          addAggregatorFactory(dt, rollup, alias, column.alias.getOrElse(column.name), column.name)
        case DruidDerFactCol(_, _, dt, cc, de, annotations, rollup, _) =>
          //this is a post aggregate but we need to add source columns
          de.sourceColumns.foreach {
            src =>
              val sourceCol = fact.columnsByNameMap(src)
              if (!sourceCol.isDerivedColumn) {
                val name = sourceCol.alias.getOrElse(sourceCol.name)
                sourceCol match {
                  case FactCol(_, _, _, rollup, _, _, _) =>
                    rollup match {
                      case DruidFilteredRollup(filter, _, re) =>
                        re match {
                          case DruidThetaSketchRollup =>
                            if (queryContext.factBestCandidate.dimColMapping.contains(filter.field)) {
                              //check if we already added this column
                              if (!aggregatorAliasSet(name)) {
                                renderColumnWithAlias(fact, sourceCol, name, forPostAggregator = true)
                              }
                            }
                          case _ =>
                            if (!aggregatorAliasSet(name)) {
                              renderColumnWithAlias(fact, sourceCol, name, forPostAggregator = true)
                            }
                        }
                      case _ =>
                        if (!aggregatorAliasSet(name)) {
                          renderColumnWithAlias(fact, sourceCol, name, forPostAggregator = true)
                        }
                    }
                  case _ =>
                    if (!aggregatorAliasSet(name)) {
                      renderColumnWithAlias(fact, sourceCol, name, forPostAggregator = true)
                    }
                }
              }
          }

          postAggregatorList += de.render(alias)(alias, aggregatorNameAliasMap.toMap)

        case ConstFactCol(_, dt, value, cc, rollup, _, annotations, _) =>
        //Handling Constant Cols in Post Process step in executor
        case DruidConstDerFactCol(_, dt, value, _, cc, rollup, _, annotations, _) =>
        //Handling Constant Derived Fact Cols in Post Process step in executor
        case DruidPostResultDerivedFactCol(_, _, dt, cc, de, annotations, rollup, _, prf) =>
          //this is a post aggregate but we need to add source columns
          de.sourceColumns.foreach {
            src =>
              val sourceCol = fact.columnsByNameMap(src)
              if (!sourceCol.isDerivedColumn) {
                val name = sourceCol.alias.getOrElse(sourceCol.name)
                //check if we already added this column
                if (!aggregatorAliasSet(name)) {
                  renderColumnWithAlias(fact, sourceCol, name, forPostAggregator = true)
                }
              }
          }

          postAggregatorList += de.render(alias)(alias, aggregatorNameAliasMap.toMap)
        case any =>
          throw new UnsupportedOperationException(s"Found unhandled column : $any")
      }
    }

    val factCols = queryContext.factBestCandidate.factColMapping.toList.collect {
      case (nonFkCol, alias) => (fact.columnsByNameMap(nonFkCol), alias)
    }

    //render derived columns last
    val groupedFactCols = factCols.groupBy(_._1.isDerivedColumn)
    //render non derived columns first
    groupedFactCols.get(false).foreach { nonDerivedCols =>
      nonDerivedCols.foreach {
        case (column, alias) =>
          renderColumnWithAlias(fact, column, alias)
      }
    }

    def renderDerivedColumns(derivedCols: List[(Column, String)], renderedDerivedCols: mutable.Set[String] = mutable.Set.empty[String]): Unit = {
      if (derivedCols.nonEmpty) {
        val dependentColumns: Set[String] =
          derivedCols.view.map(_._1.asInstanceOf[DerivedColumn]).flatMap(dc => dc.derivedExpression.sourceColumns).toSet
        val derivedDependentCols: List[(Column, String)] = dependentColumns.toList.collect {
          case col if fact.columnsByNameMap(col).isDerivedColumn =>
            fact.columnsByNameMap(col) -> col
        }
        renderDerivedColumns(derivedDependentCols, renderedDerivedCols)

        derivedCols.foreach {
          case (column, alias) =>
            if (!dependentColumns(column.name) && !renderedDerivedCols(column.name)) {
              renderColumnWithAlias(fact, column, alias)
              renderedDerivedCols += column.name
            }
        }
      }
    }

    groupedFactCols.get(true).foreach(renderDerivedColumns(_))
    (aggregatorList, postAggregatorList)
  }

  private[this] def getDimensions(queryContext: FactQueryContext, factRequestCols: Set[String], dims: SortedSet[DimensionBundle], isUsingDruidLookups: Boolean): mutable.Buffer[(DimensionSpec, Option[DimensionSpec])] = {
    val fact = queryContext.factBestCandidate.fact
    val dimensionSpecTupleList = new ArrayBuffer[(DimensionSpec, Option[DimensionSpec])](queryContext.factBestCandidate.dimColMapping.size)

    def getTargetTimeFormat(fact: Fact, dimCol: Column): String = {
      val targetTimeFormat: String = dimCol.dataType match {
        case DateType(sourceFormat) =>
          sourceFormat.getOrElse(fact.grain.formatString)
        case StrType(_, _, _) =>
          fact.grain.formatString
        case any =>
          throw new UnsupportedOperationException(s"Found unhandled dataType : $any")
      }
      targetTimeFormat
    }

    def getTimeDimExtractionSpec(fact: Fact, outputName: String, colName: String, format: String): (DimensionSpec, Option[DimensionSpec]) = {
      val dimCol = fact.columnsByNameMap(colName)
      val targetTimeFormat: String = getTargetTimeFormat(fact, dimCol)
      val exFn = new TimeDimExtractionFn(targetTimeFormat, format)
      (new ExtractionDimensionSpec(dimCol.alias.getOrElse(dimCol.name), outputName, getDimValueType(dimCol), exFn, null), Option.empty)
    }

    def renderColumnWithAlias(fact: Fact, column: Column, alias: String): (DimensionSpec, Option[DimensionSpec]) = {
      val name = column.alias.getOrElse(column.name)
      column match {
        case DimCol(_, dt, cc, _, annotations, _) =>
          dt match {
            case IntType(_, sm, _, _, _) if sm.isDefined =>
              val defaultValue = sm.get.default
              val map = sm.get.tToStringMap.map { case (k, v) => k.toString -> v }
              val lookup = new MapLookupExtractor(map.asJava, false)
              val exFn = new LookupExtractionFn(lookup, false, defaultValue, false, true)
              (new ExtractionDimensionSpec(name, alias, getDimValueType(column), exFn, null), Option.empty)
            case StrType(_, sm, _) if sm.isDefined =>
              val defaultValue = sm.get.default
              val lookup = new MapLookupExtractor(sm.get.tToStringMap.asJava, false)
              val exFn = new LookupExtractionFn(lookup, false, defaultValue, false, true)
              (new ExtractionDimensionSpec(name, alias, getDimValueType(column), exFn, null), Option.empty)
            case _ =>
              (new DefaultDimensionSpec(name, alias), Option.empty)
          }
        case DruidFuncDimCol(_, dt, cc, df, _, _, _) =>
          df match {
            case intervalDateFunc@GET_INTERVAL_DATE(filedName, resultFormat) =>
              val dimCol = fact.columnsByNameMap(intervalDateFunc.dimColName)
              val targetTimeFormat: String = dimCol.dataType match {
                case DateType(sourceFormat) =>
                  sourceFormat.getOrElse(fact.grain.formatString)
                case StrType(_, _, _) =>
                  fact.grain.formatString
                case any =>
                  throw new UnsupportedOperationException(s"Found unhandled dataType : $any")
              }

              val exFn = new TimeDimExtractionFn(targetTimeFormat, resultFormat)
              (new ExtractionDimensionSpec(dimCol.alias.getOrElse(dimCol.name), alias, getDimValueType(column), exFn, null), Option.empty)
            case dayOfWeekFunc@DAY_OF_WEEK(fieldName) =>
              val dimCol = fact.columnsByNameMap(dayOfWeekFunc.dimColName)
              val targetTimeFormat: String = dimCol.dataType match {
                case DateType(sourceFormat) =>
                  sourceFormat.getOrElse(fact.grain.formatString)
                case StrType(_, _, _) =>
                  fact.grain.formatString
                case any =>
                  throw new UnsupportedOperationException(s"Found unhandled dataType : $any")
              }

              val exFn = new TimeDimExtractionFn(targetTimeFormat, "EEEE")
              (new ExtractionDimensionSpec(dimCol.alias.getOrElse(dimCol.name), alias, getDimValueType(column), exFn, null), Option.empty)

            case decodeDimFunction@DECODE_DIM(fieldName, args@_*) =>
              dt match {
                case IntType(_, _, _, _, _) | StrType(_, _, _) if args.length > 1 =>
                  val map = args.grouped(2).filter(_.size == 2).map(seq => (seq.head, seq(1))).toMap
                  val lookup = new MapLookupExtractor(map.asJava, false)
                  val exFn = {
                    if (args.length > 2 && args.length % 2 == 1) {
                      new LookupExtractionFn(lookup, false, args(args.length - 1), false, true)
                    } else {
                      new LookupExtractionFn(lookup, true, null, false, true)
                    }
                  }
                  (new ExtractionDimensionSpec(decodeDimFunction.dimColName, alias, getDimValueType(column), exFn, null), Option.empty)
                case _ =>
                  (new DefaultDimensionSpec(name, alias), Option.empty)
              }

            case javascript@JAVASCRIPT(_, function) =>
              val exFn = new JavaScriptExtractionFn(function, false, JavaScriptConfig.getEnabledInstance)
              (new ExtractionDimensionSpec(javascript.dimColName, alias, getDimValueType(column), exFn, null), Option.empty)
            case regex@REGEX(fieldName, expr, index, replaceMissingValue, replaceMissingValueWith) =>
              val exFn = new RegexDimExtractionFn(expr, index, replaceMissingValue, replaceMissingValueWith)
              (new ExtractionDimensionSpec(regex.dimColName, alias, getDimValueType(column), exFn, null), Option.empty)
            case DRUID_TIME_FORMAT(fmt, zone) =>
              val exFn = new TimeFormatExtractionFn(fmt, zone, null, null, false)
              (new ExtractionDimensionSpec(DRUID_TIME_FORMAT.sourceDimColName, alias, getDimValueType(column), exFn, null), Option.empty)
            case DRUID_TIME_FORMAT_WITH_PERIOD_GRANULARITY(fmt, period, zone) =>
              val periodGranularity = new PeriodGranularity(new Period(period), null, zone)
              val exFn = new TimeFormatExtractionFn(fmt, zone, null, periodGranularity, false)
              (new ExtractionDimensionSpec(DRUID_TIME_FORMAT_WITH_PERIOD_GRANULARITY.sourceDimColName, alias, getDimValueType(column), exFn, null), Option.empty)
            case TIME_FORMAT_WITH_REQUEST_CONTEXT(fmt) =>
              val timezoneValue = queryContext.requestModel.additionalParameters
                .getOrElse(Parameter.TimeZone, TimeZoneValue.apply(DateTimeZone.UTC.getID)).asInstanceOf[TimeZoneValue]
              val timezone = DateTimeZone.forID(timezoneValue.value)
              val exFn = new TimeFormatExtractionFn(fmt, timezone, null, null, false)
              (new ExtractionDimensionSpec(TIME_FORMAT_WITH_REQUEST_CONTEXT.sourceDimColName, alias, getDimValueType(column), exFn, null), Option.empty)
            case datetimeFormatter@DATETIME_FORMATTER(fieldName, index, length) =>
              val exFn = new SubstringDimExtractionFn(index, length)
              (new ExtractionDimensionSpec(datetimeFormatter.dimColName, alias, getDimValueType(column), exFn, null), Option.empty)
            case any =>
              throw new UnsupportedOperationException(s"Found unhandled DruidDerivedFunction : $any")
          }
        case DruidPostResultFuncDimCol(outputName, _, _, pdf, _, _, _) =>
          pdf match {
            case startOfTheWeek@START_OF_THE_WEEK(_) =>
              getTimeDimExtractionSpec(fact, outputName, startOfTheWeek.colName, startOfTheWeek.yearandWeekOfTheYearFormatForDruid)

            case startOfTheMonth@START_OF_THE_MONTH(_) =>
              getTimeDimExtractionSpec(fact, outputName, startOfTheMonth.colName, startOfTheMonth.startOfTheMonthFormat)

            case any =>
              throw new UnsupportedOperationException(s"Found unhandled DruidPostResultFuncDimCol : $any")
          }
        case any =>
          throw new UnsupportedOperationException(s"Found unhandled column : $any")
      }
    }

    def renderColumnWithAliasUsingDimensionBundle(fact: Fact, column: Column, alias: String, db: DimensionBundle, useQueryLevelCache: Boolean): (DimensionSpec, Option[DimensionSpec]) = {
      val name = column.alias.getOrElse(column.name)
      column match {
        case DimCol(_, dt, cc, _, annotations, _) =>
          renderColumnWithAlias(fact, column, alias)

        case DruidFuncDimCol(outputName, dt, cc, df, _, _, _) =>
          df match {
            case intervalDateFunc@GET_INTERVAL_DATE(filedName, resultFormat) =>
              renderColumnWithAlias(fact, column, alias)
            case dayOfWeekFunc@DAY_OF_WEEK(fieldName) =>
              renderColumnWithAlias(fact, column, alias)
            case decodeDimFunction@DECODE_DIM(fieldName, args@_*) =>
              renderColumnWithAlias(fact, column, alias)
            case javascript@JAVASCRIPT(fieldName, function) =>
              renderColumnWithAlias(fact, column, alias)
            case regex@REGEX(fieldName, expr, index, replaceMissingValue, replaceMissingValueWith) =>
              renderColumnWithAlias(fact, column, alias)
            case datetimeFormatter@DATETIME_FORMATTER(fieldName, index, length) =>
              renderColumnWithAlias(fact, column, alias)

            case lookupFunc@LOOKUP(lookupNamespace, valueColumn, dimensionOverrideMap) =>
              val regExFn = new MahaRegisteredLookupExtractionFn(null, lookupNamespace, false, DruidQuery.replaceMissingValueWith, false, true, valueColumn, null, dimensionOverrideMap.asJava, useQueryLevelCache)
              val primaryColumn = queryContext.factBestCandidate.fact.publicDimToForeignKeyColMap(db.publicDim.name)
              (new ExtractionDimensionSpec(primaryColumn.alias.getOrElse(primaryColumn.name), alias, getDimValueType(column), regExFn, null), Option.empty)

            case lookupFunc@LOOKUP_WITH_EMPTY_VALUE_OVERRIDE(lookupNamespace, valueColumn, overrideValue, dimensionOverrideMap) =>
              val regExFn = new MahaRegisteredLookupExtractionFn(null, lookupNamespace, false, overrideValue, false, true, valueColumn, null, dimensionOverrideMap.asJava, useQueryLevelCache)
              val primaryColumn = queryContext.factBestCandidate.fact.publicDimToForeignKeyColMap(db.publicDim.name)
              (new ExtractionDimensionSpec(primaryColumn.alias.getOrElse(primaryColumn.name), alias, getDimValueType(column), regExFn, null), Option.empty)

            case lookupFunc@LOOKUP_WITH_DECODE(lookupNamespace, valueColumn, dimensionOverrideMap, args@_*) =>
              val regExFn = new MahaRegisteredLookupExtractionFn(null, lookupNamespace, false, lookupFunc.default.getOrElse(DruidQuery.replaceMissingValueWith), false, true, valueColumn, null, dimensionOverrideMap.asJava, useQueryLevelCache)
              val mapLookup = new MapLookupExtractor(lookupFunc.map.asJava, false)
              val mapExFn = new LookupExtractionFn(mapLookup, false, lookupFunc.default.getOrElse(null), false, true)
              val primaryColumn = queryContext.factBestCandidate.fact.publicDimToForeignKeyColMap(db.publicDim.name)
              (new ExtractionDimensionSpec(primaryColumn.alias.getOrElse(primaryColumn.name), alias, getDimValueType(column), regExFn, null),
                Option.apply(new ExtractionDimensionSpec(alias, alias, getDimValueType(column), mapExFn, null)))

            case lookupFunc@LOOKUP_WITH_DECODE_RETAIN_MISSING_VALUE(lookupNamespace, valueColumn, retainMissingValue, injective, dimensionOverrideMap, args@_*) =>
              val regExFn = new MahaRegisteredLookupExtractionFn(null, lookupNamespace, false, lookupFunc.lookupWithDecode.default.getOrElse(DruidQuery.replaceMissingValueWith), false, true, valueColumn, null, dimensionOverrideMap.asJava, useQueryLevelCache)
              val mapLookup = new MapLookupExtractor(lookupFunc.lookupWithDecode.map.asJava, false)
              val mapExFn = new LookupExtractionFn(mapLookup, retainMissingValue, null, injective, true)
              val primaryColumn = queryContext.factBestCandidate.fact.publicDimToForeignKeyColMap(db.publicDim.name)
              (new ExtractionDimensionSpec(primaryColumn.alias.getOrElse(primaryColumn.name), alias, getDimValueType(column), regExFn, null),
                Option.apply(new ExtractionDimensionSpec(alias, alias, getDimValueType(column), mapExFn, null)))

            case lookupFunc@LOOKUP_WITH_DECODE_ON_OTHER_COLUMN(lookupNamespace, columnToCheck, valueToCheck, columnIfValueMatched, columnIfValueNotMatched, dimensionOverrideMap) =>
              val decodeConfig = new DecodeConfig()
              decodeConfig.setColumnToCheck(columnToCheck)
              decodeConfig.setValueToCheck(valueToCheck)
              decodeConfig.setColumnIfValueMatched(columnIfValueMatched)
              decodeConfig.setColumnIfValueNotMatched(columnIfValueNotMatched)
              val regExFn = new MahaRegisteredLookupExtractionFn(null, lookupNamespace, false, DruidQuery.replaceMissingValueWith, false, true, columnToCheck, decodeConfig, dimensionOverrideMap.asJava, useQueryLevelCache)
              val primaryColumn = queryContext.factBestCandidate.fact.publicDimToForeignKeyColMap(db.publicDim.name)
              (new ExtractionDimensionSpec(primaryColumn.alias.getOrElse(primaryColumn.name), alias, getDimValueType(column), regExFn, null), Option.empty)

            case lookupFunc@LOOKUP_WITH_TIMEFORMATTER(lookupNamespace, valueColumn, inputFormat, resultFormat, dimensionOverrideMap, overrideValue) =>
              val regExFn = new MahaRegisteredLookupExtractionFn(null, lookupNamespace, false, overrideValue.getOrElse(DruidQuery.replaceMissingValueWith), false, true, valueColumn, null, dimensionOverrideMap.asJava, useQueryLevelCache)
              val timeFormatFn = new TimeDimExtractionFn(inputFormat, resultFormat)
              val primaryColumn = queryContext.factBestCandidate.fact.publicDimToForeignKeyColMap(db.publicDim.name)
              (new ExtractionDimensionSpec(primaryColumn.alias.getOrElse(primaryColumn.name), alias, getDimValueType(column), regExFn, null),
                Option.apply(new ExtractionDimensionSpec(alias, alias, getDimValueType(column), timeFormatFn, null)))

            case DRUID_TIME_FORMAT(fmt, zone) =>
              renderColumnWithAlias(fact, column, alias)

            case DRUID_TIME_FORMAT_WITH_PERIOD_GRANULARITY(fmt, period, zone) =>
              renderColumnWithAlias(fact, column, alias)

            case TIME_FORMAT_WITH_REQUEST_CONTEXT(fmt) =>
              renderColumnWithAlias(fact, column, alias)

            case lookupFunc@LOOKUP_WITH_TIMESTAMP(lookupNamespace, valueColumn, resultFormat, dimensionOverrideMap, overrideValue, asMillis) =>
              val regExFn = new MahaRegisteredLookupExtractionFn(null, lookupNamespace, false, overrideValue.getOrElse(DruidQuery.replaceMissingValueWith), false, true, valueColumn, null, dimensionOverrideMap.asJava, useQueryLevelCache)
              val timezoneValue = queryContext.requestModel.additionalParameters
                .getOrElse(Parameter.TimeZone, TimeZoneValue.apply(DateTimeZone.UTC.getID)).asInstanceOf[TimeZoneValue]
              val timezone = DateTimeZone.forID(timezoneValue.value)
              val timeFormatFn = new TimeFormatExtractionFn(resultFormat, timezone, null, null, asMillis)
              val primaryColumn = queryContext.factBestCandidate.fact.publicDimToForeignKeyColMap(db.publicDim.name)
              (new ExtractionDimensionSpec(primaryColumn.alias.getOrElse(primaryColumn.name), alias, getDimValueType(column), regExFn, null),
                Option.apply(new ExtractionDimensionSpec(alias, alias, getDimValueType(column), timeFormatFn, null)))

            case any =>
              throw new UnsupportedOperationException(s"Found unhandled DruidDerivedFunction : $any")
          }
        case any =>
          throw new UnsupportedOperationException(s"Found unhandled column : $any")
      }
    }

    val orFilterTypes = queryContext.requestModel.orFilterMeta.map {
      orFilterMeta => orFilterMeta.filterType
    }.toSet

    val flatOrFilterMetaFields: Set[String] = queryContext.requestModel.orFilterMeta.flatMap(orFilterMeta => orFilterMeta.orFilter.filters).map(_.field)
    queryContext.factBestCandidate.dimColMapping.foreach {
      //Add factColumn to inner query's dimensionSpecTupleList even if not in group by, as the factColumn's filter is part of outer query
      case (dimCol, alias) if !isUsingDruidLookups && factRequestCols(dimCol) || (
          (isUsingDruidLookups && queryContext.requestModel.requestColsSet(alias) && factRequestCols(dimCol)) ||
          (isUsingDruidLookups && flatOrFilterMetaFields.contains(alias) && orFilterTypes.contains(MetaType.FactType) && orFilterTypes.contains(MetaType.DimType))
        ) =>
          val column = fact.columnsByNameMap(dimCol)
          if (!column.isInstanceOf[ConstDimCol]) {
            dimensionSpecTupleList += renderColumnWithAlias(fact, column, alias)
          }
          /* local time day filter has to do with filtering by both utc time and local time filter, not rendering columns
          if (Grain.grainFields(alias) && !queryContext.factBestCandidate.publicFact.renderLocalTimeFilter) {
            //don't include time
          } else {
            if (!column.isInstanceOf[ConstDimCol]) {
              dimensionSpecTupleList += renderColumnWithAlias(fact, column, alias)
            }
          }
          */
      case _ => //do nothing
    }

    queryContext.factBestCandidate.factColMapping.foreach {
      case (factCol, alias) =>
        if (factRequestCols(factCol)) {
          val column = fact.columnsByNameMap(factCol)
          column match {
            case DruidPostResultDerivedFactCol(_, _, _, _, _, _, _, _, prf) =>
              prf.sourceColumns.foreach {
                sc => {
                  for {
                    prfColumn <- fact.dimColMap.get(sc)
                  } {
                    dimensionSpecTupleList += renderColumnWithAlias(fact, prfColumn, prfColumn.name)
                  }
                }
              }
            case _ =>
          }
        }
    }

    val dimAliasSet = new mutable.TreeSet[String]()
    val lastDimLevel: DimLevel = if (dims.nonEmpty) dims.last.dim.dimLevel else LevelOne
    dims.filter(p => p.dim.engine == DruidEngine).map {
      case (db) =>
        val aliasSet = db.fields.filterNot(f => f.equals(db.publicDim.primaryKeyByAlias) || db.publicDim.foreignKeyByAlias(f))
        aliasSet.map {
          case (alias) =>
            val name = db.publicDim.aliasToNameMap(alias)
            val column = db.dim.dimensionColumnsByNameMap(name)
            dimAliasSet += alias
            if (!column.isInstanceOf[ConstDimCol]) {
              val useQueryLevelCache: Boolean = db.dim.dimLevel < lastDimLevel
              dimensionSpecTupleList += renderColumnWithAliasUsingDimensionBundle(fact, column, alias, db, useQueryLevelCache)
            }
        }
    }

    // include dimfilter columns also in dimSpecList if not included already as it is required for filters in outer query
    dims.filter(p => p.dim.engine == DruidEngine).foreach {
      db => {
        db.filters.filterNot(f => f.field.equals(db.publicDim.primaryKeyByAlias) || db.publicDim.foreignKeyByAlias(f.field)).foreach {
          filter => {
            if (!dimAliasSet(filter.field)) {
              val name = db.publicDim.aliasToNameMap(filter.field)
              val column = db.dim.dimensionColumnsByNameMap(name)
              if (!column.isInstanceOf[ConstDimCol]) {
                val useQueryLevelCache: Boolean = db.dim.dimLevel < lastDimLevel
                dimensionSpecTupleList += renderColumnWithAliasUsingDimensionBundle(fact, column, filter.field, db, useQueryLevelCache)
              }
            }
          }
        }
      }
    }

    // include expensive dateTimeFilter columns also in dimSpecList if not included already as it is required for filters in outer query
    val hasExpensiveDateDimFilter = FilterDruid.isExpensiveDateDimFilter(queryContext.requestModel, queryContext.factBestCandidate.publicFact.aliasToNameColumnMap, queryContext.factBestCandidate.fact.columnsByNameMap)
    val name: String = queryContext.factBestCandidate.publicFact.aliasToNameColumnMap(queryContext.requestModel.localTimeDayFilter.field)
    if(!queryContext.factBestCandidate.dimColMapping.contains(name) && hasExpensiveDateDimFilter) {
      val column = queryContext.factBestCandidate.fact.columnsByNameMap(name)
      dimensionSpecTupleList += renderColumnWithAlias(fact, column, queryContext.requestModel.localTimeDayFilter.field)
    }

    dimensionSpecTupleList
  }

  private[this] def getDateTimeFilters(queryContext: FactQueryContext): Seq[DimFilter] = {
    val fact = queryContext.factBestCandidate.fact
    val filters = queryContext.factBestCandidate.filters
    val whereFilters = new ArrayBuffer[DimFilter](filters.size)
    if (queryContext.factBestCandidate.publicFact.renderLocalTimeFilter) {
      whereFilters ++= FilterDruid.renderDateDimFilters(queryContext.requestModel, queryContext.factBestCandidate.publicFact.aliasToNameColumnMap, fact.columnsByNameMap)
    }
    whereFilters
  }

  private[this] def getFilters(queryContext: FactQueryContext, dims: SortedSet[DimensionBundle]): (mutable.Buffer[DimFilter], mutable.Buffer[HavingSpec]) = {
    val fact = queryContext.factBestCandidate.fact
    val publicFact = queryContext.factBestCandidate.publicFact
    val filters = queryContext.factBestCandidate.filters
    val factForcedFilters = queryContext.factBestCandidate.publicFact.forcedFilters
    val allFilters = factForcedFilters // ++ filters need to append regular filters or pass in
    val whereFilters = new ArrayBuffer[DimFilter](filters.size)
    val havingFilters = new ArrayBuffer[HavingSpec](filters.size)

    val constantColumnNames: Set[String] = {
      fact match {
        case f: FactView =>
          f.constantColNameToValueMap.map(_._1).toSet
        case f: Fact =>
          Set.empty
      }
    }


    val unique_filters = removeDuplicateIfForced(filters.toSeq, allFilters.toSeq, queryContext)

    unique_filters.sorted.foreach {
      filter =>
        val name = publicFact.aliasToNameColumnMap(filter.field)
        val grainOption = Option(fact.grain)
        if (constantColumnNames.contains(name)) {
          // ignoring constant columns
        } else if (fact.dimColMap.contains(name)) {
          whereFilters += FilterDruid.renderFilterDim(
            filter,
            queryContext.factBestCandidate.publicFact.aliasToNameColumnMap,
            fact.columnsByNameMap, grainOption)
        } else if (fact.factColMap.contains(name)) {
          havingFilters += FilterDruid.renderFilterFact(
            filter,
            queryContext.factBestCandidate.publicFact.aliasToNameColumnMap,
            fact.columnsByNameMap
          )
        } else {
          throw new IllegalArgumentException(
            s"Unknown fact column: publicFact=${publicFact.name}, fact=${fact.name} alias=${filter.field}, name=$name")
        }
    }


    val orFilterTypes = queryContext.requestModel.orFilterMeta.map {
      orFilterMeta => orFilterMeta.filterType
    }.toSet

    queryContext.requestModel.orFilterMeta.foreach {
      orFilterMeta =>
        if (orFilterMeta.filterType.equals(MetaType.MetricType)) {
          havingFilters += FilterDruid.renderOrFactFilters(orFilterMeta.orFilter.filters,
            queryContext.factBestCandidate.publicFact.aliasToNameColumnMap,
            fact.columnsByNameMap)
        }
          //Add factType filter to inner query only if it is not a part of OR filter that has combo of both factType and dimType
        else if (orFilterMeta.filterType.equals(MetaType.FactType) && !(orFilterTypes.contains(MetaType.FactType) && orFilterTypes.contains(MetaType.DimType))){
          val cols = fact.columnsByNameMap
          val aliases = queryContext.factBestCandidate.publicFact.aliasToNameColumnMap
          whereFilters += FilterDruid.renderOrDimFilters(orFilterMeta.orFilter.filters, aliases, cols, Option(fact.grain))
          /*val cols = dims.flatMap(f => f.dim.columnsByNameMap).toMap
          val aliases = dims.flatMap(f => f.publicDim.aliasToNameMapFull)
          whereFilters += FilterDruid.renderOrDimFilters(orFilterMeta.orFilter.filters,
            aliases.toMap,
            cols, Option(fact.grain))*/
        }
    }

    (whereFilters, havingFilters)
  }

  private[this] def additionalColumns(queryContext: QueryContext): IndexedSeq[String] = {
    //TODO: add row count support for fact driven queries
    IndexedSeq.empty[String]
  }

  private[this] def ephemeralAliasColumnMap(queryContext: FactQueryContext): Map[String, Column] = {
    val aliasColumnMapFromPostResultCols = new mutable.HashMap[String, Column]()

    queryContext.factBestCandidate.fact.factColMap.foreach {
      case (name, factCol) => if (factCol.isInstanceOf[PostResultDerivedFactColumn]) {
        factCol.asInstanceOf[PostResultDerivedFactColumn].postResultFunction.sourceColumns.foreach {
          sc => {
            val column: Column = queryContext.factBestCandidate.fact.columnsByNameMap(sc)
            if (!queryContext.factBestCandidate.requestCols.contains(column.name)) {
              aliasColumnMapFromPostResultCols += (sc -> column)
            }
          }
        }
      }
    }
    aliasColumnMapFromPostResultCols.toMap
  }

}
