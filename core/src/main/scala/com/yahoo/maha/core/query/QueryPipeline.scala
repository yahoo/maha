// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core._
import com.yahoo.maha.core.bucketing.{BucketParams, BucketSelector}
import com.yahoo.maha.core.dimension.Dimension
import com.yahoo.maha.core.fact.Fact.ViewTable
import com.yahoo.maha.core.fact.{FactBestCandidate, FactView, ViewBaseTable}
import com.yahoo.maha.core.request.{AsyncRequest, SyncRequest}
import com.yahoo.maha.report.RowCSVWriterProvider
import grizzled.slf4j.Logging
import org.json4s.JValue
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{SortedSet, mutable}
import scala.util.{Failure, Try}

/**
  * Created by jians on 10/22/15.
  */
case class QueryPipelineResult(queryPipeline: QueryPipeline, queryChain: QueryChain, rowList: RowList, queryAttributes: QueryAttributes, pagination: Map[Engine, JValue])

trait QueryPipeline {
  def queryChain: QueryChain

  def fallbackQueryChainOption: Option[QueryChain]

  def requestModel: RequestModel

  def factBestCandidate: Option[FactBestCandidate]

  def bestDimCandidates: SortedSet[DimensionBundle]

  def execute(executorContext: QueryExecutorContext): Try[QueryPipelineResult]

  def execute(executorContext: QueryExecutorContext, queryAttributes: QueryAttributes): Try[QueryPipelineResult]
}

object QueryPipeline extends Logging {
  val syncDisqualifyingSet: Set[Engine] = Set(HiveEngine, PrestoEngine)
  val syncNonDruidDisqualifyingSet: Set[Engine] = Set(DruidEngine) ++ syncDisqualifyingSet
  val asyncDisqualifyingSet: Set[Engine] = Set(DruidEngine)

  val completeRowList: Query => RowList = (q) => new CompleteRowList(q)

  val dimDrivenPartialRowList: Query => RowList = (q) => {
    new DimDrivenPartialRowList(RowGrouping(q.queryContext.indexAliasOption.get, List(q.queryContext.indexAliasOption.get)), q)
  }
  val dimDrivenFactOrderedPartialRowList: Query => RowList = (q) => {
    new DimDrivenFactOrderedPartialRowList(RowGrouping(q.queryContext.indexAliasOption.get, List(q.queryContext.indexAliasOption.get)), q)}
  val factDrivenPartialRowList: Query => RowList = (q) => {
    val indexAlias = q.queryContext.indexAliasOption.get
    val groupByKeys: List[String] = q.queryContext.factGroupByKeys
    new FactDrivenPartialRowList(RowGrouping(indexAlias, groupByKeys), q)
  }

  def unionViewPartialRowList(queryList: List[Query]): Query => RowList = {
    require(queryList.forall(q => q.queryContext.isInstanceOf[FactualQueryContext] && q.queryContext.asInstanceOf[FactualQueryContext].factBestCandidate.fact.isInstanceOf[ViewBaseTable])
      , s"UnionViewPartialRowList only supports fact query context with fact views : ${
        queryList.map {
          _.queryContext.getClass.getSimpleName
        }
      }")
    val constAliasToValueMapList: List[Map[String, String]] = queryList.map {
      q =>
        val bestFact = q.queryContext.asInstanceOf[FactualQueryContext].factBestCandidate

        val constantColNameToValueMap = {
          val tempMap = new mutable.HashMap[String, String]()
          bestFact.fact.asInstanceOf[ViewBaseTable].constantColNameToValueMap.foreach {
            entry =>
              if (bestFact.publicFact.nameToAliasColumnMap.contains(entry._1)) {
                tempMap ++= bestFact.publicFact.nameToAliasColumnMap(entry._1).map(alias => alias -> entry._2).toMap
              }
          }
          tempMap.toMap
        }
        constantColNameToValueMap
    }

    val result: Query => RowList = (q) => {
      // we already checked for existance of bestCandidate
      val requestModel = q.queryContext.requestModel
      q.queryContext match {
        case f: FactQueryContext =>
          f.factBestCandidate.fact match {
            case fv: FactView =>
              val bestCandidate = f.factBestCandidate
              val compositeKey = bestCandidate.dimColMapping.values.filter(d => requestModel.requestColsSet.contains(d)).toSet
              val factAliasToDataTypeMap = bestCandidate.factColMapping.map(entry => entry._2 -> bestCandidate.fact.factColMap(entry._1).dataType)
              new UnionViewRowList(compositeKey, q, factAliasToDataTypeMap, constAliasToValueMapList)
            case any =>
              throw new UnsupportedOperationException(s"Cannot create UnionViewRowList with fact of type : ${any.getClass.getSimpleName}")
          }
        case DimQueryContext(_, _, _, _, _) =>
          throw new IllegalArgumentException(s"Requested UnionViewRowList in DimQueryContext")
        case _ =>
          throw new IllegalArgumentException(s"Requested UnionViewRowList in Unhandled/UnknownQueryContext")
      }
    }
    result
  }


  def csvRowList(csvWriterProvider: RowCSVWriterProvider, writeHeader: Boolean): Query => RowList = (q) =>
    new CSVRowList(q, csvWriterProvider, writeHeader)
}

object QueryPipelineWithFallback {
  private val logger = LoggerFactory.getLogger(classOf[QueryPipelineWithFallback])
}

case class QueryPipelineWithFallback(queryChain: QueryChain
                                     , factBestCandidate: Option[FactBestCandidate]
                                     , bestDimCandidates: SortedSet[DimensionBundle]
                                     , rowListFn: Query => RowList
                                     , fallbackQueryChain: QueryChain
                                     , fallbackRowListFn: Query => RowList) extends QueryPipeline {

  val fallbackQueryChainOption: Option[QueryChain] = Option(fallbackQueryChain)

  def execute(executorContext: QueryExecutorContext): Try[QueryPipelineResult] = {
    val stats = new EngineQueryStats
    Try {
      val result = queryChain.execute(executorContext, rowListFn, QueryAttributes.empty, stats)
      QueryPipelineResult(this, queryChain, result.rowList, result.queryAttributes, result.pagination)
    }.recover {
      case throwable =>
        QueryPipelineWithFallback.logger.error("Primary query chain failed, recovering with fall back", throwable)
        val result = fallbackQueryChain.execute(executorContext, fallbackRowListFn, QueryAttributes.empty, stats)
        QueryPipelineResult(this, fallbackQueryChain, result.rowList, result.queryAttributes, result.pagination)
    }
  }

  def execute(executorContext: QueryExecutorContext, queryAttributes: QueryAttributes): Try[QueryPipelineResult] = {
    val stats = new EngineQueryStats
    Try {
      val result = queryChain.execute(executorContext, rowListFn, queryAttributes, stats)
      QueryPipelineResult(this, queryChain, result.rowList, result.queryAttributes, result.pagination)
    }.recover {
      case throwable =>
        QueryPipelineWithFallback.logger.error("Primary query chain failed, recovering with fall back", throwable)
        val result = fallbackQueryChain.execute(executorContext, fallbackRowListFn, queryAttributes, stats)
        QueryPipelineResult(this, fallbackQueryChain, result.rowList, result.queryAttributes, result.pagination)
    }
  }

  override def requestModel: RequestModel = queryChain.drivingQuery.queryContext.requestModel
}

case class DefaultQueryPipeline(queryChain: QueryChain
                                , factBestCandidate: Option[FactBestCandidate]
                                , bestDimCandidates: SortedSet[DimensionBundle]
                                , rowListFn: Query => RowList) extends QueryPipeline {

  val fallbackQueryChainOption: Option[QueryChain] = None

  def execute(executorContext: QueryExecutorContext): Try[QueryPipelineResult] = {
    execute(executorContext, QueryAttributes.empty)
  }

  def execute(executorContext: QueryExecutorContext, queryAttributes: QueryAttributes): Try[QueryPipelineResult] = {
    val stats = new EngineQueryStats
    Try {
      val result = queryChain.execute(executorContext, rowListFn, queryAttributes, stats)
      QueryPipelineResult(this, queryChain, result.rowList, result.queryAttributes, result.pagination)
    }
  }

  override def requestModel: RequestModel = queryChain.drivingQuery.queryContext.requestModel
}

object QueryPipelineBuilder {
  val logger = LoggerFactory.getLogger(classOf[QueryPipelineBuilder])
}

class QueryPipelineBuilder(queryChain: QueryChain
                           , factBestCandidate: Option[FactBestCandidate]
                           , bestDimCandidates: SortedSet[DimensionBundle]
                           , debugEnabled: Boolean
                          ) {

  import QueryPipelineBuilder._

  private[this] var rowListFn: Query => RowList = QueryPipeline.completeRowList
  private[this] var fallbackRowListFn: Option[Query => RowList] = None
  private[this] var fallbackQueryChain: Option[QueryChain] = None

  def requestDebug(msg: => String): Unit = {
    if (debugEnabled) {
      logger.info(msg)
    }
  }

  def withRowListFunction(fn: Query => RowList): QueryPipelineBuilder = {
    require(fn != null, "Undefined function Query => RowList")
    rowListFn = fn
    this
  }

  def withFallbackRowListFunction(fn: Query => RowList): QueryPipelineBuilder = {
    require(fn != null, "Undefined function Query => RowList")
    fallbackRowListFn = Option(fn)
    this
  }

  def withFallbackQueryChain(fb: QueryChain): QueryPipelineBuilder = {
    require(fb != null, "Undefined QueryChain")
    fallbackQueryChain = Option(fb)
    this
  }

  def build(): QueryPipeline = {
    if (fallbackQueryChain.isDefined) {
      requestDebug("fallbackQueryChain defined")
      new QueryPipelineWithFallback(
        queryChain, factBestCandidate, bestDimCandidates, rowListFn, fallbackQueryChain.get, fallbackRowListFn.getOrElse(QueryPipeline.completeRowList))
    } else {
      requestDebug("fallbackQueryChain not defined")
      new DefaultQueryPipeline(queryChain, factBestCandidate, bestDimCandidates, rowListFn)
    }
  }
}

class EngineQueryStats {
  private[this] val statsList: ArrayBuffer[EngineQueryStat] = new ArrayBuffer(5)

  def addStat(stat: EngineQueryStat): Unit = {
    statsList += stat
  }

  def getStats: IndexedSeq[EngineQueryStat] = {
    statsList.toIndexedSeq
  }
}

case class EngineQueryStat(engine: Engine, startTime: Long, endTime: Long, tableName: String)

case class QueryChainExecutionResult(rowList: RowList, queryAttributes: QueryAttributes, pagination: Map[Engine, JValue])

sealed trait QueryChain {
  def drivingQuery: Query

  def execute(executorContext: QueryExecutorContext, rowListFn: Query => RowList, queryAttributes: QueryAttributes, engineQueryStats: EngineQueryStats): QueryChainExecutionResult

  def subsequentQueryList: IndexedSeq[Query]
}

object QueryChain {
  val logger: Logger = LoggerFactory.getLogger(classOf[QueryChain])
}

case class SingleEngineQuery(drivingQuery: Query) extends QueryChain {
  val subsequentQueryList: IndexedSeq[Query] = IndexedSeq.empty

  def execute(executorContext: QueryExecutorContext, rowListFn: Query => RowList, queryAttributes: QueryAttributes, engineQueryStats: EngineQueryStats): QueryChainExecutionResult = {
    val executor = executorContext.getExecutor(drivingQuery.engine)
    val drivingQueryStartTime = System.currentTimeMillis()
    val rowList = rowListFn(drivingQuery)
    rowList.withLifeCycle {
      val result = executor.fold(
        throw new IllegalArgumentException(s"Executor not found for engine=${drivingQuery.engine}, query=${drivingQuery.getClass.getSimpleName}")
      )(_.execute(drivingQuery, rowList, queryAttributes))
      val drivingQueryEndTime = System.currentTimeMillis()
      val queryStats = EngineQueryStat(drivingQuery.engine, drivingQueryStartTime, drivingQueryEndTime, drivingQuery.tableName)
      engineQueryStats.addStat(queryStats)
      result.requireSuccess("query execution failed")
      QueryChainExecutionResult(result.rowList
        , result.queryAttributes.toBuilder.addAttribute(QueryAttributes.QueryStats, QueryStatsAttribute(engineQueryStats)).build
        , result.pagination.fold(Map.empty[Engine, JValue])(json => Map(drivingQuery.engine -> json))
      )
    }
  }
}

/*
    MultiEngineQuery usage : MultiEngine Query is used to generate the subsequent queries based on the driving result
    For Example: Driving Query Result as Druid Engine will be used to generate the subsequent Oracle Queries
 */
case class MultiEngineQuery(drivingQuery: Query,
                            engines: Set[Engine], subsequentQueries: IndexedSeq[(IndexedRowList, QueryAttributes) => Query],
                            fallbackQueryOption: Option[(Query, RowList)] = None) extends QueryChain {
  require(subsequentQueries.nonEmpty, "MultiEngineQuery must have > 1 subsequent queries!")
  require(drivingQuery.queryContext.indexAliasOption.isDefined, "Driving query must have index alias defined!")

  val subQueryList = new ArrayBuffer[Query]()

  def subsequentQueryList: IndexedSeq[Query] = subQueryList.toIndexedSeq

  def execute(executorContext: QueryExecutorContext, rowListFn: Query => RowList, queryAttributes: QueryAttributes, engineQueryStats: EngineQueryStats): QueryChainExecutionResult = {
    val executorsMap = engines.map { engine =>
      val executor = executorContext.getExecutor(engine)
      require(executor.isDefined, s"Executor not found for engine=$engine")
      engine -> executor.get
    }.toMap
    val rowList = rowListFn(drivingQuery)

    require(rowList.isInstanceOf[IndexedRowList], "Multi Engine Query requires an indexed row list!")

    val result = rowList.withLifeCycle {
      val drivingQueryStartTime = System.currentTimeMillis()
      val drivingResult = executorsMap(drivingQuery.engine).execute(drivingQuery, rowList.asInstanceOf[IndexedRowList], queryAttributes)
      val drivingQueryEndTime = System.currentTimeMillis()
      engineQueryStats.addStat(EngineQueryStat(drivingQuery.engine, drivingQueryStartTime, drivingQueryEndTime, drivingQuery.tableName))
      if (drivingResult.isFailure || drivingResult.rowList.keys.isEmpty) drivingResult else subsequentQueries.foldLeft(drivingResult) {
        (previousResult, subsequentQuery) =>
          val query = subsequentQuery(previousResult.rowList, previousResult.queryAttributes)
          query match {
            case NoopQuery =>
              previousResult
            case _ =>
              subQueryList += query
              val queryStartTime = System.currentTimeMillis()
              val result = executorsMap(query.engine).execute(query, previousResult.rowList, previousResult.queryAttributes)
              val queryEndTime = System.currentTimeMillis()
              engineQueryStats.addStat(EngineQueryStat(query.engine, queryStartTime, queryEndTime, query.tableName))
              result
          }
      }
    }

    // result._1.isEmpty returns of the updatedRows.isEmpty
    if ((result.isFailure || result.rowList.isUpdatedRowListEmpty) && fallbackQueryOption.isDefined) {
      QueryChain.logger.info("No data from driving query, running fall back query")
      val (query, rowList) = fallbackQueryOption.get
      subQueryList += query
      rowList.withLifeCycle {
        val queryStartTime = System.currentTimeMillis()
        val newResult = executorsMap(query.engine).execute(query, rowList, result.queryAttributes)
        val queryEndTime = System.currentTimeMillis()
        engineQueryStats.addStat(EngineQueryStat(query.engine, queryStartTime, queryEndTime, query.tableName))
        newResult.requireSuccess("fallback query execution failed")
        QueryChainExecutionResult(newResult.rowList
          , newResult.queryAttributes.toBuilder.addAttribute(QueryAttributes.QueryStats, QueryStatsAttribute(engineQueryStats)).build
          , newResult.pagination.fold(Map.empty[Engine, JValue])(json => Map(query.engine -> json))
        )
      }
    } else {
      result.requireSuccess("query execution failed")
      QueryChainExecutionResult(result.rowList
        , result.queryAttributes.toBuilder.addAttribute(QueryAttributes.QueryStats, QueryStatsAttribute(engineQueryStats)).build
        , result.pagination.fold(Map.empty[Engine, JValue])(json => Map(drivingQuery.engine -> json))
      )
    }
  }
}

/*
 MultiQuery usage: MultiQuery can be used to execute the list if queries irrespective of Engine
  and it merged all the results into one rowList
 */
case class MultiQuery(unionQueryList: List[Query], fallbackQueryOption: Option[(Query, RowList)] = None) extends QueryChain {
  // Driving query is not actually driving the result here
  val drivingQuery: Query = unionQueryList.head
  val subsequentQueryList: IndexedSeq[Query] = unionQueryList.drop(1).toIndexedSeq

  def execute(executorContext: QueryExecutorContext, rowListFn: Query => RowList, queryAttributes: QueryAttributes, engineQueryStats: EngineQueryStats): QueryChainExecutionResult = {
    unionQueryList.foreach {
      q =>
        val executorOption = executorContext.getExecutor(q.engine)
        require(executorOption.isDefined, s"Executor not found for engine=${q.engine}, query=$q")
    }
    val firstQuery = drivingQuery
    val executorOption = executorContext.getExecutor(firstQuery.engine)
    val executor = executorOption.get
    val drivingQueryStartTime = System.currentTimeMillis()
    val rowList = rowListFn(firstQuery)

    val result = rowList.withLifeCycle {
      val firstResult = executor.execute(firstQuery, rowList, queryAttributes) //it is just first seed to MultiQuery
      val drivingQueryEndTime = System.currentTimeMillis()
      engineQueryStats.addStat(EngineQueryStat(firstQuery.engine, drivingQueryStartTime, drivingQueryEndTime, firstQuery.tableName))

      if (firstResult.isFailure) firstResult else {
        unionQueryList.filter(q => q != firstQuery).foldLeft(firstResult) {
          (previousResult, subsequentQuery) =>
            val query = subsequentQuery
            query match {
              case NoopQuery =>
                previousResult
              case _ =>
                if (previousResult.isFailure) previousResult else {
                  val subsequentQueryStartTime = System.currentTimeMillis()

                  val result = {
                    //Step to trigger the use of the next ConstAliasToValueMap out of the list created at init
                    previousResult.rowList.nextStage()
                    executor.execute(query, previousResult.rowList, previousResult.queryAttributes)
                  }
                  val subsequentQueryEndTime = System.currentTimeMillis()
                  engineQueryStats.addStat(EngineQueryStat(query.engine, subsequentQueryStartTime, subsequentQueryEndTime, query.tableName))
                  result
                }
            }
        }
      }
    }

    if ((result.isFailure || result.rowList.isEmpty) && fallbackQueryOption.isDefined) {
      QueryChain.logger.info("No data from driving query, running fall back query for MultiQuery")
      val (query, rowList) = fallbackQueryOption.get
      rowList.withLifeCycle {
        val queryStartTime = System.currentTimeMillis()
        val executorOption = executorContext.getExecutor(query.engine)
        val executor = executorOption.get
        val newResult = executor.execute(query, rowList, result.queryAttributes)
        val queryEndTime = System.currentTimeMillis()
        engineQueryStats.addStat(EngineQueryStat(query.engine, queryStartTime, queryEndTime, query.tableName))
        newResult.requireSuccess("fallback query execution failed")
        QueryChainExecutionResult(newResult.rowList
          , newResult.queryAttributes.toBuilder.addAttribute(QueryAttributes.QueryStats, QueryStatsAttribute(engineQueryStats)).build
          , newResult.pagination.fold(Map.empty[Engine, JValue])(json => Map(query.engine -> json))
        )
      }
    } else {
      result.requireSuccess("query execution failed")
      QueryChainExecutionResult(result.rowList
        , result.queryAttributes.toBuilder.addAttribute(QueryAttributes.QueryStats, QueryStatsAttribute(engineQueryStats)).build
        , result.pagination.fold(Map.empty[Engine, JValue])(json => Map(drivingQuery.engine -> json))
      )
    }
  }
}

trait QueryPipelineFactory {

  def from(requestModel: RequestModel, queryAttributes: QueryAttributes, bucketParams: BucketParams): Try[QueryPipeline]

  def fromQueryGenVersion(requestModel: RequestModel, queryAttributes: QueryAttributes, queryGenVersion: Version): Try[QueryPipeline]

  def from(requestModels: Tuple2[RequestModel, Option[RequestModel]], queryAttributes: QueryAttributes): Tuple2[Try[QueryPipeline], Option[Try[QueryPipeline]]]

  def fromBucketSelector(requestModels: Tuple2[RequestModel, Option[RequestModel]], queryAttributes: QueryAttributes, bucketSelector: BucketSelector, bucketParams: BucketParams): Tuple3[Try[QueryPipeline], Option[Try[QueryPipeline]], Option[Try[QueryPipeline]]]

  def fromBucketSelector(requestModel: RequestModel, queryAttributes: QueryAttributes, bucketSelector: BucketSelector, bucketParams: BucketParams): Tuple2[Try[QueryPipeline], Option[Try[QueryPipeline]]]

  def builder(requestModel: RequestModel, queryAttributes: QueryAttributes): Try[QueryPipelineBuilder]

  def builder(requestModel: RequestModel, queryAttributes: QueryAttributes, bucketSelector: Option[BucketSelector], bucketParams: BucketParams, fallbackDisqualifySet: Set[Engine]): Tuple2[Try[QueryPipelineBuilder], Option[Try[QueryPipelineBuilder]]]

  def builder(requestModels: Tuple2[RequestModel, Option[RequestModel]], queryAttributes: QueryAttributes): Tuple2[Try[QueryPipelineBuilder], Option[Try[QueryPipelineBuilder]]]

  def builder(requestModels: Tuple2[RequestModel, Option[RequestModel]], queryAttributes: QueryAttributes, bucketSelector: BucketSelector, bucketParams: BucketParams): Tuple3[Try[QueryPipelineBuilder], Option[Try[QueryPipelineBuilder]], Option[Try[QueryPipelineBuilder]]]

  protected def findBestFactCandidate(requestModel: RequestModel, forceDisqualifySet: Set[Engine] = Set.empty, dimEngines: Set[Engine]): FactBestCandidate

  protected def findBestDimCandidates(requestModel: RequestModel, factEngine: Engine, dimMapping: Map[String, SortedSet[DimensionBundle]]): SortedSet[DimensionBundle]

}

object DefaultQueryPipelineFactory extends Logging {

  val druidMultiQueryEngineList: Seq[Engine] = List(OracleEngine, PostgresEngine)

  private[this] def aLessThanBByLevelAndCostAndCardinality(a: (String, Engine, Long, Int, Int), b: (String, Engine, Long, Int, Int)): Boolean = {
    if (a._2 == b._2) {
      if (a._4 == b._4) {
        a._3 < b._3
      } else {
        a._4 < b._4
      }
    } else {
      if (a._5 == b._5) {
        a._3 < b._3
      } else {
        a._5 < b._5
      }
    }
  }

  def findBestFactCandidate(requestModel: RequestModel, forceDisqualifySet: Set[Engine] = Set.empty, dimEngines: Set[Engine], queryGeneratorRegistry: QueryGeneratorRegistry): FactBestCandidate = {
    require(requestModel.bestCandidates.isDefined, s"Cannot create fact best candidate without best candidates : ${requestModel.reportingRequest}")
    //schema specific rules?
    requestModel.schema match {
      case _ =>
        //do we want to force engine?
        val forceEngine = requestModel.forceQueryEngine.filterNot(forceDisqualifySet)

        //do not use druid if we have both dim and fact operations
        val disqualifySet: Set[Engine] = {

          if (!requestModel.outerFilters.isEmpty) {
            if (requestModel.isDebugEnabled) info(s"Outer filters are supported by Oracle Engine and Postgres Engine, Adding Druid, Hive to disqualifySet")
            QueryPipeline.syncNonDruidDisqualifyingSet
          } else {
            //is fact query with filter on dim where the dim's primary key is not in the request cols
            if (requestModel.isSyncRequest) {
              val hasIndexInOutput = requestModel.dimensionsCandidates.forall(d => requestModel.requestColsSet(d.dim.primaryKeyByAlias))

              //disqualify druid for a bunch of reasons, need to better codify this, it is messy
              if (dimEngines(DruidEngine)) {
                QueryPipeline.syncDisqualifyingSet ++ forceDisqualifySet
              } else if ((requestModel.hasLowCardinalityDimFilters && (requestModel.forceDimDriven || !dimEngines(DruidEngine)) && requestModel.hasDimAndFactOperations)
                || (!requestModel.forceDimDriven && requestModel.isFactDriven && !hasIndexInOutput && !dimEngines.contains(DruidEngine)) //this does not apply if druid is the only dim candidate
              ) {
                if (requestModel.isDebugEnabled) {
                  info(s"hasLowCardinalityDimFilters=${requestModel.hasLowCardinalityDimFilters} forceDimDriven=${requestModel.forceDimDriven} hasDimAndFactOperations=${requestModel.hasDimAndFactOperations}")
                  info(s"isFactDriven=${requestModel.isFactDriven} hasIndexInOutput=$hasIndexInOutput dimEngines=$dimEngines dimCandidates=${requestModel.dimensionsCandidates.map(_.dim.name)}")
                }
                QueryPipeline.syncNonDruidDisqualifyingSet ++ forceDisqualifySet
              } else if (requestModel.hasFactSortBy
                || (requestModel.hasDimSortBy && !requestModel.hasNonFKFactFilters)
                || (!requestModel.hasDimAndFactOperations)
                || dimEngines.contains(DruidEngine)) {
                QueryPipeline.syncDisqualifyingSet ++ forceDisqualifySet
              } else {
                QueryPipeline.syncNonDruidDisqualifyingSet ++ forceDisqualifySet
              }
            } else {
              // Allow Druid in Async only if
              // (i)  explicitly enabled through asyncDruidEnabled
              // (ii) request does not have any dim filter/field
              // (iii) explicitly enabled through asyncDruidEnabled and asyncDruidLookupEnabled
              if (dimEngines.contains(DruidEngine) || !(requestModel.hasDimFilters || requestModel.requestCols.filter(c => c.isInstanceOf[DimColumnInfo]).size > 0)) {
                if (requestModel.isDebugEnabled) {
                  info(s"isFactDriven=${requestModel.isFactDriven} forceDisqualifySet=$forceDisqualifySet dimEngines=$dimEngines")
                  info(s"hasDimFilters=${requestModel.hasDimFilters} requestCols=${requestModel.requestCols.filter(c => c.isInstanceOf[DimColumnInfo])}")
                }
                (QueryPipeline.asyncDisqualifyingSet -- Set(DruidEngine)) ++ forceDisqualifySet
              } else {
                QueryPipeline.asyncDisqualifyingSet ++ forceDisqualifySet
              }
            }
          }
        }

        if (requestModel.isDebugEnabled) {
          info(s"disqualifySet = $disqualifySet")
        }
        val result: IndexedSeq[(String, Engine, Long, Int, Int)] = requestModel.factCost.toIndexedSeq.collect {
          case ((fn, engine), rowcost) if (!queryGeneratorRegistry.getDefaultGenerator(engine).isDefined || queryGeneratorRegistry.getDefaultGenerator(engine).get.validateEngineConstraints(requestModel)) && (forceEngine.contains(engine) || (!disqualifySet(engine) && forceEngine.isEmpty)) =>
            val fact = requestModel.bestCandidates.get.facts(fn).fact
            val level = fact.level
            val dimCardinality: Long = requestModel.dimCardinalityEstimate.getOrElse(fact.defaultCardinality)
            val dimCardinalityPreference = requestModel.bestCandidates.get.publicFact.dimCardinalityEnginePreference(dimCardinality).get(requestModel.requestType).flatMap(_.get(engine)).getOrElse(Int.MaxValue)
            if (requestModel.isDebugEnabled) {
              info(s"fn=$fn engine=$engine cost=${rowcost.costEstimate} level=$level cardinalityPreference=$dimCardinalityPreference")
            }
            (fn, engine, rowcost.costEstimate, level, dimCardinalityPreference)
        }.sortWith(aLessThanBByLevelAndCostAndCardinality)
        require(result.nonEmpty,
          s"Failed to find best candidate, forceEngine=$forceEngine, engine disqualifyingSet=$disqualifySet, candidates=${requestModel.bestCandidates.get.facts.mapValues(_.fact.engine).toSet}")
        requestModel.bestCandidates.get.getFactBestCandidate(result.head._1, requestModel)
    }
  }

  def findBestDimCandidates(factEngine: Engine, requestModel: RequestModel, dimMapping: Map[String, SortedSet[DimensionBundle]], druidMultiQueryEngineList: Seq[Engine]): SortedSet[DimensionBundle] = {
    val schema: Schema = requestModel.schema
    val bestDimensionCandidates = new mutable.TreeSet[DimensionBundle]
    //we special case druid for multi engine support
    if (factEngine == DruidEngine) {
      val iter = dimMapping.iterator
      while (iter.hasNext) {
        val (name, bundles) = iter.next()
        //find druid first
        var dc = bundles.filter(_.dim.engine == DruidEngine)
        //find supported druid multi engine next
        if (dc.isEmpty) {
          druidMultiQueryEngineList.exists {
            engine =>
              dc = bundles.filter(_.dim.engine == engine)
              dc.nonEmpty
          }
        }
        if (dc.isEmpty) {
          //if no candidate found, we return empty set
          warn(s"No concrete dimension found for factEngine=$factEngine, schema=$schema, dim=$name")
          return SortedSet.empty
        }
        bestDimensionCandidates += dc.head

        // if forceDimDriven and factEngine is Druid then add other Engines which are part of MultiQueryEngineList
        if(requestModel.isSyncRequest && requestModel.forceDimDriven) {
          druidMultiQueryEngineList.map {
            engine =>
              val dc = bundles.filter(_.dim.engine == engine)
              if(dc.nonEmpty)
                bestDimensionCandidates += dc.head
          }
        }

      }
    } else {
      val iter = dimMapping.iterator
      while (iter.hasNext) {
        val (name, bundles) = iter.next()
        var dc = bundles.filter(_.dim.engine == factEngine)
        if (dc.isEmpty) {
          //if no candidate found, we return empty set
          warn(s"No concrete dimension found for factEngine=$factEngine, schema=$schema, dim=$name")
          return SortedSet.empty
        }
        bestDimensionCandidates += dc.head
      }
    }
    bestDimensionCandidates
  }

  def findDimCandidatesMapping(requestModel: RequestModel): Map[String, SortedSet[DimensionBundle]] = {
    val dimCandidates = requestModel.dimensionsCandidates
    val schema = requestModel.schema
    val engineSet: IndexedSeq[Engine] = Engine.engines

    val publicDimToConcreteDimMap: Map[(String, Engine), Dimension] = {
      val dimMap = new mutable.HashMap[(String, Engine), Dimension]
      dimCandidates.foreach {
        dc =>
          val allDimColumns = dc.fields ++ dc.filters.map(_.field)
          engineSet.map { engine =>
            val dimOption = dc.dim.forColumns(engine, schema, allDimColumns)
            if (dimOption.isDefined) {
              val dim = dimOption.get
              //check if the dimension is within supported max days lookback
              val withinMaxDaysLookBackOption = {
                val maxDaysLookBack = dim.maxDaysLookBack.map(_.getOrElse(requestModel.requestType, 0)).getOrElse(Int.MaxValue)
                requestModel.requestedDaysLookBack <= maxDaysLookBack
              }
              if (withinMaxDaysLookBackOption) {
                dimMap += ((dc.dim.name, engine) -> dimOption.get)
              }
            }
          }
      }
      dimMap.toMap
    }

    val publicDimToConcreteDimMapWhichCanSupportAllDim = new mutable.HashMap[(String, Engine), Dimension]()
    publicDimToConcreteDimMap
      .groupBy(key => key._1._2)
      .filter(row => row._2.size == publicDimToConcreteDimMap.keySet.map(_._1).size)
      .map(_._2)
      .foreach(_.foreach(publicDimToConcreteDimMapWhichCanSupportAllDim += _))

    val bestDimCandidatesMapping = new mutable.HashMap[String, SortedSet[DimensionBundle]]()

    def processCombination(upperCandidates: List[Dimension], lowerCandidates: List[Dimension], dc: DimensionCandidate
                           , engine: Engine, upper_dim_engine: Engine, lower_dim_engine: Engine) : Unit = {
      if (upperCandidates.size == dc.upperCandidates.size
        && lowerCandidates.size == dc.lowerCandidates.size) {
        val publicUpperCandidatesMap = dc.upperCandidates.map(pd => publicDimToConcreteDimMapWhichCanSupportAllDim((pd.name, upper_dim_engine)).name -> pd).toMap
        val publicLowerCandidatesMap = dc.lowerCandidates.map(pd => publicDimToConcreteDimMapWhichCanSupportAllDim((pd.name, lower_dim_engine)).name -> pd).toMap
        val concreteDimension = publicDimToConcreteDimMapWhichCanSupportAllDim((dc.dim.name, engine))
        bestDimCandidatesMapping(dc.dim.name) += DimensionBundle(
          concreteDimension
          , dc.dim
          , dc.fields
          , dc.filters
          , upperCandidates
          , publicUpperCandidatesMap
          , lowerCandidates
          , publicLowerCandidatesMap
          , dc.dim.partitionColumns.map(pubCol => pubCol.alias -> concreteDimension.dimensionColumnsByNameMap(pubCol.name)).toMap
          , isDrivingDimension = dc.isDrivingDimension
          , hasNonFKOrForcedFilters = dc.hasNonFKOrForcedFilters
          , hasNonFKSortBy = dc.hasNonFKSortBy
          , hasNonPushDownFilters = dc.hasNonPushDownFilters
          , hasPKRequested = dc.hasPKRequested
          , hasNonFKNonForceFilters = dc.hasNonFKNonForceFilters
          , hasLowCardinalityFilter = dc.hasLowCardinalityFilter
        )
      }
    }
    dimCandidates.foreach {
      dc =>
        bestDimCandidatesMapping += (dc.dim.name -> SortedSet[DimensionBundle]())
        engineSet.foreach {
          engine =>
            if (publicDimToConcreteDimMapWhichCanSupportAllDim.contains(dc.dim.name, engine)) {
              var upper_dim_engine = engine
              var lower_dim_engine = engine
              var upperCandidates = dc.upperCandidates.flatMap(pd => publicDimToConcreteDimMapWhichCanSupportAllDim.get((pd.name, upper_dim_engine)))
              var lowerCandidates = dc.lowerCandidates.flatMap(pd => publicDimToConcreteDimMapWhichCanSupportAllDim.get((pd.name, lower_dim_engine)))
              if (engine == DruidEngine && (upperCandidates.isEmpty || lowerCandidates.isEmpty)) {
                if (lowerCandidates.nonEmpty) {
                  druidMultiQueryEngineList.foreach {
                    multiQueryEngine =>
                      upper_dim_engine = multiQueryEngine
                      upperCandidates = dc.upperCandidates.flatMap(pd => publicDimToConcreteDimMapWhichCanSupportAllDim.get((pd.name, upper_dim_engine)))
                      processCombination(upperCandidates, lowerCandidates, dc, engine, upper_dim_engine, lower_dim_engine)
                  }
                } else if (upperCandidates.nonEmpty) {
                  druidMultiQueryEngineList.foreach {
                    multiQueryEngine =>
                      lower_dim_engine = multiQueryEngine
                      lowerCandidates = dc.lowerCandidates.flatMap(pd => publicDimToConcreteDimMapWhichCanSupportAllDim.get((pd.name, lower_dim_engine)))
                      processCombination(upperCandidates, lowerCandidates, dc, engine, upper_dim_engine, lower_dim_engine)
                  }
                } else {
                  druidMultiQueryEngineList.foreach {
                    multiQueryEngine =>
                      upper_dim_engine = multiQueryEngine
                      upperCandidates = dc.upperCandidates.flatMap(pd => publicDimToConcreteDimMapWhichCanSupportAllDim.get((pd.name, upper_dim_engine)))
                      lower_dim_engine = multiQueryEngine
                      lowerCandidates = dc.lowerCandidates.flatMap(pd => publicDimToConcreteDimMapWhichCanSupportAllDim.get((pd.name, lower_dim_engine)))
                      processCombination(upperCandidates, lowerCandidates, dc, engine, upper_dim_engine, lower_dim_engine)
                  }
                }
              } else {
                processCombination(upperCandidates, lowerCandidates, dc, engine, upper_dim_engine, lower_dim_engine)
              }
            }
        }
    }
    bestDimCandidatesMapping.toMap
  }
}

case class QueryPipelineContext(offHeapRowListConfigOption: Option[OffHeapRowListConfig] = None)

class DefaultQueryPipelineFactory(defaultFactEngine: Engine = OracleEngine, druidMultiQueryEngineList: Seq[Engine] = DefaultQueryPipelineFactory.druidMultiQueryEngineList, queryPipelineContext: QueryPipelineContext = QueryPipelineContext())(implicit val queryGeneratorRegistry: QueryGeneratorRegistry) extends QueryPipelineFactory with Logging {

  private[this] def getDimOnlyQuery(bestDimCandidates: SortedSet[DimensionBundle], requestModel: RequestModel, queryGenVersion: Version): Query = {
    val dimOnlyContextBuilder = QueryContext
      .newQueryContext(DimOnlyQuery, requestModel)
      .addDimTable(bestDimCandidates)
    require(bestDimCandidates.nonEmpty, "Cannot generate dim only query with no best dim candidates!")
    require(queryGeneratorRegistry.isEngineRegistered(bestDimCandidates.head.dim.engine, Option(queryGenVersion))
      , s"Failed to find query generator for engine : ${bestDimCandidates.head.dim.engine}")
    queryGeneratorRegistry.getValidGeneratorForVersion(bestDimCandidates.head.dim.engine, queryGenVersion, Option(requestModel)).get.generate(dimOnlyContextBuilder.build())
  }

  private[this] def getMultiEngineDimQuery(bestDimCandidates: SortedSet[DimensionBundle],
                                           requestModel: RequestModel,
                                           indexAlias: String,
                                           factGroupByKeys: List[String],
                                           queryAttributes: QueryAttributes,
                                           queryGenVersion: Version): Query = {
    val dimOnlyContextBuilder = QueryContext
      .newQueryContext(DimOnlyQuery, requestModel)
      .addDimTable(bestDimCandidates)
      .addIndexAlias(indexAlias)
      .addFactGroupByKeys(factGroupByKeys)
      .setQueryAttributes(queryAttributes)

    require(queryGeneratorRegistry.isEngineRegistered(bestDimCandidates.head.dim.engine, Option(queryGenVersion))
      , s"Failed to find query generator for engine : ${bestDimCandidates.head.dim.engine}")
    queryGeneratorRegistry.getValidGeneratorForVersion(bestDimCandidates.head.dim.engine, queryGenVersion, Option(requestModel)).get.generate(dimOnlyContextBuilder.build())
  }

  private[this] def getFactQuery(bestFactCandidate: FactBestCandidate, requestModel: => RequestModel, indexAlias: String, factGroupByKeys: List[String], queryGenVersion: Version, bestDimCandidates: Option[SortedSet[DimensionBundle]] = None): Query = {
    val factOnlyContextBuilder = QueryContext
      .newQueryContext(FactOnlyQuery, requestModel)
      .addFactBestCandidate(bestFactCandidate)
      .addIndexAlias(indexAlias)
      .addFactGroupByKeys(factGroupByKeys)
      .addDimTable(bestDimCandidates.getOrElse(SortedSet.empty[DimensionBundle]))
    require(queryGeneratorRegistry.isEngineRegistered(bestFactCandidate.fact.engine, Some(queryGenVersion))
      , s"Failed to find query generator for engine : ${bestFactCandidate.fact.engine}")
    queryGeneratorRegistry.getValidGeneratorForVersion(bestFactCandidate.fact.engine, queryGenVersion, Option(requestModel)).get.generate(factOnlyContextBuilder.build())
  }

  private[this] def getDimFactQuery(bestDimCandidates: SortedSet[DimensionBundle],
                                    bestFactCandidate: FactBestCandidate,
                                    requestModel: RequestModel,
                                    queryAttributes: QueryAttributes,
                                    queryGenVersion: Version): Query = {
    val queryType = if (isOuterGroupByQuery(bestDimCandidates, bestFactCandidate, requestModel)) {
      DimFactOuterGroupByQuery
    } else DimFactQuery

    val dimFactContext = QueryContext
      .newQueryContext(queryType, requestModel)
      .addDimTable(bestDimCandidates)
      .addFactBestCandidate(bestFactCandidate)
      .setQueryAttributes(queryAttributes)
      .build()

    require(queryGeneratorRegistry.isEngineRegistered(bestFactCandidate.fact.engine, Option(queryGenVersion))
      , s"No query generator registered for engine ${bestFactCandidate.fact.engine} for fact ${bestFactCandidate.fact.name}")
    queryGeneratorRegistry.getValidGeneratorForVersion(bestFactCandidate.fact.engine, queryGenVersion, Option(requestModel)).get.generate(dimFactContext)
  }

  private[this] def isOuterGroupByQuery(bestDimCandidates: SortedSet[DimensionBundle],
                                        bestFactCandidate: FactBestCandidate,
                                        requestModel: RequestModel): Boolean = {
    /*
OuterGroupBy operation has to be applied only in the following cases
   1. Highest Dim PKID is not projected and Non PK is requested
        eg. If Dimension D1 is requested and D1 Non PK is requested and D1 PK is not requested
   AND
   2. At least one Dim Non PK ID is projected
   AND
   3. Requested PK IDs has lower levels than best dimension candidates max level
   AND
   4. Request is not dimension driven
    OR
   if multiple dims with no relations are selected and do no have PK in requested cols
 */
    //val projectedNonIDCols = requestModel.requestColsSet.filter(!bestFactCandidate.publicFact.foreignKeyAliases.contains(_))
    //val nonKeyRequestedDimCols = bestFactCandidate.dimColMapping.map(_._2).filter(projectedNonIDCols.contains(_))
    val unrelatedDimensionsRequested: Boolean = {
      requestModel.dimensionRelations.hasUnrelatedDimensions &&
        !bestDimCandidates.forall(_.hasPKRequested)
    }
    val isHighestDimPkIDRequested = if (bestDimCandidates.nonEmpty) {
      bestDimCandidates.takeRight(1).head.hasPKRequested
    } else false

    val isRequestedHigherDimLevelKey: Boolean = { // check dim level of requested FK which is not one of the dim candidates
      if (bestDimCandidates.nonEmpty && requestModel.requestedFkAliasToPublicDimensionMap.nonEmpty) {
        val fkMaxDimLevel = requestModel.requestedFkAliasToPublicDimensionMap.map(e => e._2.dimLevel.level).max
        val dimCandidateMaxDimLevel = bestDimCandidates.map(e => e.dim.dimLevel.level).max
        if (fkMaxDimLevel >= dimCandidateMaxDimLevel) {
          true
        } else false
      } else false
    }

    val allSubQueryCandidates = bestDimCandidates.forall(_.isSubQueryCandidate)

    val ogbSupportedEngines:Set[Engine] = Set(OracleEngine, HiveEngine, PrestoEngine, PostgresEngine, BigqueryEngine)

    val hasOuterGroupBy = (//nonKeyRequestedDimCols.isEmpty
      ((!isRequestedHigherDimLevelKey && !isHighestDimPkIDRequested)
        || unrelatedDimensionsRequested
        )
        && bestDimCandidates.nonEmpty
        && !requestModel.isDimDriven
        && !allSubQueryCandidates
        && (ogbSupportedEngines.contains(bestFactCandidate.fact.engine))
        && bestDimCandidates.forall(candidate => ogbSupportedEngines.contains(candidate.dim.engine)))

    hasOuterGroupBy
  }

  private[this] def getViewQueryList(bestFactCandidate: FactBestCandidate,
                                     requestModel: RequestModel,
                                     queryAttributes: QueryAttributes,
                                     queryGenVersion: Version): List[Query] = {
    require(bestFactCandidate.fact.isInstanceOf[ViewTable], s"Unable to get view query list from fact ${bestFactCandidate.fact.name}")
    val viewTable = bestFactCandidate.fact.asInstanceOf[ViewTable]
    viewTable.view.facts.map {
      fact =>
        val factContext = QueryContext
          .newQueryContext(FactOnlyQuery, requestModel)
          .addFactBestCandidate(bestFactCandidate.copy(fact = fact))
          .setQueryAttributes(queryAttributes)
          .build()

        require(queryGeneratorRegistry.isEngineRegistered(bestFactCandidate.fact.engine, Option(queryGenVersion))
          , s"No query generator registered for engine ${bestFactCandidate.fact.engine} for fact ${bestFactCandidate.fact.name}")
        queryGeneratorRegistry.getValidGeneratorForVersion(bestFactCandidate.fact.engine, queryGenVersion, Option(requestModel)).get.generate(factContext)
    }.toList
  }


  private[this] def factOnlyInjectFilter(fbc: FactBestCandidate, injectFilter: Filter): FactBestCandidate = {
    if (fbc.publicFact.columnsByAlias(injectFilter.field)) {
      fbc.copy(filters = fbc.filters ++ Set(injectFilter))
    } else {
      fbc
    }
  }

  protected def findBestFactCandidate(requestModel: RequestModel, forceDisqualifySet: Set[Engine] = Set.empty, dimEngines: Set[Engine]): FactBestCandidate = {
    DefaultQueryPipelineFactory.findBestFactCandidate(requestModel, forceDisqualifySet, dimEngines, queryGeneratorRegistry)
  }

  protected def findBestDimCandidates(requestModel: RequestModel, factEngine: Engine, dimMapping: Map[String, SortedSet[DimensionBundle]]): SortedSet[DimensionBundle] = {
    DefaultQueryPipelineFactory.findBestDimCandidates(factEngine, requestModel, dimMapping, druidMultiQueryEngineList)
  }

  protected def findDimCandidatesMapping(requestModel: RequestModel): Map[String, SortedSet[DimensionBundle]] = {
    DefaultQueryPipelineFactory.findDimCandidatesMapping(requestModel)
  }

  def builder(requestModels: Tuple2[RequestModel, Option[RequestModel]], queryAttributes: QueryAttributes): Tuple2[Try[QueryPipelineBuilder], Option[Try[QueryPipelineBuilder]]] = {
    val bucketParams = new BucketParams()

    val queryPipelineTryDefault = builder(requestModels._1, queryAttributes, None, bucketParams)._1
    var queryPipelineTryDryRun: Option[Try[QueryPipelineBuilder]] = None
    if (requestModels._2.isDefined) {
      queryPipelineTryDryRun = Option(builder(requestModels._2.get, queryAttributes, None, bucketParams)._1)
    }
    (queryPipelineTryDefault, queryPipelineTryDryRun)
  }

  def builder(requestModels: Tuple2[RequestModel, Option[RequestModel]], queryAttributes: QueryAttributes, bucketSelector: BucketSelector, bucketParams: BucketParams): Tuple3[Try[QueryPipelineBuilder], Option[Try[QueryPipelineBuilder]], Option[Try[QueryPipelineBuilder]]] = {
    val queryPipelineTryDefault = builder(requestModels._1, queryAttributes, Option(bucketSelector), bucketParams)
    var queryPipelineTryDryRun: Option[Try[QueryPipelineBuilder]] = None
    if (requestModels._2.isDefined) {
      queryPipelineTryDryRun = Option(builder(requestModels._2.get, queryAttributes, None, bucketParams)._1)
    }
    (queryPipelineTryDefault._1, queryPipelineTryDefault._2, queryPipelineTryDryRun)
  }

  def builder(requestModel: RequestModel, queryAttributes: QueryAttributes): Try[QueryPipelineBuilder] = {
    builder(requestModel, queryAttributes, None, new BucketParams())._1
  }

  def builder(requestModel: RequestModel, queryAttributes: QueryAttributes, bucketSelector: Option[BucketSelector], bucketParams: BucketParams, fallbackDisqualifySet: Set[Engine] = Set.empty): Tuple2[Try[QueryPipelineBuilder], Option[Try[QueryPipelineBuilder]]] = {
    def requestDebug(msg: => String): Unit = {
      if (requestModel.isDebugEnabled) {
        info(msg)
      }
    }

    def runMultiEngineQuery(factBestCandidateOption: Option[FactBestCandidate], bestDimCandidates: SortedSet[DimensionBundle], queryGenVersion: Version): QueryPipelineBuilder = {
      val indexAlias = bestDimCandidates.last.publicDim.primaryKeyByAlias
      //if (!requestModel.hasFactSortBy || (requestModel.forceDimDriven && requestModel.hasDimFilters && requestModel.dimFilters.exists(_.operator == LikeFilterOperation))) {
      if (!requestModel.hasFactSortBy && requestModel.forceDimDriven) {
        //oracle + druid
        requestDebug("dimQueryThenFactQuery")
        val dimQuery = getMultiEngineDimQuery(bestDimCandidates.filter(_.dim.engine != DruidEngine), requestModel, indexAlias, List(indexAlias), queryAttributes, queryGenVersion)
        val subsequentQuery: (IndexedRowList, QueryAttributes) => Query = {
          case (irl, subqueryAttributes) =>
            val field = irl.rowGrouping.indexAlias
            val values = irl.keys.toList.map(_.toString)
            val filter = InFilter(field, values)
            val injectedFactBestCandidate = factOnlyInjectFilter(factBestCandidateOption.get, filter)
            // druid subsequentQuery should use startIndex = 0 to prevent wrongly row drop
            val factRequestModel = requestModel.copy(startIndex = 0)
            val query = getFactQuery(injectedFactBestCandidate, factRequestModel, indexAlias, List(indexAlias), queryGenVersion, Some(bestDimCandidates))
            irl.addSubQuery(query)
            query
        }

        new QueryPipelineBuilder(
          MultiEngineQuery(
            dimQuery,
            Set(dimQuery.engine, factBestCandidateOption.get.fact.engine),
            IndexedSeq(
              subsequentQuery
            )
          )
          , factBestCandidateOption
          , bestDimCandidates
          , requestModel.isDebugEnabled
        ).withRowListFunction(QueryPipeline.dimDrivenPartialRowList)
      } else {
        //druid + oracle
        //since druid + oracle doesn't support row count
        requestDebug("factQueryThenDimQuery")
        val noRowCountRequestModel = requestModel.copy(includeRowCount = false)
        val factQuery = getFactQuery(factBestCandidateOption.get, noRowCountRequestModel, indexAlias, List(indexAlias), queryGenVersion, Some(bestDimCandidates))
        val subsequentQuery: (IndexedRowList, QueryAttributes) => Query = {
          case (irl, subqueryqueryAttributes) =>
            val field = irl.rowGrouping.indexAlias
            val values = irl.keys.toList.map(_.toString)
            val valuesSize = values.size
            if (values.nonEmpty && (valuesSize >= noRowCountRequestModel.maxRows || noRowCountRequestModel.startIndex <= 0)) {
              val injectedAttributes = {
                val queryAttributesBuilder = subqueryqueryAttributes.toBuilder
                val injectedInFilter = InFilter(field, values)
                queryAttributesBuilder.addAttribute(QueryAttributes.injectedDimINFilter, InjectedDimFilterAttribute(injectedInFilter))
                if (noRowCountRequestModel.startIndex <= 0 && !noRowCountRequestModel.hasMetricFilters && noRowCountRequestModel.maxRows > values.size) {
                  val injectedNotInFilter = NotInFilter(field, values)
                  queryAttributesBuilder.addAttribute(QueryAttributes.injectedDimNOTINFilter, InjectedDimFilterAttribute(injectedNotInFilter)).build
                }
                queryAttributesBuilder.build
              }
              getMultiEngineDimQuery(bestDimCandidates.filter(_.dim.engine != DruidEngine), noRowCountRequestModel, indexAlias, List(indexAlias), injectedAttributes, queryGenVersion)
            } else {
              QueryChain.logger.info("No data returned from druid, should run fallback query if there is one")
              NoopQuery
            }
        }

        val fallbackQueryAndRowList: Option[(Query, RowList)] = {
          if (requestModel.bestCandidates.isDefined
            && requestModel.bestCandidates.get.facts.values.map(_.fact.engine).toSet.size > 1
            && requestModel.forceQueryEngine.isEmpty
            && requestModel.isDimDriven
          ) {
            requestDebug("hasFallbackQueryAndRowList")
            val (newFactBestCandidateOption, newBestDimCandidates) = findBestCandidates(noRowCountRequestModel, Set(factBestCandidateOption.get.fact.engine) ++ fallbackDisqualifySet)
            val query = getDimFactQuery(newBestDimCandidates, newFactBestCandidateOption.get, noRowCountRequestModel, queryAttributes, queryGenVersion)
            Option((query, QueryPipeline.completeRowList(query)))
          } else {
            None
          }
        }

        val partialRowList = {
          if (requestModel.forceDimDriven) {
            requestDebug("dimDrivenFactOrderedPartialRowList")
            QueryPipeline.dimDrivenFactOrderedPartialRowList
          } else {
            requestDebug("factDrivenPartialRowList")
            QueryPipeline.factDrivenPartialRowList
          }
        }

        new QueryPipelineBuilder(
          MultiEngineQuery(
            factQuery,
            Set(bestDimCandidates.filter(_.dim.engine != DruidEngine).head.dim.engine, factQuery.engine),
            IndexedSeq(
              subsequentQuery
            ),
            fallbackQueryAndRowList
          )
          , factBestCandidateOption
          , bestDimCandidates
          , requestModel.isDebugEnabled
        ).withRowListFunction(partialRowList)
      }
    }

    def runSyncSingleEngineQuery(factBestCandidateOption: Option[FactBestCandidate], bestDimCandidates: SortedSet[DimensionBundle], queryGenVersion: Version): QueryPipelineBuilder = {
      //oracle + oracle or druid only
      if (factBestCandidateOption.isDefined) {
        val query = getDimFactQuery(bestDimCandidates, factBestCandidateOption.get, requestModel, queryAttributes, queryGenVersion)
        val fallbackQueryOptionTry: Try[Option[Query]] = Try {
          val factEngines = requestModel.bestCandidates.get.facts.values.map(_.fact.engine).toSet
          val factBestCandidateEngine = factBestCandidateOption.get.fact.engine
          if (factEngines.size > 1) {
            val (newFactBestCandidateOption, newBestDimCandidates) = findBestCandidates(requestModel, Set(factBestCandidateEngine) ++ fallbackDisqualifySet)
            if (newFactBestCandidateOption.isEmpty) {
              requestDebug("No fact best candidate found for fallback query for request!")
            }
            newFactBestCandidateOption.map {
              newFactBestCandidate =>
                val query = getDimFactQuery(newBestDimCandidates, newFactBestCandidate, requestModel, queryAttributes, queryGenVersion)
                query
            }
          } else {
            requestDebug("No fallback query for request!")
            None
          }
        }
        val builder = new QueryPipelineBuilder(
          SingleEngineQuery(query)
          , factBestCandidateOption
          , bestDimCandidates
          , requestModel.isDebugEnabled
        )
        if (fallbackQueryOptionTry.isSuccess && fallbackQueryOptionTry.get.isDefined) {
          builder.withFallbackQueryChain(SingleEngineQuery(fallbackQueryOptionTry.get.get))
        } else {
          requestDebug("No fallback query option defined!")
        }
        builder
      } else {
        val query = getDimOnlyQuery(bestDimCandidates, requestModel, queryGenVersion)
        new QueryPipelineBuilder(
          SingleEngineQuery(query)
          , factBestCandidateOption
          , bestDimCandidates
          , requestModel.isDebugEnabled
        )
      }
    }

    def runViewMultiQuery(requestModel: RequestModel,
                          factBestCandidateOption: Option[FactBestCandidate],
                          bestDimCandidates: SortedSet[DimensionBundle],
                          queryGenVersion: Version): QueryPipelineBuilder = {
      val queryList: List[Query] = getViewQueryList(factBestCandidateOption.get, requestModel, queryAttributes, queryGenVersion)

      val fallbackNonViewQueryAndRowList: Option[(Query, RowList)] = {
        if (requestModel.bestCandidates.isDefined
          && requestModel.bestCandidates.get.facts.values.map(_.fact.engine).toSet.size > 1
          && requestModel.forceQueryEngine.isEmpty
        ) {
          val (newFactBestCandidateOption, newBestDimCandidates) = findBestCandidates(requestModel, Set(factBestCandidateOption.get.fact.engine) ++ fallbackDisqualifySet)
          val query = getDimFactQuery(newBestDimCandidates, newFactBestCandidateOption.get, requestModel, queryAttributes, queryGenVersion)
          Option((query, QueryPipeline.completeRowList(query)))
        } else {
          None
        }
      }

      new QueryPipelineBuilder(
        MultiQuery(queryList, fallbackNonViewQueryAndRowList)
        , factBestCandidateOption
        , bestDimCandidates
        , requestModel.isDebugEnabled
      ).withRowListFunction(QueryPipeline.unionViewPartialRowList(queryList))
    }

    def findBestCandidates(requestModel: RequestModel, forceDisqualifyEngine: Set[Engine]): (Option[FactBestCandidate], SortedSet[DimensionBundle]) = {
      val dimensionCandidatesMapping = findDimCandidatesMapping(requestModel)
      val dimEngines: Set[Engine] = dimensionCandidatesMapping.flatMap(_._2.map(_.dim.engine)).toSet
      val factBestCandidateOption =
        requestModel.bestCandidates.map(_ => findBestFactCandidate(requestModel, forceDisqualifyEngine, dimEngines))
      val dimensionCandidates = findBestDimCandidates(requestModel, factBestCandidateOption.map(_.fact.engine).getOrElse(defaultFactEngine), dimensionCandidatesMapping).filter(db => {
        val queryGenerator = queryGeneratorRegistry.getDefaultGenerator(db.dim.engine)
        !queryGenerator.isDefined || queryGenerator.get.validateEngineConstraints(requestModel)
      })
      (factBestCandidateOption, dimensionCandidates)
    }

    def getBuilder(factBestCandidateOption: Option[FactBestCandidate], bestDimCandidates: SortedSet[DimensionBundle], queryGenVersion: Version): Try[QueryPipelineBuilder] = {
      Try {
        val isMetricsOnlyViewQuery = factBestCandidateOption.isDefined && factBestCandidateOption.get.fact.isInstanceOf[ViewTable] && bestDimCandidates.isEmpty
        val isMultiEngineQuery = ((factBestCandidateOption.isDefined
          && bestDimCandidates.nonEmpty && bestDimCandidates.head.dim.engine != factBestCandidateOption.get.fact.engine)
          || (requestModel.isSyncRequest && requestModel.forceDimDriven
          && bestDimCandidates.nonEmpty && factBestCandidateOption.isDefined
          && bestDimCandidates.exists(_.dim.engine != factBestCandidateOption.get.fact.engine)
          && bestDimCandidates.filter(_.dim.engine != factBestCandidateOption.get.fact.engine).head.dim.engine != factBestCandidateOption.get.fact.engine))

        if (requestModel.isDebugEnabled) {
          info(requestModel.debugString)
          bestDimCandidates.foreach(db => info(db.debugString))
          info(s"factBestCandidateOption = ${factBestCandidateOption.map(_.debugString)}")
          info(s"isMetricsOnlyViewQuery = $isMetricsOnlyViewQuery")
          info(s"isMultiEngineQuery = $isMultiEngineQuery")
          if (isMultiEngineQuery) {
            info(s"dimEngine=${bestDimCandidates.head.dim.engine}")
            info(s"factEngine=${factBestCandidateOption.get.fact.engine}")
          }
        }

        requestModel.requestType match {
          case SyncRequest =>
            // 1. druid + oracle
            // 2. oracle + druid
            // 3. oracle + oracle
            if (isMetricsOnlyViewQuery) {
              requestDebug("runViewMultiQuery")
              runViewMultiQuery(requestModel, factBestCandidateOption, bestDimCandidates, queryGenVersion)
            } else if (isMultiEngineQuery) {
              requestDebug("runMultiEngineQuery")
              runMultiEngineQuery(factBestCandidateOption, bestDimCandidates, queryGenVersion)
            } else {
              //oracle + oracle or druid only
              requestDebug("runSingleEngineQuery")
              runSyncSingleEngineQuery(factBestCandidateOption, bestDimCandidates, queryGenVersion)
            }
          case AsyncRequest =>
            if (isMultiEngineQuery) {
              throw new UnsupportedOperationException("multi engine async request not supported!")
            } else
            if (isMetricsOnlyViewQuery && requestModel.maxRows > 0) {
              // Allow Async API side join query on when maxRows is set
              // Buffer limit on machine and max allowed rows on the Async request has to additionally checked before running async multi query

              info(s"Running Async View Multi Query for cube ${requestModel.cube}")
              runViewMultiQuery(requestModel, factBestCandidateOption, bestDimCandidates, queryGenVersion)
            } else {
              if (factBestCandidateOption.isDefined) {
                val query = getDimFactQuery(bestDimCandidates, factBestCandidateOption.get, requestModel, queryAttributes, queryGenVersion)
                val fallbackQueryOptionTry: Try[Option[Query]] = Try {
                  val factEngines = requestModel.bestCandidates.get.facts.values.map(_.fact.engine).toSet
                  val factBestCandidateEngine = factBestCandidateOption.get.fact.engine
                  if (requestModel.bestCandidates.isDefined
                    && factEngines.size > 1) {
                    val (newFactBestCandidateOption, newBestDimCandidates) = findBestCandidates(requestModel, Set(factBestCandidateEngine) ++ fallbackDisqualifySet)
                    if (newFactBestCandidateOption.isEmpty) {
                      info("No fact best candidate found for fallback query for request!")
                    }
                    newFactBestCandidateOption.map {
                      newFactBestCandidate =>
                        val query = getDimFactQuery(newBestDimCandidates, newFactBestCandidate, requestModel, queryAttributes, queryGenVersion)
                        //(query, QueryPipeline.completeRowList(query))
                        query
                    }
                  } else {
                    info("No fallback query for request!")
                    None
                  }
                }
                val builder = new QueryPipelineBuilder(
                  SingleEngineQuery(query)
                  , factBestCandidateOption
                  , bestDimCandidates
                  , requestModel.isDebugEnabled
                )
                // Use Off Heap rowlist for async if config is specified in the QP factory instantiation
                if (queryPipelineContext.offHeapRowListConfigOption.isDefined) {
                  val offHeapRowListConfig = queryPipelineContext.offHeapRowListConfigOption.get
                  builder.withRowListFunction(
                    (q) => OffHeapRowList(q, offHeapRowListConfig)
                  )
                }

                if (fallbackQueryOptionTry.isSuccess && fallbackQueryOptionTry.get.isDefined) {
                  builder.withFallbackQueryChain(SingleEngineQuery(fallbackQueryOptionTry.get.get))
                } else {
                  info(s"No fallback query for request: $fallbackQueryOptionTry")
                }
                builder
              } else {
                if (requestModel.forceDimDriven) {
                  val query = getDimOnlyQuery(bestDimCandidates, requestModel, queryGenVersion)
                  new QueryPipelineBuilder(
                    SingleEngineQuery(query)
                    , factBestCandidateOption
                    , bestDimCandidates
                    , requestModel.isDebugEnabled
                  )
                } else {
                  throw new IllegalArgumentException(s"Fact driven dim only query is not valid : ${requestModel.reportingRequest}")
                }
              }
            }
        }
      }
    }

    Try {
      val (factBestCandidateOption, bestDimCandidates) = {
        //if we have fact candidates, then we must always have a fact best candidate defined
        if (requestModel.bestCandidates.nonEmpty) {
          val isDimCandidatesRequired = requestModel.dimensionsCandidates.nonEmpty
          var factEngines: Set[Engine] = requestModel.bestCandidates.get.facts.values.map(_.fact.engine).toSet
          var disqualifySet: Set[Engine] = Set.empty
          var candidatesFound: Boolean = false
          var factBestCandidate: Option[FactBestCandidate] = None
          var bestDimCandidates: SortedSet[DimensionBundle] = SortedSet.empty
          while (factEngines.nonEmpty && !candidatesFound) {
            val (factBestCandidateResult, bestDimCandidatesResult) = findBestCandidates(requestModel, disqualifySet)
            factBestCandidate = factBestCandidateResult
            bestDimCandidates = bestDimCandidatesResult
            if (factBestCandidateResult.isDefined) {
              if (isDimCandidatesRequired) {
                if (bestDimCandidatesResult.isEmpty) {
                  val disqualifyEngine = factBestCandidateResult.get.fact.engine
                  factEngines -= disqualifyEngine
                  disqualifySet += disqualifyEngine
                } else {
                  candidatesFound = true
                }
              } else {
                candidatesFound = true
              }
            } else {
              require(factBestCandidateResult.nonEmpty, "Failed to find fact best candidate!")
            }
          }
          require(candidatesFound, "Failed to find best candidates")
          (factBestCandidate, bestDimCandidates)
        } else {
          findBestCandidates(requestModel, Set.empty)
        }
      }

      val (queryGenVersion: Version, dryRunQgenVersion: Option[Version]) = {
        val forceQueryGenVersion = bucketParams.forceQueryGenVersion

        if (forceQueryGenVersion.isDefined) {
          (forceQueryGenVersion.get, None)
        } else {
          if (bucketSelector.isDefined) {
            val engine = {
              if (factBestCandidateOption.isDefined) {
                factBestCandidateOption.get.fact.engine
              } else {
                bestDimCandidates.head.dim.engine
              }
            }
            val bucketSelected = bucketSelector.get.selectBucketsForQueryGen(engine, bucketParams)
            bucketSelected.fold(t => {
              warn(s"No query generator buckets selected for engine: $engine")
              (Version.DEFAULT, None)
            }, bucket => {
              (bucket.queryGenVersion, bucket.dryRunQueryGenVersion)
            })
          } else {
            (Version.DEFAULT, None)
          }
        }
      }

      val queryPipelineBuilder = getBuilder(factBestCandidateOption, bestDimCandidates, queryGenVersion)
      val queryPipelineBuilderDryRun = dryRunQgenVersion.map(dryRunVersion => getBuilder(factBestCandidateOption, bestDimCandidates, dryRunVersion))

      (queryPipelineBuilder, queryPipelineBuilderDryRun)
    }.fold(ex => {
      (new Failure(ex), None)
    }, result => result)
  }

  def from(requestModel: RequestModel, queryAttributes: QueryAttributes, bucketParams: BucketParams = new BucketParams()): Try[QueryPipeline] = {
    builder(requestModel, queryAttributes, None, bucketParams)._1.map(_.build())
  }

  def fromQueryGenVersion(requestModel: RequestModel, queryAttributes: QueryAttributes, queryGenVerion: Version): Try[QueryPipeline] = {
    val bucketParams = new BucketParams(forceQueryGenVersion = Some(queryGenVerion))
    builder(requestModel, queryAttributes, None, bucketParams)._1.map(_.build())
  }

  def from(requestModels: Tuple2[RequestModel, Option[RequestModel]], queryAttributes: QueryAttributes): Tuple2[Try[QueryPipeline], Option[Try[QueryPipeline]]] = {
    val queryPipelineBuilderTries = builder(requestModels, queryAttributes)
    if (queryPipelineBuilderTries._2.isDefined) {
      (queryPipelineBuilderTries._1.map(_.build()), Option(queryPipelineBuilderTries._2.get.map(_.build())))
    } else {
      (queryPipelineBuilderTries._1.map(_.build()), None)
    }
  }

  def fromBucketSelector(requestModels: Tuple2[RequestModel, Option[RequestModel]], queryAttributes: QueryAttributes, bucketSelector: BucketSelector, bucketParams: BucketParams): Tuple3[Try[QueryPipeline], Option[Try[QueryPipeline]], Option[Try[QueryPipeline]]] = {
    val queryPipelineBuilderTries = builder(requestModels, queryAttributes, bucketSelector, bucketParams)
    val queryPipeline: Try[QueryPipeline] = queryPipelineBuilderTries._1.map(_.build())
    val queryPipelineDryRunQgen: Option[Try[QueryPipeline]] = {
      if (queryPipelineBuilderTries._2.isDefined)
        Option(queryPipelineBuilderTries._2.get.map(_.build()))
      else None
    }
    val queryPipelineDryRunCube: Option[Try[QueryPipeline]] = {
      if (queryPipelineBuilderTries._3.isDefined)
        Option(queryPipelineBuilderTries._3.get.map(_.build()))
      else None
    }
    (queryPipeline, queryPipelineDryRunQgen, queryPipelineDryRunCube)
  }

  def fromBucketSelector(requestModel: RequestModel, queryAttributes: QueryAttributes, bucketSelector: BucketSelector, bucketParams: BucketParams): Tuple2[Try[QueryPipeline], Option[Try[QueryPipeline]]] = {
    val queryPipelines = fromBucketSelector(new Tuple2[RequestModel, Option[RequestModel]](requestModel, None), queryAttributes, bucketSelector, bucketParams)
    (queryPipelines._1, queryPipelines._2)
  }
}
