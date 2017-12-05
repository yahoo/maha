// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension.Dimension
import com.yahoo.maha.core.fact.Fact.ViewTable
import com.yahoo.maha.core.fact.{ViewBaseTable, FactView, FactBestCandidate}
import com.yahoo.maha.core.request.{AsyncRequest, SyncRequest}
import com.yahoo.maha.report.RowCSVWriter
import grizzled.slf4j.Logging
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{SortedSet, mutable}
import scala.util.Try

/**
 * Created by jians on 10/22/15.
 */
trait QueryPipeline {
  def queryChain: QueryChain

  def factBestCandidate: Option[FactBestCandidate]

  def bestDimCandidates: SortedSet[DimensionBundle]

  def execute(executorContext: QueryExecutorContext): Try[(RowList, QueryAttributes)]

  def execute(executorContext: QueryExecutorContext, queryAttributes: QueryAttributes): Try[(RowList, QueryAttributes)]
}

object QueryPipeline extends Logging {
  val syncNonDruidDisqualifyingSet: Set[Engine] = Set(DruidEngine, HiveEngine)
  val syncDisqualifyingSet: Set[Engine] = Set(HiveEngine)
  val asyncDisqualifyingSet: Set[Engine] = Set(DruidEngine)

  val completeRowList: Query => RowList = (q) => new CompleteRowList(q)
  val dimDrivenPartialRowList: Query => RowList = (q) => new DimDrivenPartialRowList(q.queryContext.indexAliasOption.get, q)
  val dimDrivenFactOrderedPartialRowList: Query => RowList = (q) => new DimDrivenFactOrderedPartialRowList(q.queryContext.indexAliasOption.get, q)
  val factDrivenPartialRowList: Query => RowList = (q) => new FactDrivenPartialRowList(q.queryContext.indexAliasOption.get, q)
  def unionViewPartialRowList(queryList: List[Query]) : Query => RowList = {
    require(queryList.forall(q => q.queryContext.isInstanceOf[FactualQueryContext] && q.queryContext.asInstanceOf[FactualQueryContext].factBestCandidate.fact.isInstanceOf[ViewBaseTable])
      , s"UnionViewPartialRowList only supports fact query context with fact views : ${queryList.map{_.queryContext.getClass.getSimpleName}}")
    val constAliasToValueMapList: List[Map[String, String]] = queryList.map {
        q =>
          val bestFact = q.queryContext.asInstanceOf[FactualQueryContext].factBestCandidate

          val constantColNameToValueMap = {
            val tempMap = new mutable.HashMap[String, String]()
            bestFact.fact.asInstanceOf[ViewBaseTable].constantColNameToValueMap.foreach {
              entry=>
                if (bestFact.publicFact.nameToAliasColumnMap.contains(entry._1)) {
                  tempMap ++= bestFact.publicFact.nameToAliasColumnMap(entry._1).map(alias=> alias-> entry._2).toMap
                }
            }
            tempMap.toMap
          }
          constantColNameToValueMap
    }

    val result : Query => RowList = (q) => {
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
        case DimQueryContext(_, _, _, _) =>
          throw new IllegalArgumentException(s"Requested UnionViewRowList in DimQueryContext")
        case _ =>
          throw new IllegalArgumentException(s"Requested UnionViewRowList in Unhandled/UnknownQueryContext")
      }
    }
    result
  }


  def csvRowList(csvWriter: RowCSVWriter, writeHeader: Boolean): Query => RowList = (q) =>
    new CSVRowList(q, csvWriter, writeHeader)
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
  def execute(executorContext: QueryExecutorContext): Try[(RowList, QueryAttributes)] = {
    val stats = new EngineQueryStats
    Try(queryChain.execute(executorContext, rowListFn, QueryAttributes.empty, stats)).recover {
      case throwable =>
        QueryPipelineWithFallback.logger.error("Primary query chain failed, recovering with fall back", throwable)
        fallbackQueryChain.execute(executorContext, fallbackRowListFn, QueryAttributes.empty, stats)
    }
  }

  def execute(executorContext: QueryExecutorContext, queryAttributes: QueryAttributes): Try[(RowList, QueryAttributes)] = {
    val stats = new EngineQueryStats
    Try(queryChain.execute(executorContext, rowListFn, queryAttributes, stats)).recover {
      case throwable =>
        QueryPipelineWithFallback.logger.error("Primary query chain failed, recovering with fall back", throwable)
        fallbackQueryChain.execute(executorContext, fallbackRowListFn, queryAttributes, stats)
    }
  }
}

case class DefaultQueryPipeline(queryChain: QueryChain
                                , factBestCandidate: Option[FactBestCandidate]
                                , bestDimCandidates: SortedSet[DimensionBundle]
                                , rowListFn: Query => RowList) extends QueryPipeline {
  def execute(executorContext: QueryExecutorContext): Try[(RowList, QueryAttributes)] = {
    val stats = new EngineQueryStats
    Try(queryChain.execute(executorContext, rowListFn, QueryAttributes.empty, stats))
  }

  def execute(executorContext: QueryExecutorContext, queryAttributes: QueryAttributes): Try[(RowList, QueryAttributes)] = {
    val stats = new EngineQueryStats
    Try(queryChain.execute(executorContext, rowListFn, queryAttributes, stats))
  }
}

class QueryPipelineBuilder(queryChain: QueryChain
                           , factBestCandidate: Option[FactBestCandidate]
                           , bestDimCandidates: SortedSet[DimensionBundle]) {

  private[this] var rowListFn: Query => RowList = QueryPipeline.completeRowList
  private[this] var fallbackRowListFn: Option[Query => RowList] = None
  private[this] var fallbackQueryChain: Option[QueryChain] = None

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
      new QueryPipelineWithFallback(
        queryChain, factBestCandidate, bestDimCandidates, rowListFn, fallbackQueryChain.get, fallbackRowListFn.getOrElse(QueryPipeline.completeRowList))
    } else {
      new DefaultQueryPipeline(queryChain, factBestCandidate, bestDimCandidates, rowListFn)
    }
  }
}

class EngineQueryStats {
  private[this] val statsList: ArrayBuffer[EngineQueryStat] = new ArrayBuffer(3)

  def addStat(stat: EngineQueryStat): Unit = {
    statsList += stat
  }

  def getStats: IndexedSeq[EngineQueryStat] = {
    statsList.toIndexedSeq
  }
}

case class EngineQueryStat(engine: Engine, startTime: Long, endTime: Long)

sealed trait QueryChain {
  def drivingQuery: Query

  def execute(executorContext: QueryExecutorContext, rowListFn: Query => RowList, queryAttributes: QueryAttributes, engineQueryStats: EngineQueryStats): (RowList, QueryAttributes)

  def subsequentQueryList: IndexedSeq[Query]
}

object QueryChain {
  val logger : Logger = LoggerFactory.getLogger(classOf[QueryChain])
}

case class SingleEngineQuery(drivingQuery: Query, fallbackQueryOption: Option[(Query, RowList)] = None) extends QueryChain {
  val subsequentQueryList: IndexedSeq[Query] = IndexedSeq.empty

  def execute(executorContext: QueryExecutorContext, rowListFn: Query => RowList, queryAttributes: QueryAttributes, engineQueryStats: EngineQueryStats): (RowList, QueryAttributes) = {
    val executor = executorContext.getExecutor(drivingQuery.engine)
    val drivingQueryStartTime = System.currentTimeMillis()
    val rowList = rowListFn(drivingQuery)
    rowList.start()
    val result = executor.fold(
      throw new IllegalArgumentException(s"Executor not found for engine=${drivingQuery.engine}, query=$drivingQuery")
    )(_.execute(drivingQuery, rowList, queryAttributes))
    val drivingQueryEndTime = System.currentTimeMillis()
    val queryStats = EngineQueryStat(drivingQuery.engine, drivingQueryStartTime, drivingQueryEndTime)
    engineQueryStats.addStat(queryStats)
    rowList.end()

    if(result.isFailure && fallbackQueryOption.isDefined) {
      QueryChain.logger.info(s"running fall back query, driving query failed with message=${result.message}")
      val (query, rowList) = fallbackQueryOption.get
      rowList.start()
      val queryStartTime = System.currentTimeMillis()
      val executor = executorContext.getExecutor(query.engine).get
      val newResult = executor.execute(query, rowList, result.queryAttributes)
      val queryEndTime = System.currentTimeMillis()
      engineQueryStats.addStat(EngineQueryStat(query.engine, queryStartTime, queryEndTime))
      rowList.end()
      (newResult.rowList, newResult.queryAttributes.toBuilder.addAttribute(QueryAttributes.QueryStats, QueryStatsAttribute(engineQueryStats)).build)
    } else {
      (result.rowList, result.queryAttributes.toBuilder.addAttribute(QueryAttributes.QueryStats, QueryStatsAttribute(engineQueryStats)).build)
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
  require(subsequentQueries.size > 0, "MultiEngineQuery must have > 1 subsequent queries!")
  require(drivingQuery.queryContext.indexAliasOption.isDefined, "Driving query must have index alias defined!")

  val subQueryList = new ArrayBuffer[Query]()

  def subsequentQueryList: IndexedSeq[Query] = subQueryList.toIndexedSeq

  def execute(executorContext: QueryExecutorContext, rowListFn: Query => RowList, queryAttributes: QueryAttributes, engineQueryStats: EngineQueryStats): (RowList, QueryAttributes) = {
    val executorsMap = engines.map { engine =>
      val executor = executorContext.getExecutor(engine)
      require(executor.isDefined, s"Executor not found for engine=$engine")
      engine -> executor.get
    }.toMap
    val rowList = rowListFn(drivingQuery)

    require(rowList.isInstanceOf[IndexedRowList], "Multi Engine Query requires an indexed row list!")

    rowList.start()
    val drivingQueryStartTime = System.currentTimeMillis()
    val drivingResult = executorsMap(drivingQuery.engine).execute(drivingQuery, rowList.asInstanceOf[IndexedRowList], queryAttributes)
    val drivingQueryEndTime = System.currentTimeMillis()
    engineQueryStats.addStat(EngineQueryStat(drivingQuery.engine, drivingQueryStartTime, drivingQueryEndTime))
    val result = if (drivingResult.rowList.keys.isEmpty) drivingResult else subsequentQueries.foldLeft(drivingResult) {
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
            engineQueryStats.addStat(EngineQueryStat(query.engine, queryStartTime, queryEndTime))
            result
        }
    }
    rowList.end()

    // result._1.isEmpty returns of the updatedRows.isEmpty
    if (result.rowList.isUpdatedRowListEmpty && fallbackQueryOption.isDefined) {
      QueryChain.logger.info("No data from driving query, running fall back query")
      val (query, rowList) = fallbackQueryOption.get
      subQueryList += query
      rowList.start()
      val queryStartTime = System.currentTimeMillis()
      val newResult = executorsMap(query.engine).execute(query, rowList, result.queryAttributes)
      val queryEndTime = System.currentTimeMillis()
      engineQueryStats.addStat(EngineQueryStat(query.engine, queryStartTime, queryEndTime))
      rowList.end()
      (newResult.rowList, newResult.queryAttributes.toBuilder.addAttribute(QueryAttributes.QueryStats, QueryStatsAttribute(engineQueryStats)).build)
    } else {
      (result.rowList, result.queryAttributes.toBuilder.addAttribute(QueryAttributes.QueryStats, QueryStatsAttribute(engineQueryStats)).build)
    }
  }
}

/*
 MultiQuery usage: MultiQuery can be used to execute the list if queries irrespective of Engine
  and it merged all the results into one rowList
 */
case class MultiQuery(unionQueryList: List[Query], fallbackQueryOption: Option[(Query, RowList)] = None) extends QueryChain {
  // Driving query is not actually driving the result here
  val drivingQuery  = unionQueryList.head
  val subsequentQueryList: IndexedSeq[Query] = unionQueryList.drop(1).toIndexedSeq

  def execute(executorContext: QueryExecutorContext, rowListFn: Query => RowList, queryAttributes: QueryAttributes, engineQueryStats: EngineQueryStats): (RowList, QueryAttributes) = {
    unionQueryList.foreach {
      q=>
        val executorOption = executorContext.getExecutor(q.engine)
        require(executorOption.isDefined, s"Executor not found for engine=${q.engine}, query=$q")
    }
    val firstQuery = drivingQuery
    val executorOption = executorContext.getExecutor(firstQuery.engine)
    val executor = executorOption.get
    val drivingQueryStartTime = System.currentTimeMillis()
    val rowList = rowListFn(firstQuery)
    rowList.start()

    val firstResult = executor.execute(firstQuery, rowList, queryAttributes)  //it is just first seed to MultiQuery
    val drivingQueryEndTime = System.currentTimeMillis()
    engineQueryStats.addStat(EngineQueryStat(firstQuery.engine, drivingQueryStartTime, drivingQueryEndTime))

    val result = unionQueryList.filter(q=> q!=firstQuery).foldLeft(firstResult) {
      (previousResult, subsequentQuery) =>
        val query = subsequentQuery
        query match {
          case NoopQuery =>
            previousResult
          case _ =>
            val subsequentQueryStartTime = System.currentTimeMillis()

            val result = {
                //Step to trigger the use of the next ConstAliasToValueMap out of the list created at init
               previousResult.rowList.nextStage()
               executor.execute(query, previousResult.rowList, previousResult.queryAttributes)
            }
            val subsequentQueryEndTime = System.currentTimeMillis()
            engineQueryStats.addStat(EngineQueryStat(query.engine, subsequentQueryStartTime, subsequentQueryEndTime))
            result
        }
    }
    rowList.end()

    if (result.rowList.isEmpty && fallbackQueryOption.isDefined) {
      QueryChain.logger.info("No data from driving query, running fall back query for MultiQuery")
      val (query, rowList) = fallbackQueryOption.get
      rowList.start()
      val queryStartTime = System.currentTimeMillis()
      val executorOption = executorContext.getExecutor(query.engine)
      val executor = executorOption.get
      val newResult = executor.execute(query, rowList, result.queryAttributes)
      val queryEndTime = System.currentTimeMillis()
      engineQueryStats.addStat(EngineQueryStat(query.engine, queryStartTime, queryEndTime))
      rowList.end()
      (newResult.rowList, newResult.queryAttributes.toBuilder.addAttribute(QueryAttributes.QueryStats, QueryStatsAttribute(engineQueryStats)).build)
    } else {
      (result.rowList, result.queryAttributes.toBuilder.addAttribute(QueryAttributes.QueryStats, QueryStatsAttribute(engineQueryStats)).build)
    }
  }
}

trait QueryPipelineFactory {
  def from(requestModel: RequestModel, queryAttributes: QueryAttributes): Try[QueryPipeline]

  def builder(requestModel: RequestModel, queryAttributes: QueryAttributes): Try[QueryPipelineBuilder]

  protected def findBestFactCandidate(requestModel: RequestModel, forceDisqualifySet: Set[Engine] = Set.empty, dimEngines: Set[Engine]): FactBestCandidate

  protected def findBestDimCandidates(requestModel: RequestModel, factEngine: Engine, dimMapping: Map[String, SortedSet[DimensionBundle]]): SortedSet[DimensionBundle]

  protected def from(requestModels: Tuple2[RequestModel, Option[RequestModel]], queryAttributes: QueryAttributes): Tuple2[Try[QueryPipeline], Option[Try[QueryPipeline]]]
}

object DefaultQueryPipelineFactory extends Logging {
  private[this] def aLessThanBByLevelAndCostAndCardinality(a: (String, Engine, Long, Int, Int), b: (String, Engine, Long, Int, Int)): Boolean = {
    if (a._2 == b._2) {
      if (a._4 == b._4) {
        a._3 < b._3
      } else {
        a._4 < b._4
      }
    } else {
      if(a._5 == b._5) {
        a._3 < b._3
      } else {
        a._5 < b._5
      }
    }
  }

  def findBestFactCandidate(requestModel: RequestModel, forceDisqualifySet: Set[Engine] = Set.empty, dimEngines: Set[Engine], queryGeneratorRegistry: QueryGeneratorRegistry): FactBestCandidate = {
    require(requestModel.bestCandidates.isDefined, s"Cannot create fact best candidate without best candidates : $requestModel")
    //schema specific rules?
    requestModel.schema match {
      case _ =>
        //do we want to force engine?
        val forceEngine = requestModel.forceQueryEngine

        //do not use druid if we have both dim and fact operations
        val disqualifySet: Set[Engine] = {

          if (!requestModel.outerFilters.isEmpty) {
            if (requestModel.isDebugEnabled) info(s"Outer filters are supported only Oracle Engine, Adding Druid, Hive to disqualifySet")
            QueryPipeline.syncNonDruidDisqualifyingSet
          } else {
            //is fact query with filter on dim where the dim's primary key is not in the request cols
            if (requestModel.isSyncRequest) {
              val hasIndexInOutput = requestModel.dimensionsCandidates.forall(d => requestModel.requestColsSet(d.dim.primaryKeyByAlias))

              if ((requestModel.hasLowCardinalityDimFilters && requestModel.forceDimDriven && requestModel.hasDimAndFactOperations)
                || (!requestModel.forceDimDriven && requestModel.isFactDriven && !hasIndexInOutput && !dimEngines.contains(DruidEngine)) //this does not apply if druid is the only dim candidate
              ) {
                QueryPipeline.syncNonDruidDisqualifyingSet
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
              if ((dimEngines.contains(DruidEngine) && !forceDisqualifySet(DruidEngine)) || !(requestModel.hasDimFilters || requestModel.requestCols.filter(c => c.isInstanceOf[DimColumnInfo]).size > 0)) {
                QueryPipeline.asyncDisqualifyingSet ++ forceDisqualifySet -- Set(DruidEngine)
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
          case ((fn, engine), rowcost) if (!queryGeneratorRegistry.getGenerator(engine).isDefined || queryGeneratorRegistry.getGenerator(engine).get.validateEngineConstraints(requestModel)) && (forceEngine.contains(engine) || (!disqualifySet(engine) && forceEngine.isEmpty)) =>
            val fact = requestModel.bestCandidates.get.facts(fn).fact
            val level = fact.level
            val dimCardinality: Long = requestModel.dimCardinalityEstimate.getOrElse(fact.defaultCardinality)
            val dimCardinalityPreference = requestModel.bestCandidates.get.publicFact.dimCardinalityEnginePreference(dimCardinality).get(requestModel.requestType).flatMap(_.get(engine)).getOrElse(Int.MaxValue)
            if(requestModel.isDebugEnabled) {
              info(s"fn=$fn engine=$engine cost=${rowcost.costEstimate} level=$level cardinalityPreference=$dimCardinalityPreference")
            }
            (fn, engine, rowcost.costEstimate, level, dimCardinalityPreference)
        }.sortWith(aLessThanBByLevelAndCostAndCardinality)
        require(result.nonEmpty,
          s"Failed to find best candidate, forceEngine=$forceEngine, engine disqualifyingSet=$disqualifySet, candidates=${requestModel.bestCandidates.get.facts.mapValues(_.fact.engine).toSet}")
        requestModel.bestCandidates.get.getFactBestCandidate(result.head._1, requestModel)
    }
  }

  def findBestDimCandidates(fact_engine: Engine, schema: Schema, dimMapping: Map[String, SortedSet[DimensionBundle]]) : SortedSet[DimensionBundle] = {
    val bestDimensionCandidates = new mutable.TreeSet[DimensionBundle]
    dimMapping.foreach {
      case (name, bundles) if fact_engine == DruidEngine =>
        var dc = bundles.filter(_.dim.engine == DruidEngine)
        if (dc.isEmpty) {
          dc = bundles.filter(_.dim.engine == OracleEngine)
          require(dc.nonEmpty, s"No concrete dimension found for engine=$OracleEngine, schema=$schema, dim=${dc.head.dim.name}")
        }
        bestDimensionCandidates += dc.head
      case (name, bundles) =>
        var dc = bundles.filter(_.dim.engine == fact_engine)
        if (dc.isEmpty) {
          dc = bundles.filter(_.dim.engine == fact_engine)
          require(dc.nonEmpty, s"No concrete dimension found for engine=$fact_engine, schema=$schema, dim=${dc.head.dim.name}")
        }
        bestDimensionCandidates += dc.head
    }
    bestDimensionCandidates
  }

  def findDimCandidatesMapping(requestModel: RequestModel): Map[String, SortedSet[DimensionBundle]] = {
    val dimCandidates = requestModel.dimensionsCandidates
    val schema = requestModel.schema
    val engineSet: Set[Engine] = Set(DruidEngine, OracleEngine, HiveEngine)

    val publicDimToConcreteDimMap: Map[(String, Engine), Dimension] = {
      val dimMap = new mutable.HashMap[(String, Engine), Dimension]
      dimCandidates.foreach {
        case dc =>
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
              if(withinMaxDaysLookBackOption) {
                dimMap += ((dc.dim.name, engine) -> dimOption.get)
              }
            }
          }
      }
      dimMap.toMap
    }

    val publicDimToConcreteDimMapWhichCanSupportAllDim  = new mutable.HashMap[(String, Engine), Dimension]()
    publicDimToConcreteDimMap
      .groupBy(key => key._1._2)
      .filter(row => row._2.size == publicDimToConcreteDimMap.keySet.map(_._1).size)
      .map(_._2)
      .foreach(_.foreach(publicDimToConcreteDimMapWhichCanSupportAllDim += _))

    val bestDimCandidatesMapping = new mutable.HashMap[String, SortedSet[DimensionBundle]]()
    dimCandidates.foreach {
      case dc =>
        bestDimCandidatesMapping += (dc.dim.name -> SortedSet[DimensionBundle]())
        engineSet.foreach {
          case engine =>
            if (publicDimToConcreteDimMapWhichCanSupportAllDim.contains(dc.dim.name, engine)) {
              var upper_dim_engine = engine
              var lower_dim_engine = engine
              var upperCandidates = dc.upperCandidates.flatMap(pd => publicDimToConcreteDimMapWhichCanSupportAllDim.get((pd.name, upper_dim_engine)))
              var lowerCandidates = dc.lowerCandidates.flatMap(pd => publicDimToConcreteDimMapWhichCanSupportAllDim.get((pd.name, lower_dim_engine)))
              if (engine == DruidEngine && upperCandidates.isEmpty) {
                upper_dim_engine = OracleEngine
                upperCandidates = dc.upperCandidates.flatMap(pd => publicDimToConcreteDimMapWhichCanSupportAllDim.get((pd.name, upper_dim_engine)))
              }
              if (engine == DruidEngine && lowerCandidates.isEmpty) {
                lower_dim_engine = OracleEngine
                lowerCandidates = dc.lowerCandidates.flatMap(pd => publicDimToConcreteDimMapWhichCanSupportAllDim.get((pd.name, lower_dim_engine)))
              }
              if (upperCandidates.size == dc.upperCandidates.size
                && lowerCandidates.size == dc.lowerCandidates.size) {
                val publicUpperCandidatesMap = dc.upperCandidates.map(pd => publicDimToConcreteDimMapWhichCanSupportAllDim((pd.name, upper_dim_engine)).name -> pd).toMap
                val publicLowerCandidatesMap = dc.lowerCandidates.map(pd => publicDimToConcreteDimMapWhichCanSupportAllDim((pd.name, lower_dim_engine)).name -> pd).toMap
                val hasNonPushDownFilters = dc.filters.exists(filter => !filter.isPushDown)
                bestDimCandidatesMapping(dc.dim.name) += DimensionBundle(
                  publicDimToConcreteDimMapWhichCanSupportAllDim((dc.dim.name, engine))
                  , dc.dim
                  , dc.fields
                  , dc.filters
                  , upperCandidates
                  , publicUpperCandidatesMap
                  , lowerCandidates
                  , publicLowerCandidatesMap
                  , isDrivingDimension = dc.isDrivingDimension
                  , hasNonFKOrForcedFilters = dc.hasNonFKOrForcedFilters
                  , hasNonFKSortBy = dc.hasNonFKSortBy
                  , hasNonPushDownFilters = hasNonPushDownFilters
                  , hasPKRequested = dc.hasPKRequested
                )
              }
            }
        }
    }
    bestDimCandidatesMapping.toMap
  }
}

class DefaultQueryPipelineFactory(implicit val queryGeneratorRegistry: QueryGeneratorRegistry) extends QueryPipelineFactory with Logging {

  private[this] def getDimOnlyQuery(bestDimCandidates: SortedSet[DimensionBundle], requestModel: RequestModel): Query = {
    val dimOnlyContextBuilder = QueryContext
      .newQueryContext(DimOnlyQuery, requestModel)
      .addDimTable(bestDimCandidates)
    require(bestDimCandidates.nonEmpty, "Cannot generate dim only query with no best dim candidates!")
    require(queryGeneratorRegistry.isEngineRegistered(bestDimCandidates.head.dim.engine)
      , s"Failed to find query generator for engine : ${bestDimCandidates.head.dim.engine}")
    queryGeneratorRegistry.getGenerator(bestDimCandidates.head.dim.engine).get.generate(dimOnlyContextBuilder.build())
  }

  private[this] def getMultiEngineDimQuery(bestDimCandidates: SortedSet[DimensionBundle], requestModel: RequestModel, indexAlias: String, queryAttributes: QueryAttributes): Query = {
    val dimOnlyContextBuilder = QueryContext
      .newQueryContext(DimOnlyQuery, requestModel)
      .addDimTable(bestDimCandidates)
      .addIndexAlias(indexAlias)
      .setQueryAttributes(queryAttributes)

    require(queryGeneratorRegistry.isEngineRegistered(bestDimCandidates.head.dim.engine)
      , s"Failed to find query generator for engine : ${bestDimCandidates.head.dim.engine}")
    queryGeneratorRegistry.getGenerator(bestDimCandidates.head.dim.engine).get.generate(dimOnlyContextBuilder.build())
  }

  private[this] def getFactQuery(bestFactCandidate: FactBestCandidate, requestModel: => RequestModel, indexAlias: String): Query = {
    val factOnlyContextBuilder = QueryContext
      .newQueryContext(FactOnlyQuery, requestModel)
      .addFactBestCandidate(bestFactCandidate)
      .addIndexAlias(indexAlias)
    require(queryGeneratorRegistry.isEngineRegistered(bestFactCandidate.fact.engine)
      , s"Failed to find query generator for engine : ${bestFactCandidate.fact.engine}")
    queryGeneratorRegistry.getGenerator(bestFactCandidate.fact.engine).get.generate(factOnlyContextBuilder.build())
  }

  private[this] def getDimFactQuery(bestDimCandidates: SortedSet[DimensionBundle],
                                    bestFactCandidate: FactBestCandidate,
                                    requestModel: RequestModel, queryAttributes: QueryAttributes): Query = {
    val queryType = if (isOuterGroupByQuery(bestDimCandidates, bestFactCandidate, requestModel)) {
        DimFactOuterGroupByQuery
    } else DimFactQuery

    val dimFactContext = QueryContext
      .newQueryContext(queryType, requestModel)
      .addDimTable(bestDimCandidates)
      .addFactBestCandidate(bestFactCandidate)
      .setQueryAttributes(queryAttributes)
      .build()

    require(queryGeneratorRegistry.isEngineRegistered(bestFactCandidate.fact.engine)
      , s"No query generator registered for engine ${bestFactCandidate.fact.engine} for fact ${bestFactCandidate.fact.name}")
    queryGeneratorRegistry.getGenerator(bestFactCandidate.fact.engine).get.generate(dimFactContext)
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
 */
    val projectedNonIDCols = requestModel.requestColsSet.filter(!bestFactCandidate.publicFact.foreignKeyAliases.contains(_))
    val nonKeyRequestedDimCols = bestFactCandidate.dimColMapping.map(_._2).filter(projectedNonIDCols.contains(_))
    val isHighestDimPkIDRequested = if(bestDimCandidates.nonEmpty) {
      bestDimCandidates.takeRight(1).head.hasPKRequested
    } else false

    val foreignKeyToPublicDimMap = bestFactCandidate.fact.publicDimToForeignKeyMap.map(e=> e._2 -> e._1)
    val isRequstedHigherDimLevelKey:Boolean =  { // check dim level of requested FK which is not one of the dim candidates
      if(bestDimCandidates.nonEmpty && requestModel.requestedFkAliasToPublicDimensionMap.nonEmpty) {
        val fkMaxDimLevel = requestModel.requestedFkAliasToPublicDimensionMap.map(e=> e._2.dimLevel.level).max
        val dimCandidateMaxDimLevel = bestDimCandidates.map(e=> e.dim.dimLevel.level).max
        if(fkMaxDimLevel >= dimCandidateMaxDimLevel) {
          true
        } else false
      } else false
    }

    val hasOuterGroupBy = (nonKeyRequestedDimCols.isEmpty
      && !isRequstedHigherDimLevelKey
      && !isHighestDimPkIDRequested
      && bestDimCandidates.nonEmpty
      && !requestModel.isDimDriven)

    hasOuterGroupBy
  }

  private[this] def getViewQueryList(bestFactCandidate: FactBestCandidate, requestModel: RequestModel, queryAttributes: QueryAttributes): List[Query] = {
    require(bestFactCandidate.fact.isInstanceOf[ViewTable], s"Unable to get view query list from fact ${bestFactCandidate.fact.name}")
    val viewTable = bestFactCandidate.fact.asInstanceOf[ViewTable]
    viewTable.view.facts.map {
      fact=>
        val factContext = QueryContext
          .newQueryContext(FactOnlyQuery, requestModel)
          .addFactBestCandidate(bestFactCandidate.copy(fact= fact))
          .setQueryAttributes(queryAttributes)
          .build()

        require(queryGeneratorRegistry.isEngineRegistered(bestFactCandidate.fact.engine)
          , s"No query generator registered for engine ${bestFactCandidate.fact.engine} for fact ${bestFactCandidate.fact.name}")
        queryGeneratorRegistry.getGenerator(bestFactCandidate.fact.engine).get.generate(factContext)
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
    DefaultQueryPipelineFactory.findBestDimCandidates(factEngine, requestModel.schema, dimMapping)
  }

  protected def findDimCandidatesMapping(requestModel: RequestModel) : Map[String, SortedSet[DimensionBundle]] = {
    DefaultQueryPipelineFactory.findDimCandidatesMapping(requestModel)
  }

  def builder(requestModels: Tuple2[RequestModel, Option[RequestModel]], queryAttributes: QueryAttributes): Tuple2[Try[QueryPipelineBuilder], Option[Try[QueryPipelineBuilder]]] = {
    val queryPipelineTryDefault = builder(requestModels._1, queryAttributes)
    var queryPipelineTryDryRun: Option[Try[QueryPipelineBuilder]] = None
    if (requestModels._2.isDefined) {
      queryPipelineTryDryRun =  Some(builder(requestModels._2.get, queryAttributes))
    }
    (queryPipelineTryDefault, queryPipelineTryDryRun)
  }


  def builder(requestModel: RequestModel, queryAttributes: QueryAttributes): Try[QueryPipelineBuilder] = {
    def requestDebug(msg: => String): Unit = {
      if(requestModel.isDebugEnabled) {
        info(msg)
      }
    }

    def runMultiEngineQuery(factBestCandidateOption: Option[FactBestCandidate], bestDimCandidates: SortedSet[DimensionBundle]): QueryPipelineBuilder = {
      val indexAlias = bestDimCandidates.last.publicDim.primaryKeyByAlias
      //if (!requestModel.hasFactSortBy || (requestModel.forceDimDriven && requestModel.hasDimFilters && requestModel.dimFilters.exists(_.operator == LikeFilterOperation))) {
      if (!requestModel.hasFactSortBy && requestModel.forceDimDriven) {
        //oracle + druid
        requestDebug("dimQueryThenFactQuery")
        val dimQuery = getMultiEngineDimQuery(bestDimCandidates, requestModel, indexAlias, queryAttributes)
        val subsequentQuery: (IndexedRowList, QueryAttributes) => Query = {
          case (irl, subqueryAttributes) =>
            val field = irl.indexAlias
            val values = irl.keys.toList.map(_.toString)
            val filter = InFilter(field, values)
            val injectedFactBestCandidate = factOnlyInjectFilter(factBestCandidateOption.get, filter)
            val query = getFactQuery(injectedFactBestCandidate, requestModel, indexAlias)
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
        ).withRowListFunction(QueryPipeline.dimDrivenPartialRowList)
      } else {
        //druid + oracle
        //since druid + oracle doesn't support row count
        requestDebug("factQueryThenDimQuery")
        val noRowCountRequestModel = requestModel.copy(includeRowCount = false)
        val factQuery = getFactQuery(factBestCandidateOption.get, noRowCountRequestModel, indexAlias)
        val subsequentQuery: (IndexedRowList, QueryAttributes) => Query = {
          case (irl, subqueryqueryAttributes) =>
            val field = irl.indexAlias
            val values = irl.keys.toList.map(_.toString)
            val injectedAttributes = if (values.nonEmpty) {
              val queryAttributesBuilder = subqueryqueryAttributes.toBuilder
              val injectedInFilter = InFilter(field, values)
              queryAttributesBuilder.addAttribute(QueryAttributes.injectedDimINFilter, InjectedDimFilterAttribute(injectedInFilter))
              if (values.size < noRowCountRequestModel.maxRows) {
                val injectedNotInFilter = NotInFilter(field, values)
                queryAttributesBuilder.addAttribute(QueryAttributes.injectedDimNOTINFilter, InjectedDimFilterAttribute(injectedNotInFilter)).build
              }
              queryAttributesBuilder.build
            } else {
              queryAttributes
            }
            if (values.nonEmpty) {
              getMultiEngineDimQuery(bestDimCandidates, noRowCountRequestModel, indexAlias, injectedAttributes)
            } else if(requestModel.isFactDriven) {
              NoopQuery
            } else {
              QueryChain.logger.info("No data returned from druid, re-running on alt engine")
              val (newFactBestCandidateOption, newBestDimCandidates) = findBestCandidates(noRowCountRequestModel, Set(factBestCandidateOption.get.fact.engine))
              getDimFactQuery(newBestDimCandidates, newFactBestCandidateOption.get, noRowCountRequestModel, queryAttributes)
            }

        }

        val fallbackQueryAndRowList: Option[(Query, RowList)] = {
          if (requestModel.bestCandidates.isDefined
            && requestModel.bestCandidates.get.facts.values.map(_.fact.engine).toSet.size > 1
            && requestModel.forceQueryEngine.isEmpty
            && requestModel.isDimDriven
          ) {
            requestDebug("hasFallbackQueryAndRowList")
            val (newFactBestCandidateOption, newBestDimCandidates) = findBestCandidates(noRowCountRequestModel, Set(factBestCandidateOption.get.fact.engine))
            val query = getDimFactQuery(newBestDimCandidates, newFactBestCandidateOption.get, noRowCountRequestModel, queryAttributes)
            Option((query, QueryPipeline.completeRowList(query)))
          } else {
            None
          }
        }

        val partialRowList = {
          if(requestModel.forceDimDriven) {
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
            Set(bestDimCandidates.head.dim.engine, factQuery.engine),
            IndexedSeq(
              subsequentQuery
            ),
            fallbackQueryAndRowList
          )
          , factBestCandidateOption
          , bestDimCandidates
        ).withRowListFunction(partialRowList)
      }
    }

    def runSingleEngineQuery(factBestCandidateOption: Option[FactBestCandidate], bestDimCandidates: SortedSet[DimensionBundle]): QueryPipelineBuilder = {
      //oracle + oracle or druid only
      if (factBestCandidateOption.isDefined) {
        val query = getDimFactQuery(bestDimCandidates, factBestCandidateOption.get, requestModel, queryAttributes)
        new QueryPipelineBuilder(
          SingleEngineQuery(query)
          , factBestCandidateOption
          , bestDimCandidates
        )
      } else {
        val query = getDimOnlyQuery(bestDimCandidates, requestModel)
        new QueryPipelineBuilder(
          SingleEngineQuery(query)
          , factBestCandidateOption
          , bestDimCandidates
        )
      }
    }

    def runViewMultiQuery(requestModel: RequestModel, factBestCandidateOption: Option[FactBestCandidate], bestDimCandidates: SortedSet[DimensionBundle]): QueryPipelineBuilder = {
        val queryList: List[Query] = getViewQueryList(factBestCandidateOption.get, requestModel, queryAttributes)

      val fallbackNonViewQueryAndRowList: Option[(Query, RowList)] = {
        if (requestModel.bestCandidates.isDefined
          && requestModel.bestCandidates.get.facts.values.map(_.fact.engine).toSet.size > 1
          && requestModel.forceQueryEngine.isEmpty
        ) {
          val (newFactBestCandidateOption, newBestDimCandidates) = findBestCandidates(requestModel, Set(factBestCandidateOption.get.fact.engine))
          val query = getDimFactQuery(newBestDimCandidates, newFactBestCandidateOption.get, requestModel, queryAttributes)
          Option((query, QueryPipeline.completeRowList(query)))
        } else {
          None
        }
      }

        new QueryPipelineBuilder(
          MultiQuery(queryList, fallbackNonViewQueryAndRowList)
          , factBestCandidateOption
          , bestDimCandidates
        ).withRowListFunction(QueryPipeline.unionViewPartialRowList(queryList))
    }

    def findBestCandidates(requestModel: RequestModel, forceDisqualifyEngine: Set[Engine]): (Option[FactBestCandidate], SortedSet[DimensionBundle]) = {
      val dimensionCandidatesMapping = findDimCandidatesMapping(requestModel)
      val dimEngines : Set[Engine] = dimensionCandidatesMapping.flatMap(_._2.map(_.dim.engine)).toSet
      val factBestCandidateOption =
        requestModel.bestCandidates.map(_ => findBestFactCandidate(requestModel, forceDisqualifyEngine, dimEngines))
      val dimensionCandidates = findBestDimCandidates(requestModel, factBestCandidateOption.map(_.fact.engine).getOrElse(OracleEngine), dimensionCandidatesMapping).filter(db => {
        val queryGenerator = queryGeneratorRegistry.getGenerator(db.dim.engine)
        !queryGenerator.isDefined || queryGenerator.get.validateEngineConstraints(requestModel)
      })
      (factBestCandidateOption, dimensionCandidates)
    }

    Try {
      val (factBestCandidateOption, bestDimCandidates) = findBestCandidates(requestModel, Set.empty)

      val isMetricsOnlyViewQuery = factBestCandidateOption.isDefined && factBestCandidateOption.get.fact.isInstanceOf[ViewTable] && bestDimCandidates.isEmpty
      val isMultiEngineQuery = (factBestCandidateOption.isDefined
        && bestDimCandidates.nonEmpty && bestDimCandidates.head.dim.engine != factBestCandidateOption.get.fact.engine)

      if (requestModel.isDebugEnabled) {
        info(requestModel.debugString)
        bestDimCandidates.foreach(db => info(db.debugString))
        info(s"factBestCandidateOption = ${factBestCandidateOption.map(_.debugString)}")
        info(s"isMetricsOnlyViewQuery = $isMetricsOnlyViewQuery")
        info(s"isMultiEngineQuery = $isMultiEngineQuery")
        if(isMultiEngineQuery) {
          info(s"dimEngine=${bestDimCandidates.head.dim.engine}")
          info(s"factEngine=${factBestCandidateOption.get.fact.engine}")
        }
      }

      requestModel.requestType match {
        case SyncRequest =>
          // 1. druid + oracle
          // 2. oracle + druid
          // 3. oracle + oracle
          if(isMetricsOnlyViewQuery) {
            requestDebug("runViewMultiQuery")
            runViewMultiQuery(requestModel, factBestCandidateOption, bestDimCandidates)
          } else if (isMultiEngineQuery) {
            requestDebug("runMultiEngineQuery")
            runMultiEngineQuery(factBestCandidateOption, bestDimCandidates)
          } else {
            //oracle + oracle or druid only
            requestDebug("runSingleEngineQuery")
            runSingleEngineQuery(factBestCandidateOption, bestDimCandidates)
          }
        case AsyncRequest =>
          if (isMultiEngineQuery) {
            throw new UnsupportedOperationException("multi engine async request not supported!")
          } else {
            if (factBestCandidateOption.isDefined) {
              val query = getDimFactQuery(bestDimCandidates, factBestCandidateOption.get, requestModel, queryAttributes)
              val fallbackQueryOptionTry: Try[Option[(Query, RowList)]] = Try {
                if (requestModel.bestCandidates.isDefined
                  && requestModel.bestCandidates.get.facts.values.map(_.fact.engine).toSet.size > 1
                  && requestModel.forceQueryEngine.isEmpty) {
                  val (newFactBestCandidateOption, newBestDimCandidates) = findBestCandidates(requestModel, Set(factBestCandidateOption.get.fact.engine))
                  val query = getDimFactQuery(newBestDimCandidates, newFactBestCandidateOption.get, requestModel, queryAttributes)
                  Option((query, QueryPipeline.completeRowList(query)))
                } else {
                  None
                }
              }
              new QueryPipelineBuilder(
                SingleEngineQuery(query, fallbackQueryOptionTry.getOrElse(None))
                , factBestCandidateOption
                , bestDimCandidates
              )
            } else {
              if (requestModel.forceDimDriven) {
                val query = getDimOnlyQuery(bestDimCandidates, requestModel)
                new QueryPipelineBuilder(
                  SingleEngineQuery(query)
                  , factBestCandidateOption
                  , bestDimCandidates
                )
              } else {
                throw new IllegalArgumentException(s"Fact driven dim only query is not valid : $requestModel")
              }
            }
          }
      }
    }
  }

  def from(requestModel: RequestModel, queryAttributes: QueryAttributes): Try[QueryPipeline] = {
    builder(requestModel, queryAttributes).map(_.build())
  }

  def from(requestModels: Tuple2[RequestModel, Option[RequestModel]], queryAttributes: QueryAttributes): Tuple2[Try[QueryPipeline], Option[Try[QueryPipeline]]] = {
    val queryPipelineBuilderTries = builder(requestModels, queryAttributes)
    if (queryPipelineBuilderTries._2.isDefined) {
      (queryPipelineBuilderTries._1.map(_.build()), Some(queryPipelineBuilderTries._2.get.map(_.build())))
    } else {
      (queryPipelineBuilderTries._1.map(_.build()), None)
    }
  }
}
