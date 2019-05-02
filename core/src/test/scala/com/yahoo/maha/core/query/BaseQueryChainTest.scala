// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core._
import com.yahoo.maha.core.request.ReportingRequest

/**
 * Created by hiral on 2/18/16.
 */
trait BaseQueryChainTest {
  this: BaseQueryGeneratorTest =>

  val queryExecutorContext = getQueryExecutorContext
  val queryExecutorContextWithoutReturnedRows = getQueryExecutorContextWithoutReturnedRows
  val partialQueryExecutorContext = getPartialQueryExecutorContext

  def updateRowList(rowList: QueryRowList) : Unit = {
    val row = rowList.newRow
    
    rowList.columnNames.foreach {
      col =>
        row.addValue(col, s"$col-value")
    }
    rowList.addRow(row)
  }

  def getQueryExecutorContext: QueryExecutorContext = {
    val qeOracle = new QueryExecutor {
      override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes) : QueryResult[T] = {
        updateRowList(rowList.asInstanceOf[QueryRowList])
        QueryResult(rowList, queryAttributes, QueryResultStatus.SUCCESS)
      }
      override def engine: Engine = OracleEngine
    }

    val qeHive = new QueryExecutor {
      override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes) : QueryResult[T] = {
        updateRowList(rowList.asInstanceOf[QueryRowList])
        QueryResult(rowList, queryAttributes, QueryResultStatus.SUCCESS)
      }
      override def engine: Engine = HiveEngine
    }
    val qeDruid = new QueryExecutor {
      override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes) : QueryResult[T] = {
        updateRowList(rowList.asInstanceOf[QueryRowList])
        QueryResult(rowList, queryAttributes, QueryResultStatus.SUCCESS)
      }
      override def engine: Engine = DruidEngine
    }

    val qec = new QueryExecutorContext
    qec.register(qeOracle)
    qec.register(qeHive)
    qec.register(qeDruid)
    qec
  }

  def getQueryExecutorContextWithoutReturnedRows: QueryExecutorContext = {
    val qeOracle = new QueryExecutor {
      override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes) : QueryResult[T] = {
        //updateRowList(rowList.asInstanceOf[QueryRowList])
        QueryResult(rowList, queryAttributes, QueryResultStatus.SUCCESS)
      }
      override def engine: Engine = OracleEngine
    }

    val qeHive = new QueryExecutor {
      override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes) : QueryResult[T] = {
        //updateRowList(rowList.asInstanceOf[QueryRowList])
        QueryResult(rowList, queryAttributes, QueryResultStatus.SUCCESS)
      }
      override def engine: Engine = HiveEngine
    }
    val qeDruid = new QueryExecutor {
      override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes) : QueryResult[T] = {
        //updateRowList(rowList.asInstanceOf[QueryRowList])
        QueryResult(rowList, queryAttributes, QueryResultStatus.SUCCESS)
      }
      override def engine: Engine = DruidEngine
    }

    val qec = new QueryExecutorContext
    qec.register(qeOracle)
    qec.register(qeHive)
    qec.register(qeDruid)
    qec
  }

  def getPartialQueryExecutorContext: QueryExecutorContext = {
    val qeOracle = new QueryExecutor {
      override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes) : QueryResult[T] = {
        updateRowList(rowList.asInstanceOf[QueryRowList])
        QueryResult(rowList, queryAttributes, QueryResultStatus.SUCCESS)
      }
      override def engine: Engine = OracleEngine
    }

    val qeHive = new QueryExecutor {
      override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes) : QueryResult[T] = {
        updateRowList(rowList.asInstanceOf[QueryRowList])
        QueryResult(rowList, queryAttributes, QueryResultStatus.SUCCESS)
      }
      override def engine: Engine = HiveEngine
    }
    val qeDruid = new QueryExecutor {
      override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes) : QueryResult[T] = {
        updateRowList(rowList.asInstanceOf[QueryRowList])
        QueryResult(rowList, queryAttributes, QueryResultStatus.FAILURE)
      }
      override def engine: Engine = DruidEngine
    }

    val qec = new QueryExecutorContext
    qec.register(qeOracle)
    qec.register(qeHive)
    qec.register(qeDruid)
    qec
  }

  def getRequestModel(jsonString: String): RequestModel = {
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val res = RequestModel.from(request, registry)
    assert(res.isSuccess, res.errorMessage("Failed to build request model"))
    res.toOption.get
  }

  def getDimQueryContext(engine: Engine,model: RequestModel, indexOption: Option[String], factGroupByKeys: List[String]) : DimQueryContext = {
    val dimMapping = DefaultQueryPipelineFactory.findDimCandidatesMapping(model)
    val dims = DefaultQueryPipelineFactory.findBestDimCandidates(engine, model.schema, dimMapping, druidMultiQueryEngineList)
    DimQueryContext(dims, model, indexOption, factGroupByKeys)
  }

  def getFactQueryContext(engine: Engine,model: RequestModel, indexOption: Option[String], factGroupByKeys: List[String], queryAttributes: QueryAttributes) : FactQueryContext = {
    val fact = DefaultQueryPipelineFactory.findBestFactCandidate(model, dimEngines = Set(engine), queryGeneratorRegistry = new QueryGeneratorRegistry)
    FactQueryContext(fact, model, indexOption, factGroupByKeys, queryAttributes)
  }

  def getCombinedQueryContext(engine: Engine,model: RequestModel, indexOption: Option[String], queryAttributes: QueryAttributes) : CombinedQueryContext = {
    val dimMapping = DefaultQueryPipelineFactory.findDimCandidatesMapping(model)
    val dims = DefaultQueryPipelineFactory.findBestDimCandidates(engine, model.schema, dimMapping, druidMultiQueryEngineList)
    val fact = DefaultQueryPipelineFactory.findBestFactCandidate(model, dimEngines = Set(engine), queryGeneratorRegistry = new QueryGeneratorRegistry)
    CombinedQueryContext(dims, fact, model, queryAttributes)
  }

  def getQuery(queryEngine: Engine, context: QueryContext, queryType: QueryType) : Query = {
    new Query {

      override def queryContext: QueryContext = context

      override def asString: String = "<query>"

      override def additionalColumns: IndexedSeq[String] = IndexedSeq.empty


      override def engine: Engine = queryEngine

      override def aliasColumnMap: Map[String, Column] = Map.empty

      override def queryGenVersion: Option[Version] = None
    }
  }

}
