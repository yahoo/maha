package com.yahoo.maha.utils

import com.yahoo.maha.core._
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.query.druid.DruidQueryGenerator
import com.yahoo.maha.core.query.oracle.OracleQueryGenerator
import com.yahoo.maha.core.registry.Registry
import com.yahoo.maha.core.request.{Field, ReportingRequest}
import grizzled.slf4j.Logging

import scala.util.Try

object GetTotalRowsRequest extends Logging {

  def getTotalRowsRequest(request: ReportingRequest, pipeline: QueryPipeline) : Try[ReportingRequest] = {
    //no filters except fk filters
    Try {
      require(
        pipeline.bestDimCandidates.nonEmpty
        , s"Invalid total rows request, no best dim candidates! : $request")

      //force dim driven
      //remove all fields except primary key
      //remove all sorts
      val primaryKeyAliasFields = pipeline.bestDimCandidates.map(dim => Field(dim.publicDim.primaryKeyByAlias, None, None)).toIndexedSeq
      request.copy(
        selectFields = primaryKeyAliasFields
        , sortBy = IndexedSeq.empty
        , includeRowCount = true
        , forceDimensionDriven = true
        , forceFactDriven = false
        , paginationStartIndex = 0
        , rowsPerPage = request.rowsPerPage
      )
    }
  }

  def getTotalRows(request: RequestModel, sourcePipeline: QueryPipeline, registry: Registry, queryContext: QueryExecutorContext)(implicit queryGeneratorRegistry: QueryGeneratorRegistry) : Try[Int] = {
    Try {
      val totalRowsRequest: Try[ReportingRequest] = getTotalRowsRequest(request.reportingRequest, sourcePipeline)
      require(totalRowsRequest.isSuccess, "Failed to get valid totalRowsRequest\n" + totalRowsRequest)
      val modelTry: Try[RequestModel] = RequestModel.from(totalRowsRequest.get, registry)
      require(modelTry.isSuccess, "Failed to get valid request model\n" + modelTry)
      val model = modelTry.get

      val queryPipelineFactory = new DefaultQueryPipelineFactory()

      val requestPipelineTry = queryPipelineFactory.from(model, QueryAttributes.empty)
      require(requestPipelineTry.isSuccess, "Failed to get the query pipeline\n" + requestPipelineTry)
      val rowListAttempt = requestPipelineTry.toOption.get.execute(queryContext)
      require(rowListAttempt.isSuccess, "Failed to get valid executor and row list\n" + rowListAttempt)

      var rowCount = 0

      rowListAttempt.get._1.foreach(input => {
        require(input.aliasMap.contains(OracleQueryGenerator.ROW_COUNT_ALIAS), "TOTALROWS not defined in alias map, only valid in Oracle Queries")
        val totalrow_col_num = input.aliasMap(OracleQueryGenerator.ROW_COUNT_ALIAS)
        val current_totalrows = input.cols(totalrow_col_num).toString.toInt
        if(model.isDebugEnabled) {
          logger.info(s"Rows Returned: $current_totalrows")
        }

        rowCount = current_totalrows
      })

      rowCount
    }
  }

}
