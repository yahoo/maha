package com.yahoo.maha.utils

import com.yahoo.maha.core._
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.registry.Registry
import com.yahoo.maha.core.request.{Field, ReportingRequest}
import grizzled.slf4j.Logging

import scala.util.Try

object GetTotalRowsRequest extends Logging {

  private[this] def getTotalRowsRequest(sourcePipeline: QueryPipeline) : Try[ReportingRequest] = {
    //no filters except fk filters
    Try {
      require(
        sourcePipeline.bestDimCandidates.nonEmpty
        , s"Invalid total rows request, no best dim candidates! : ${sourcePipeline.requestModel}")

      //force dim driven
      //remove all fields except primary key
      //remove all sorts
      val primaryKeyAliasFields = sourcePipeline.bestDimCandidates.map(dim => Field(dim.publicDim.primaryKeyByAlias, None, None)).toIndexedSeq
      sourcePipeline.requestModel.reportingRequest.copy(
        selectFields = primaryKeyAliasFields
        , sortBy = IndexedSeq.empty
        , includeRowCount = true
        , forceDimensionDriven = true
        , forceFactDriven = false
        , paginationStartIndex = 0
        , rowsPerPage = sourcePipeline.requestModel.reportingRequest.rowsPerPage
      )
    }
  }

  def getTotalRows(sourcePipeline: QueryPipeline
                   , registry: Registry
                   , queryContext: QueryExecutorContext
                   , queryPipelineFactory: QueryPipelineFactory): Try[Int] = {
    Try {
      val totalRowsRequest: Try[ReportingRequest] = getTotalRowsRequest(sourcePipeline)
      require(totalRowsRequest.isSuccess, "Failed to get valid ReportingRequest for totalRows\n" + totalRowsRequest)

      val modelTry: Try[RequestModel] = RequestModel.from(totalRowsRequest.get, registry)
      require(modelTry.isSuccess, "Failed to get valid request model for totalRows\n" + modelTry)
      val model = modelTry.get

      val requestPipelineTry = queryPipelineFactory.from(model, QueryAttributes.empty)
      require(requestPipelineTry.isSuccess, "Failed to get the query pipeline for totalRows\n" + requestPipelineTry)

      val rowListAttempt = requestPipelineTry.toOption.get.execute(queryContext)
      require(rowListAttempt.isSuccess, "Failed to execute the totalRows query pipeline\n" + rowListAttempt)

      val rowCount = rowListAttempt.get._1.getTotalRowCount
      if(model.isDebugEnabled) {
        logger.info(s"Rows Returned: $rowCount")
      }
      rowCount
    }
  }

}
