package com.yahoo.maha.api.example

import com.yahoo.maha.core.query.QueryRowList
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.service.{MahaRequestContext, MahaService}
import com.yahoo.maha.service.calcite.DefaultMahaCalciteSqlParser
import com.yahoo.maha.service.calcite.avatica.{DefaultAvaticaRowListTransformer, DefaultConnectionUserInfoProvider, DefaultMahaAvaticaService, MahaAvaticaService}
import com.yahoo.maha.service.error.MahaServiceExecutionException
import com.yahoo.maha.service.utils.MahaRequestLogHelper

object ExampleAvaticaService {

  def getMahaAvaticaService(mahaService:MahaService) : MahaAvaticaService = {
    new DefaultMahaAvaticaService(avaticaExecuteFunction,
      new DefaultMahaCalciteSqlParser(mahaService.getMahaServiceConfig),
      mahaService,
      new DefaultAvaticaRowListTransformer(),
      (schma)=> ExampleSchema.namesToValuesMap(schma),
      defaultRegistry = "student",
      defaultSchema = ExampleSchema.StudentSchema,
      ReportingRequest,
      new DefaultConnectionUserInfoProvider()
    )
  }

  private def avaticaExecuteFunction: (MahaRequestContext, MahaService) =>QueryRowList = (mahaRequestContext, mahaService) => {
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext,mahaService.getMahaServiceConfig.mahaRequestLogWriter)
    val processRequestResult = mahaService.processRequest(mahaRequestContext.registryName, mahaRequestContext.reportingRequest, mahaRequestContext.bucketParams, mahaRequestLogHelper)
    if(processRequestResult.isLeft) {
      val ge = processRequestResult.left.get
      throw new MahaServiceExecutionException(ge.message, ge.throwableOption)
    }
    processRequestResult.right.get.queryPipelineResult.rowList.asInstanceOf[QueryRowList]
  }

}
