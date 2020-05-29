// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.utils

import java.nio.charset.StandardCharsets

import com.yahoo.maha.core.CoreSchema.AdvertiserSchema
import com.yahoo.maha.core.{DruidEngine, OracleEngine, PrestoEngine}
import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.log.MahaRequestLogWriter
import com.yahoo.maha.service.curators.DefaultCurator
import com.yahoo.maha.service.{MahaRequestContext, MahaServiceConfig}
import org.mockito.Mockito._
import org.scalatest.{FunSuite, Matchers}
import org.slf4j.MDC

/**
 * Created by pranavbhole on 21/09/17.
 */
class MahaRequestLogHelperTest extends FunSuite with Matchers {
  val jsonString =
    """
      |{
      |                          "cube": "publicFact",
      |                          "selectFields": [
      |                              {"field": "Advertiser ID"},
      |                              {"field": "Campaign ID"},
      |                              {"field": "Impressions"},
      |                              {"field": "Pricing Type"}
      |                          ],
      |                          "filterExpressions": [
      |                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
      |                              {"field": "Day", "operator": "between", "from": "2018-02-01", "to": "2018-02-02"}
      |                          ],
      |                          "sortBy": [
      |                          ],
      |                          "paginationStartIndex":20,
      |                          "rowsPerPage":100
      |}
    """.stripMargin
  val request: ReportingRequest = {
    val result = ReportingRequest.deserializeSync(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    result.toOption.get
  }

  val bucketParams: BucketParams = BucketParams(UserInfo("uid", true))

  val queryAttributeBuilder  = new QueryAttributeBuilder
  val engineStats = new EngineQueryStats()
  engineStats.addStat(EngineQueryStat(PrestoEngine, System.currentTimeMillis(), System.currentTimeMillis(), "presto"))
  engineStats.addStat(EngineQueryStat(OracleEngine, System.currentTimeMillis(), System.currentTimeMillis(), "oracle"))
  engineStats.addStat(EngineQueryStat(DruidEngine, System.currentTimeMillis(), System.currentTimeMillis(), "druid"))
  queryAttributeBuilder.addAttribute(QueryAttributes.QueryStats, QueryStatsAttribute(engineStats))
  queryAttributeBuilder.addAttribute(QueryAttributes.QueryStats, QueryStatsAttribute(engineStats))


  test("Test MahaRequestLogHelper") {
    val mahaServiceConf = mock(classOf[MahaServiceConfig])
    val mahaRequestLogWriter = mock(classOf[MahaRequestLogWriter])
    val mahaRequestContext = MahaRequestContext("ir",
      bucketParams,
      request,
      jsonString.getBytes,
      Map.empty, "123", "abc")
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConf.mahaRequestLogWriter)
    val proto = mahaRequestLogHelper.getbuilder()
    MDC.put(MahaConstants.REQUEST_ID,"123")
    MDC.put(MahaConstants.USER_ID,"abc")
    mahaRequestLogHelper.setDryRun()
    mahaRequestLogHelper.setAsyncQueueParams()
    mahaRequestLogHelper.logQueryStats(queryAttributeBuilder.build)
    when(mahaServiceConf.mahaRequestLogWriter).thenReturn(mahaRequestLogWriter)
    when(mahaRequestLogWriter.write(proto.build())).thenAnswer(_)
    mahaRequestLogHelper.logSuccess()
    assert(proto.getStatus == 200)
    assert(proto.getRequestId == "123")
    assert(proto.getUserId == "abc")
    assert(MahaRequestLogHelper.hostname.isDefined)
    assert(MahaRequestLogHelper.logger.isErrorEnabled)
    assert(proto.getRequestHash == "be92d6566e6030cb42a061f42765875c")
  }

  test("Test MahaRequestLogHelper LogFailed with status") {
    val mahaServiceConf = mock(classOf[MahaServiceConfig])
    val mahaRequestLogWriter = mock(classOf[MahaRequestLogWriter])
    val mahaRequestContext = MahaRequestContext("ir",
      bucketParams,
      request,
      jsonString.getBytes,
      Map.empty, "123", "abc")
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConf.mahaRequestLogWriter)
    val proto = mahaRequestLogHelper.getbuilder()
    MDC.put(MahaConstants.REQUEST_ID,"123")
    MDC.put(MahaConstants.USER_ID,"abc")
    mahaRequestLogHelper.setDryRun()
    mahaRequestLogHelper.setJobId(12345)
    mahaRequestLogHelper.setJobIdString("abcdefg")
    mahaRequestLogHelper.setAsyncQueueParams()
    mahaRequestLogHelper.logQueryStats(queryAttributeBuilder.build)
    when(mahaServiceConf.mahaRequestLogWriter).thenReturn(mahaRequestLogWriter)
    when(mahaRequestLogWriter.write(proto.build())).thenAnswer(_)
    mahaRequestLogHelper.logFailed("Test Failed", Some(400))
    assert(proto.getStatus == 400)
    assert(proto.getRequestId == "123")
    assert(proto.getUserId == "abc")
    assert(proto.getFirstSubsequentQueryEngine === "Oracle")
    assert(proto.getFirstSubsequentQueryTable=== "oracle")
    assert(proto.getReRunEngine.toString === "Druid")
    assert(proto.getReRunEngineQueryTable === "druid")
  }

  test("Test MahaRequestLogHelper LogFailed with status and null request context") {
    val mahaServiceConf = mock(classOf[MahaServiceConfig])
    val mahaRequestLogWriter = mock(classOf[MahaRequestLogWriter])
    val mahaRequestContext = MahaRequestContext("ir",
      null,
      null,
      null,
      null, null, null)
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConf.mahaRequestLogWriter)
    val proto = mahaRequestLogHelper.getbuilder()
    mahaRequestLogHelper.setDryRun()
    mahaRequestLogHelper.setJobId(12345)
    mahaRequestLogHelper.setAsyncQueueParams()
    mahaRequestLogHelper.logQueryStats(queryAttributeBuilder.build)
    when(mahaServiceConf.mahaRequestLogWriter).thenReturn(mahaRequestLogWriter)
    when(mahaRequestLogWriter.write(proto.build())).thenAnswer(_)
    mahaRequestLogHelper.logFailed("Test Failed", Some(400))
    assert(proto.getStatus == 400)
  }

  test("Create curatorMahaRequestLogHelper to check logging") {
    val mahaServiceConf = mock(classOf[MahaServiceConfig])
    val mahaRequestLogWriter = mock(classOf[MahaRequestLogWriter])
    val asyncRequest : ReportingRequest = ReportingRequest.deserializeAsync(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema).toOption.get
    val mahaRequestContext = MahaRequestContext("ir",
      bucketParams,
      asyncRequest,
      jsonString.getBytes,
      Map.empty, "123", "abc")
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConf.mahaRequestLogWriter)
    val failedLog = mahaRequestLogHelper.logFailed("new error message")
    mahaRequestLogHelper.logSuccess()
    mahaRequestLogHelper.logSuccess()
    mahaRequestLogHelper.setJobId(12345)
    mahaRequestLogHelper.logQueryStats(queryAttributeBuilder.build)
    val curatorLogBuilder = mahaRequestLogHelper.curatorLogBuilder(new DefaultCurator())
    val curatorHelper = CuratorMahaRequestLogHelper(curatorLogBuilder)
    curatorHelper.setJobIdString("abcd")
    curatorHelper.logFailed("a second new error message")
  }
}
