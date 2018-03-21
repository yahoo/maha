// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.utils

import java.nio.charset.StandardCharsets

import com.google.protobuf.ByteString
import com.yahoo.maha.core.CoreSchema.AdvertiserSchema
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.proto.MahaRequestLog.MahaRequestProto
import com.yahoo.maha.service.MahaService
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

  test("Test MahaRequestLogHelper") {
    val mahaService = mock(classOf[MahaService])
    val mahaRequestLogHelper = MahaRequestLogHelper("ir", mahaService)
    MDC.put(MahaConstants.REQUEST_ID,"123")
    MDC.put(MahaConstants.USER_ID,"abc")
    mahaRequestLogHelper.init(request,Option(123L),
      MahaRequestProto.RequestType.SYNC, ByteString.copyFrom(jsonString.getBytes(StandardCharsets.UTF_8)))
    mahaRequestLogHelper.setDryRun()
    mahaRequestLogHelper.logSuccess()
    mahaRequestLogHelper.setAsyncQueueParams()
    val proto = mahaRequestLogHelper.getbuilder()
    assert(proto.getStatus == 200)
    assert(proto.getRequestId == "123")
    assert(proto.getUserId == "abc")
  }
}
