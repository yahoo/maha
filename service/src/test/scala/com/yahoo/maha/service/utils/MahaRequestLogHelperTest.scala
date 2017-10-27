// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.utils

import com.yahoo.maha.service.MahaService
import org.mockito.Mockito._
import org.scalatest.{FunSuite, Matchers}

/**
 * Created by pranavbhole on 21/09/17.
 */
class MahaRequestLogHelperTest extends FunSuite with Matchers {
  test("Test MahaRequestLogHelper") {
    val mahaService = mock(classOf[MahaService])
    val mahaRequestLogHelper = MahaRequestLogHelper("ir", mahaService)
    mahaRequestLogHelper.setDryRun()
    mahaRequestLogHelper.logSuccess()
    val builder = mahaRequestLogHelper.protoBuilder
    assert(builder.getStatus == 200)
  }
}
