// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.api.jersey.example

import com.yahoo.maha.core.request.{BaseRequest, ReportingRequest}

object ExampleRequest {

  def getRequest: BaseRequest = {
    ReportingRequest
  }

}
