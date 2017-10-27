// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core.DataType

/**
 * Created by jians on 10/20/15.
 */

case class QueryParameter(key: String, literalValue: String, dataType: DataType)
case class QueryParameters(paramList: List[QueryParameter])

class QueryParameterBuilder() {
  def build() : QueryParameters = {
    new QueryParameters(List())
  }
}

