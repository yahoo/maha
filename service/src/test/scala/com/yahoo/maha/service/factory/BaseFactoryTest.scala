// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import com.google.common.io.Closer
import com.yahoo.maha.core.DailyGrain
import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Created by pranavbhole on 06/06/17.
 */
class BaseFactoryTest  extends AnyFunSuite with Matchers {
 protected val closer = Closer.create()
 protected[this] val fromDate = DailyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC).minusDays(7))
 protected[this] val toDate = DailyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC))
}
