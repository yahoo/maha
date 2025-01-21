// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.report

import com.yahoo.maha.utils.MockitoHelper

import java.io.{Closeable, IOException}
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
/**
  * Created by hiral on 3/9/18.
  */
class SuccessCloseable extends Closeable {
  override def close(): Unit = {
    //success
  }
}
class FailCloseable extends Closeable {
  override def close(): Unit = {
    throw new IOException("fail")
  }
}
class SafeCloseableTest extends AnyFunSuite with Matchers with MockitoHelper {
  def successWork(closeable: Closeable): Unit = {
    //success
  }
  def failWork(closeable: Closeable): Unit = {
    require(false, "fail")
  }
  test("successfully doWork") {
    safeCloseable(new SuccessCloseable)(successWork)
  }
  test("successfully close on failed doWork") {
    val closeable = spy[SuccessCloseable](new SuccessCloseable)
    safeCloseable(closeable)(failWork)
    verify(closeable).close()
  }
  test("fail to close on failed closeable after failed doWork") {
    val closeable = spy[FailCloseable](new FailCloseable)
    safeCloseable(closeable)(failWork)
    verify(closeable).close()
  }
}
