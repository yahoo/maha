// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.report

import java.io.{Closeable, IOException}

import org.scalatest.{FunSuite, Matchers}
import org.mockito.Mockito._
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
class SafeCloseableTest extends FunSuite with Matchers {
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
    val closeable = spy(new SuccessCloseable)
    safeCloseable(closeable)(failWork)
    verify(closeable).close()
  }
  test("fail to close on failed closeable after failed doWork") {
    val closeable = spy(new FailCloseable)
    safeCloseable(closeable)(failWork)
    verify(closeable).close()
  }
}
