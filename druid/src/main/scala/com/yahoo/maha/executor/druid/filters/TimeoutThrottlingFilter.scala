// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.executor.druid.filters

import java.net.ConnectException
import java.util
import java.util.concurrent.TimeoutException
import grizzled.slf4j.Logging
import io.netty.handler.codec.http.HttpHeaders
import org.asynchttpclient.{AsyncHandler, HttpResponseBodyPart, HttpResponseStatus, Request}
import org.asynchttpclient.filter.{FilterContext, FilterException, RequestFilter}

/**
 * Created by pranavbhole on 28/06/17.
 */

trait ThrottlingRequestFilter extends RequestFilter {
  protected val timeoutHttpStatusCodes = Set(500)
}

case class TimeoutThrottlingFilter(timeoutWindow: Long = 30000L,
                                   timeoutThreshold: Int = 30,
                                   timeoutRetryInterval: Long = 600000L,
                                   timeoutMaxResponseTime: Long = 3000L) extends  ThrottlingRequestFilter with Logging {

  val timeoutMillsStore = TimeoutMillsStore(timeoutWindow, timeoutThreshold, timeoutRetryInterval)

  override def filter[T](filterContext: FilterContext[T]): FilterContext[T] = {
      try {
        if (logger.isDebugEnabled) {
          logger.debug("Checking for availability.")
        }
        timeoutMillsStore.checkAvailability(filterContext.getRequest.getUrl)
      }
      catch {
        case e1: ServiceUnavailableException => {
          throw new FilterException("ServiceUnavailableException.", e1)
        }
      }
    val requestStartTime: Long = System.currentTimeMillis
    return new FilterContext.FilterContextBuilder[T](filterContext)
      .asyncHandler(new AsyncHandlerWrapper[T](filterContext.getAsyncHandler, requestStartTime, filterContext.getRequest).asInstanceOf[AsyncHandler[T]])
      .build
  }

  case class AsyncHandlerWrapper[T](asyncHandler: AsyncHandler[T], requestStartTime: Long,
                                    request: Request) extends AsyncHandler[T] {
    override def onCompleted: T = {
      val responseTime: Long = System.currentTimeMillis - this.requestStartTime
      if (responseTime > timeoutMaxResponseTime) {
          if (logger.isDebugEnabled) {
            logger.debug("Response taking too long. Returned response time is " + responseTime + " Configured maxResponseTime is: " + timeoutMaxResponseTime + " Adding to timeoutList.")
          }
          timeoutMillsStore.addTimeout()
      }
      return asyncHandler.onCompleted
    }

    def onThrowable(t: Throwable) {
      try {
        if (t.isInstanceOf[ConnectException] || t.isInstanceOf[TimeoutException]) {
          logger.error("Connection or Timeout Exception caught. " + t + " Adding to timeoutList for url: " + request.getUrl)
          timeoutMillsStore.addTimeout()
        }
        else {
          logger.error("Exception caught. " + t + " for url: " + request.getUrl)
        }
      } finally {
        asyncHandler.onThrowable(t)
      }
    }

    override def onBodyPartReceived(bodyPart: HttpResponseBodyPart): AsyncHandler.State = {
      return asyncHandler.onBodyPartReceived(bodyPart)
    }

    override def onStatusReceived(responseStatus: HttpResponseStatus): AsyncHandler.State = {
      if (timeoutHttpStatusCodes.contains(responseStatus.getStatusCode)) {
        timeoutMillsStore.addTimeout()
      }
      return asyncHandler.onStatusReceived(responseStatus)
    }

    override def onHeadersReceived(headers: HttpHeaders): AsyncHandler.State = {
      return asyncHandler.onHeadersReceived(headers)
    }
  }

}

case class ServiceUnavailableException(message : String, url : String) extends Exception {
  override def toString : String = message+" url:"+url
}

case class TimeoutMillsStore(window: Long, threshold: Int, retryInterval: Long) {
  var lastCheck : Long = 0L
  val timeouts = new util.concurrent.ConcurrentHashMap[Long, Unit]()

  def addTimeout(): Unit = synchronized {
      val now: Long = System.currentTimeMillis
      val cutoff: Long = now - window
      val iterator = timeouts.keySet().iterator()
      while(iterator.hasNext) {
        val time = iterator.next()
        if(time < cutoff) {
          timeouts.remove(time)
        }
      }
      timeouts.put(now, Unit)
      lastCheck = now
  }

  def checkAvailability(url: String): Boolean = {
    if (timeouts.size > threshold) {
      val now: Long = System.currentTimeMillis
      if (lastCheck > now - retryInterval) {
        throw new ServiceUnavailableException(s"ServiceUnavailableException, timeout threshold count exceeded ${timeouts.size} > ${threshold}", url)
      }
      else {
        lastCheck = now
        timeouts.clear()
        true
      }
    } else {
      return true
    }
  }
}
