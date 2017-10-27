// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import java.util.concurrent.RejectedExecutionHandler

import com.yahoo.maha.core.request._
import com.yahoo.maha.parrequest.future.ParallelServiceExecutor
import com.yahoo.maha.service.MahaServiceConfig
import com.yahoo.maha.service.MahaServiceConfig.MahaConfigResult
import org.json4s.JValue

import scalaz.Validation.FlatMap._
import scalaz.syntax.applicative._


/**
 * Created by pranavbhole on 07/06/17.
 */
class DefaultParallelServiceExecutoryFactory extends ParallelServiceExecutoryFactory{
  """
    |{
    |"rejectedExecutionHandlerClass" : "",
    |"rejectedExecutionHandlerConfig" : "",
    |"poolName" : "",
    |"defaultTimeoutMillis" : 10000,
    |"threadPoolSize" : 3,
    |"queueSize" : 3
    |}
  """.stripMargin
  override def fromJson(configJson: JValue): MahaConfigResult[ParallelServiceExecutor] = {
    import org.json4s.scalaz.JsonScalaz._
    val rejectedExecutionHandlerClassResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("rejectedExecutionHandlerClass")(configJson)
    val rejectedExecutionHandlerConfigResult: MahaServiceConfig.MahaConfigResult[JValue] = fieldExtended[JValue]("rejectedExecutionHandlerConfig")(configJson)
    val poolNameResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("poolName")(configJson)
    val defaultTimeoutMillisResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("defaultTimeoutMillis")(configJson)
    val threadPoolSizeResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("threadPoolSize")(configJson)
    val queueSizeResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("queueSize")(configJson)

    val rejectedExecutionHandlerResult : MahaServiceConfig.MahaConfigResult[RejectedExecutionHandler] = for {
      rejectedExecutionHandlerClass <- rejectedExecutionHandlerClassResult
      rejectedExecutionHandlerConfig <- rejectedExecutionHandlerConfigResult
      factory <- getFactory[RejectedExecutionHandlerFactory](rejectedExecutionHandlerClass)
      rejectedExecutionHandler <- factory.fromJson(rejectedExecutionHandlerConfig)
    } yield rejectedExecutionHandler

    (rejectedExecutionHandlerResult |@| poolNameResult |@| defaultTimeoutMillisResult |@| threadPoolSizeResult |@| queueSizeResult ) {
     (rejectedExecutionHandler, poolName, defaultTimeoutMillis, threadPoolSize, queueSize) =>  {
         val parallelServiceExecutor = new ParallelServiceExecutor()
         parallelServiceExecutor.setRejectedExecutionHandler(rejectedExecutionHandler)
         parallelServiceExecutor.setPoolName(poolName)
         parallelServiceExecutor.setDefaultTimeoutMillis(defaultTimeoutMillis)
         parallelServiceExecutor.setThreadPoolSize(threadPoolSize)
         parallelServiceExecutor.setQueueSize(queueSize)
         parallelServiceExecutor.init()
         parallelServiceExecutor
     }
    }
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}
