package com.yahoo.maha

import java.io.Closeable

import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Created by hiral on 3/9/18.
  */
package object report {
  private lazy val logger = LoggerFactory.getLogger("safeOperation")
  def safeOperation[A, B](operation: A)(cleanup: A => Unit)(doWork: A => B): Try[B] = {
    try {
      Success(doWork(operation))
    } catch {
      case e: Exception =>
        logger.error("Failed on safeOperation doWork", e)
        try {
          if (operation != null) {
            cleanup(operation)
          }
        } catch {
          case e: Exception =>
            logger.error("Failed on safeOperation cleanup", e)
        }
        Failure(e)
    }
  }

  def safeCloseable[A <: Closeable, B](closeable: A)(doWork: A => B): Try[B] = {
    safeOperation(closeable)(_.close())(doWork)
  }

}
