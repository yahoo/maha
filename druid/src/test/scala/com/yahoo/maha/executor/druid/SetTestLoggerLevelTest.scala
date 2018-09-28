package com.yahoo.maha.executor.druid

import org.scalatest.FunSuite

/*
    Created by pranavbhole on 9/27/18
*/
class SetTestLoggerLevelTest extends FunSuite {
  val logger  = org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
  logger match {
    case classicLogger:ch.qos.logback.classic.Logger =>
      classicLogger.setLevel(ch.qos.logback.classic.Level.ERROR)
      println("Setting Logger Level to ERROR")
    case _=>
  }

  test("set test logger level to error") {
  }

}
