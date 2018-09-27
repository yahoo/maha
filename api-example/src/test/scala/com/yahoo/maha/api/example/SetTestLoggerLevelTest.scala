package com.yahoo.maha.api.example

import org.scalatest.FunSuite

/*
    Created by pranavbhole on 9/27/18
*/
class SetTestLoggerLevelTest extends FunSuite {

  test("set test logger level to error") {
    org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
      .asInstanceOf[ch.qos.logback.classic.Logger]
      .setLevel(ch.qos.logback.classic.Level.ERROR)
  }

}
