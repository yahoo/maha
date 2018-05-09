package com.yahoo.maha.service.factory

import com.yahoo.maha.executor.druid.NoopAuthHeaderProvider
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.{FunSuite, Matchers}

class AuthHeaderProdiverFactoryTest extends FunSuite with Matchers {
  test("shouldCreateValidNoopAuthHeaderProvider") {
    val jsonDef: String =
      s"""
         |{
         |  "domain" : "Maha",
         |  "service" :"MahaProviderService",
         |  "privateKeyName" : "sa",
         |  "privateKeyId" : "sa"
         |}
       """.stripMargin

    val factory = new NoopAuthHeaderProviderFactory
    val providerTry = factory.fromJson(parse(jsonDef))
    assert(providerTry.isSuccess)
    assert(providerTry.toOption.get.isInstanceOf[NoopAuthHeaderProvider])
    //No local gets or sets on provider, since all variables only run on test boxes.
  }
}
