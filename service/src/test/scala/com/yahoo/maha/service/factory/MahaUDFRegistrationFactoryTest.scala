package com.yahoo.maha.service.factory

import org.json4s.jackson.JsonMethods._

/**
 * Created by pranavbhole on 25/04/18.
 */
class MahaUDFRegistrationFactoryTest extends BaseFactoryTest {

  test("successfully build factory from json") {
    val jsonString =     """[{}]"""
    val factoryResult = getFactory[MahaUDFRegistrationFactory]("com.yahoo.maha.service.factory.DefaultMahaUDFRegistrationFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val defaultFactCostEstimatorFactoryResult = factory.fromJson(json)
    assert(defaultFactCostEstimatorFactoryResult.isSuccess, defaultFactCostEstimatorFactoryResult)
    assert(factory.supportedProperties == List.empty)
  }
}
