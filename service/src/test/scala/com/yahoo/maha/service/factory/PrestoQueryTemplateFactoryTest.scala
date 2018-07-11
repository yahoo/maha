package com.yahoo.maha.service.factory

import com.yahoo.maha.executor.presto.PrestoQueryTemplate
import com.yahoo.maha.service.{DefaultMahaServiceConfigContext, MahaServiceConfigContext}
import org.json4s.jackson.JsonMethods._

/**
 * Created by pranavbhole on 25/04/18.
 */
class PrestoQueryTemplateFactoryTest extends BaseFactoryTest {
  implicit val context: MahaServiceConfigContext = DefaultMahaServiceConfigContext()

  test("Test PrestoQueryTemplateFactory ") {
    val factoryResult = getFactory[PrestoQueryTemplateFactory]("com.yahoo.maha.service.factory.DefaultPrestoQueryTemplateFactory", closer)
    assert(factoryResult.isSuccess)
    val factory:PrestoQueryTemplateFactory = factoryResult.toOption.get
    assert(factory.isInstanceOf[PrestoQueryTemplateFactory])
    assert(factory.supportedProperties.isEmpty)
    val json = parse("{}")
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[PrestoQueryTemplate])
  }
}
