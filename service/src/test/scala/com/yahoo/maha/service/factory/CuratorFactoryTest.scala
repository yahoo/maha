package com.yahoo.maha.service.factory

import com.yahoo.maha.service.curators._
import org.json4s.jackson.JsonMethods._

/**
 * Created by pranavbhole on 25/04/18.
 */
class CuratorFactoryTest extends BaseFactoryTest {
  test("Test DrillDown Curator Factory") {
    val factoryResult = getFactory[CuratorFactory]("com.yahoo.maha.service.factory.DrillDownCuratorFactory", closer)
    assert(factoryResult.isSuccess)
    val factory:CuratorFactory = factoryResult.toOption.get
    assert(factory.isInstanceOf[DrillDownCuratorFactory])
    assert(factory.supportedProperties.isEmpty)
    val json = parse("{}")
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[DrilldownCurator])
  }

  test("Test TimeShift Curator Factory") {
    val factoryResult = getFactory[CuratorFactory]("com.yahoo.maha.service.factory.TimeShiftCuratorFactory", closer)
    assert(factoryResult.isSuccess)
    val factory:CuratorFactory = factoryResult.toOption.get
    assert(factory.isInstanceOf[TimeShiftCuratorFactory])
    assert(factory.supportedProperties.isEmpty)
    val json = parse("{}")
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[TimeShiftCurator])
  }

  test("Test DefaultCuratorFactory") {
    val factoryResult = getFactory[CuratorFactory]("com.yahoo.maha.service.factory.DefaultCuratorFactory", closer)
    assert(factoryResult.isSuccess)
    val factory:CuratorFactory = factoryResult.toOption.get
    assert(factory.isInstanceOf[DefaultCuratorFactory])
    assert(factory.supportedProperties.isEmpty)
    val json = parse("{}")
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[DefaultCurator])
  }

  test("Test RowCountCuratorFactory") {
    val factoryResult = getFactory[CuratorFactory]("com.yahoo.maha.service.factory.RowCountCuratorFactory", closer)
    assert(factoryResult.isSuccess)
    val factory:CuratorFactory = factoryResult.toOption.get
    assert(factory.isInstanceOf[RowCountCuratorFactory])
    assert(factory.supportedProperties.isEmpty)
    val json = parse("{}")
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[RowCountCurator])
  }

  test("Test TotalMetricsCuratorFactory") {
    val factoryResult = getFactory[CuratorFactory]("com.yahoo.maha.service.factory.TotalMetricsCuratorFactory", closer)
    assert(factoryResult.isSuccess)
    val factory:CuratorFactory = factoryResult.toOption.get
    assert(factory.isInstanceOf[TotalMetricsCuratorFactory])
    assert(factory.supportedProperties.isEmpty)
    val json = parse("{}")
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[TotalMetricsCurator])
  }

}
