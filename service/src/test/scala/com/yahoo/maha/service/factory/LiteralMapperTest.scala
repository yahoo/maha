// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import com.yahoo.maha.core.dimension.DimCol
import com.yahoo.maha.core._
import org.json4s.jackson.JsonMethods._

/**
 * Created by pranavbhole on 31/05/17.
 */
class LiteralMapperTest extends BaseFactoryTest {
  test("Test OracleLiteralMapper ") {
    val factoryResult = getFactory[OracleLiteralMapperFactory]("com.yahoo.maha.service.factory.DefaultOracleLiteralMapperFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    assert(factory.supportedProperties.isEmpty)
    val json = parse("{}")
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[OracleLiteralMapper])
  }

  test("Test DruidLiteralMapper ") {
    val factoryResult = getFactory[DruidLiteralMapperFactory]("com.yahoo.maha.service.factory.DefaultDruidLiteralMapperFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    assert(factory.supportedProperties.isEmpty)
    val json = parse("{}")
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[DruidLiteralMapper])
    val druidMapper : DruidLiteralMapper = generatorResult.toList.head.asInstanceOf[DruidLiteralMapper]

    implicit val cc: ColumnContext = new ColumnContext
    val col = DimCol("field1", IntType())
    val colDateNoFormat = DimCol("field1_date", DateType())
    val colDateWithFormat = DimCol("field1_date_fmt", DateType("YYYY-MM-dd"))
    val colTSNoFmt = DimCol("field1_ts", TimestampType())
    val colTSWithFmt = DimCol("field1_ts_fmt", TimestampType("YYYY-MM-dd"))

    assert(druidMapper.toDateTime(col, "2018-01-01", Option(DailyGrain)).toString == "2018-01-01T00:00:00.000Z")
    assert(druidMapper.toDateTime(colDateNoFormat, "2018-01-01", Option(DailyGrain)).toString == "2018-01-01T00:00:00.000Z")
    assert(druidMapper.toDateTime(colDateWithFormat, "2018-01-01", Option(DailyGrain)).toString == "2018-01-01T00:00:00.000Z")
    assert(druidMapper.toDateTime(colTSNoFmt, "2018-01-01", Option(DailyGrain)).toString == "2018-01-01T00:00:00.000Z")
    assert(druidMapper.toDateTime(colTSWithFmt, "2018-01-01", Option(DailyGrain)).toString == "2018-01-01T00:00:00.000Z")
    assert(druidMapper.toLiteral(col, "2018-01-01", Option(DailyGrain)).toString == "2018-01-01")
    assert(druidMapper.toLiteral(colTSNoFmt, "2018-01-01", Option(DailyGrain)).toString == "2018-01-01")
    assert(druidMapper.toLiteral(colTSWithFmt, "2018-01-01", Option(DailyGrain)).toString == "2018-01-01")

  }

}
