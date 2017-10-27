// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import com.yahoo.maha.core.dimension.DimCol
import org.scalatest.FunSuite

/**
 * Created by vivekch on 2/24/16.
 */
class HiveLiteralMapperTest extends FunSuite {

  val HiveLiteralMapper = new HiveLiteralMapper

  test("String with quotes") {
    val exp = "' OR 1 = 1 /*"
    val column = ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      val col = DimCol("dimcol", StrType())
      col
    }
    val mapper = HiveLiteralMapper.toLiteral(column, exp, None)
    println(s"String with quotes :before   : $exp")
    println(s"String with quotes :after : $mapper")
    assert(mapper=="''' OR 1 = 1 /*'")
  }

  test("Arbitrary String Patterns") {
    val exp = "NULL/**/UNION/**/ALL/**/SELECT/**/user,pass,/**/FROM/**/user_db/**/WHERE/**/uid/**/=/*evade*/'1'//"
    val column = ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      val col = DimCol("dimcol", StrType())
      col
    }
    val mapper = HiveLiteralMapper.toLiteral(column, exp, None)
    println(s"Arbitrary String Patterns :before   : $exp")
    println(s"Arbitrary String Patterns :after : $mapper")
    val expected = "'NULL/**/UNION/**/ALL/**/SELECT/**/user,pass,/**/FROM/**/user_db/**/WHERE/**/uid/**/=/*evade*/''1''//'"
    assert(mapper == expected)
  }

  test("Wildcard Escaping") {
    val exp = "%/_%' ESCAPE '/"
    val column = ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      val col = DimCol("dimcol", StrType())
      col
    }
    val mapper = HiveLiteralMapper.toLiteral(column, exp, None)
    println(s"Wildcard Escaping :before : $exp")
    println(s"Wildcard Escaping :after : $mapper")
    val expected="'%/_%'' ESCAPE ''/'"
    assert(mapper == expected)
  }

  test("Semicolon statement") {
    val exp = "' ; DROP DATABASE db"
    val column = ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      val col = DimCol("dimcol", StrType())
      col
    }
    val mapper = HiveLiteralMapper.toLiteral(column, exp, None)
    println(s"before : $exp")
    println(s"after : $mapper")
    val expected = "''' ; DROP DATABASE db'"
    assert(mapper == expected)
  }

  test("Invalid IntType") {
    val exp = "' ; DROP DATABASE db"
    val column = ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      val col = DimCol("dimcol", IntType())
      col
    }
    intercept[java.lang.NumberFormatException]
    {
        HiveLiteralMapper.toLiteral(column,exp,None)
    }
  }

  test("valid IntType") {
    val exp = "123"
    val column = ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      val col = DimCol("dimcol", IntType())
      col
    }
    val intValue = HiveLiteralMapper.toLiteral(column, exp, None)
    println("Returned int value " + intValue)
    assert(exp == intValue)
  }

  test("Invalid DecType") {
    val exp = "' ; DROP DATABASE db"
    val column = ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      val col = DimCol("dimcol", DecType())
      col
    }
    intercept[java.lang.NumberFormatException]{
        HiveLiteralMapper.toLiteral(column,exp,None)
    }
  }

  test("valid DecType") {
    val exp = "113.444"
    val column = ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      val col = DimCol("dimcol", DecType())
      col
    }
    val decValue = HiveLiteralMapper.toLiteral(column, exp, None)
    println("Returned decimal value : " + decValue)
    assert(exp == decValue)
  }

  test("valid DateType") {
    val exp = "2016-02-24"
    val column = ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      val col = DimCol("dimcol", DateType("YYYYMMdd"))
      col
    }
    val dateValue = HiveLiteralMapper.toLiteral(column, exp, None)
    println("Returned date : " + dateValue)
    val expected = "'20160224'"
    assert(dateValue == expected)
  }


  test("invalid DateType") {
    val exp = "' ; DROP DATABASE db"
    val column = ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      val col = DimCol("dimcol", DateType("YYYYMMdd"))
      col
    }
    intercept[IllegalArgumentException] {
      val dateValue = HiveLiteralMapper.toLiteral(column, exp, None)
    }
  }


  test("invalid TimeStampType") {
    val exp = "12428384' ; DROP DATABASE db"
    val column = ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      val col = DimCol("dimcol",TimestampType())
      col
    }
    val dateValue = HiveLiteralMapper.toLiteral(column, exp, None)
    println("Returned timeStamp : " + dateValue)
    val expected = "'12428384'' ; DROP DATABASE db'"
    assert(dateValue == expected)
  }
}
