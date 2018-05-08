// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import org.scalatest.{FunSuite, Matchers}
/**
 * Created by shengyao on 2/10/16.
 */
class DataTypeTest extends FunSuite with Matchers {
  test("IntType should not have negative length") {
    val thrown1 = intercept[IllegalArgumentException] {
      IntType(-1)
    }
    thrown1.getMessage should startWith ("requirement failed: IntType(length) : invalid argument : length > 0")

    val thrown2 = intercept[IllegalArgumentException] {
      IntType(-1, 2)
    }
    thrown2.getMessage should startWith ("requirement failed: IntType(length, default) : invalid argument : length >= 0")

    val thrown3 = intercept[IllegalArgumentException] {
      IntType(-1, (Map(1 -> "One"), "None"))
    }
    thrown3.getMessage should startWith ("requirement failed: IntType(length, staticMapping) : invalid argument : length > 0")

    val thrown4 = intercept[IllegalArgumentException] {
      IntType(-1, 1, 3, 5)
    }
    thrown4.getMessage should startWith ("requirement failed: IntType(length, default, min, max) : invalid argument : length > 0")
  }

  test("IntType should successfully being constructed with length and default value") {
    val int = IntType(3, 3)
    assert(int.default.isDefined && int.default.get == 3, s"Default value is not defined successfully for $int")
  }

  test("StrType should not have negative length") {
    val thrown1 = intercept[IllegalArgumentException] {
      StrType(-1)
    }
    thrown1.getMessage should startWith ("requirement failed: StrType(length) : invalid argument : length > 0")

    val thrown2 = intercept[IllegalArgumentException] {
      StrType(-1, "Str")
    }
    thrown2.getMessage should startWith ("requirement failed: StrType(length, default) : invalid argument : length >= 0")

    val thrown3 = intercept[IllegalArgumentException] {
      StrType(-1, (Map("One" -> "One"), "NONE"))
    }
    thrown3.getMessage should startWith ("requirement failed: StrType(length, staticMapping) : invalid argument : length > 0")

    val thrown4 = intercept[IllegalArgumentException] {
      StrType(-1, (Map("One" -> "One"), "NONE"), "Str")
    }
    thrown4.getMessage should startWith ("requirement failed: StrType(length, staticMapping, default) : invalid argument : length > 0")
  }

  test("StrType should successfully being constructed with length and default value") {
    val str = StrType(3, "Hello")
    assert(str.default.isDefined && str.default.get == "Hello", s"Default value is not defined successfully for $str")
  }

  test("DecType should not have negative length") {
    val thrown1 = intercept[IllegalArgumentException] {
      DecType(-1)
    }
    thrown1.getMessage should startWith ("requirement failed: DecType(length) : invalid argument : length > 0")

    val thrown2 = intercept[IllegalArgumentException] {
      DecType(-1, 0)
    }
    thrown2.getMessage should startWith ("requirement failed: DecType(length, scale) : invalid argument : length > 0")

    val thrown3 = intercept[IllegalArgumentException] {
      DecType(-1, "4.4")
    }
    thrown3.getMessage should startWith ("requirement failed: DecType(length, default) : invalid argument : length >= 0")

    val thrown4 = intercept[IllegalArgumentException] {
      DecType(-1, 0, "4.4")
    }
    thrown4.getMessage should startWith ("requirement failed: DecType(length, scale, default) : invalid argument : length >= 0")

    val thrown5 = intercept[IllegalArgumentException] {
      DecType(-1, "0.0", "2.0", "3.0")
    }
    thrown5.getMessage should startWith ("requirement failed: DecType(length, default, min, max) : invalid argument : length >= 0")

    val thrown6 = intercept[IllegalArgumentException] {
      DecType(-1, 0, "0.0", "2.0", "3.0")
    }
    thrown6.getMessage should startWith ("requirement failed: DecType(length, scale, default, min, max) : invalid argument : length > 0")

  }

  test("DecType should not have negative scale") {
    val thrown = intercept[IllegalArgumentException] {
      DecType(1, -1)
    }
    thrown.getMessage should startWith ("requirement failed: DecType(length, scale) : invalid argument : scale >= 0")

    val thrown2 = intercept[IllegalArgumentException] {
      DecType(1, -1, "4.4")
    }
    thrown2.getMessage should startWith ("requirement failed: DecType(length, scale, default) : invalid argument : scale >= 0")
  }

  test("DecType should not have scale that's larger than the length") {
    val thrown = intercept[IllegalArgumentException] {
      DecType(1, 3)
    }
    thrown.getMessage should startWith ("requirement failed: DecType(length, scale) : invalid argument : length >= scale")
  }

  test("DecType should successfully being constructed with length and default") {
    val dec = DecType(3, "3.3")
    assert(dec.default.isDefined && dec.default.get == BigDecimal(3.3), s"Default value is not define successfully for $dec")
  }

  test("DateType with invalid format should fail") {
    val thrown = intercept[IllegalArgumentException] {
      val date = DateType("XXXX-YY-ZZ")
    }
    thrown.getMessage should startWith ("requirement failed: Invalid format for DateType(XXXX-YY-ZZ)")
  }

  test("DecType success cases") {
    val validLength = DecType(1)
    assert (validLength.length == 1, "Length value for declared DecType should be 1, but found " + validLength.length)

    val validLengthWithScaleAndDefault = DecType(1, 1, "5")
    assert (validLengthWithScaleAndDefault.default == Some(5), "Default value for declared DecType should be Some(5), but found " + validLengthWithScaleAndDefault.default)

    val validLengthWithScaleDefaultMinMax = DecType(1, 1, "5", "0", "9")
    assert (validLengthWithScaleDefaultMinMax.min == Some(0), "Min value for declared DecType should be Some(0), but found " + validLengthWithScaleDefaultMinMax.min)
    assert (validLengthWithScaleDefaultMinMax.max == Some(9), "Max value for declared DecType should be Some(9), but found " + validLengthWithScaleDefaultMinMax.max)
  }

  test("TimestampType test for valid/invalid format") {
    val thrown = intercept[IllegalArgumentException] {
      TimestampType("")
    }
    assert(thrown.getMessage.contains("invalid argument : format cannot be null or empty"), "Should throw an empty formatting error")
    val validTimestamp = TimestampType("MM")
    
  }

}
