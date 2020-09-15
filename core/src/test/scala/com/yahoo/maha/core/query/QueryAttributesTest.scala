package com.yahoo.maha.core.query

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class QueryAttributesTest extends AnyFunSuite with Matchers{
  test("QueryAttribute Creation") {
    val longAttrib : LongAttribute = new LongAttribute(1)
    assert(longAttrib.toString == "1")
    val stringAttribute : StringAttribute = new StringAttribute("10")
    assert(stringAttribute.toString == "10")
  }

  test("QueryAttributes Listing failure/success cases") {
    val longAttrib : LongAttribute = new LongAttribute(1)
    assert(longAttrib.toString == "1")
    val stringAttribute : StringAttribute = new StringAttribute("10")
    assert(stringAttribute.toString == "10")
    val attributes : QueryAttributes = new QueryAttributes(Map("1" -> longAttrib, "10" -> stringAttribute))
    assert(attributes.getAttribute("1").equals(longAttrib))
    assert(attributes.getAttributeOption("10").equals(Option(stringAttribute)))

    val getThrown = intercept[IllegalArgumentException] {
      attributes.getAttribute("fake attribute")
    }
    assert(getThrown.getMessage.equals("requirement failed: Attribute is not defined: fake attribute"))
  }
}
