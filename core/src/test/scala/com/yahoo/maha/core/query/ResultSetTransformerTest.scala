package com.yahoo.maha.core.query

import com.yahoo.maha.core._
import org.scalatest.{FunSuite, Matchers}

class ResultSetTransformerTest extends FunSuite with Matchers{
  test("Create a ResultSetTransformer") {
    val rst = DateTransformer
    val bd = BigDecimal(10)
    assert(rst.apply().extractBigDecimal(10).equals(bd))
    assert(rst.apply().extractBigDecimal("10").equals(BigDecimal(0.0)))
    assert(rst.apply().extractBigDecimal(BigInt(10)).equals(bd))
    assert(rst.apply().extractBigDecimal(10.asInstanceOf[Double]).equals(bd))
    assert(rst.apply().extractBigDecimal(10.asInstanceOf[Float]).equals(bd))
    assert(rst.apply().extractBigDecimal(10.asInstanceOf[Long]).equals(bd))

    val decCol = new TestCol {
      override def dataType : DataType = DateType("YYYYMMDD")
    }

    val retVal = rst.apply().transform(DailyGrain, "Day", decCol, "20180101")
    println(retVal)
  }

  test("Successful ResultSet conversion") {
    val bigDecimalTransformer = new NumberTransformer
    val bigDecimal : BigDecimal = 0.0
    val floatInput : Float = 1
    assert(bigDecimalTransformer.extractBigDecimal(floatInput).getClass == bigDecimal.getClass, "Output should be a BigDecimal, but found " + bigDecimalTransformer.extractBigDecimal(floatInput).getClass)
    val longInput : Long = 10
    assert(bigDecimalTransformer.extractBigDecimal(longInput).getClass == bigDecimal.getClass, "Output should be a BigDecimal, but found " + bigDecimalTransformer.extractBigDecimal(longInput).getClass)
    val intInput : Int = 53
    assert(bigDecimalTransformer.extractBigDecimal(intInput).getClass == bigDecimal.getClass, "Output should be a BigDecimal, but found " + bigDecimalTransformer.extractBigDecimal(intInput).getClass)
    val fallthroughStringInput : String = "Should not be returned"
    assert(bigDecimalTransformer.extractBigDecimal(fallthroughStringInput).getClass == bigDecimal.getClass, "Output should be a BigDecimal, but found " + bigDecimalTransformer.extractBigDecimal(fallthroughStringInput).getClass)

    val decCol = new TestCol {
      override def dataType : DataType = DecType()
    }

    val retVal = bigDecimalTransformer.transform(DailyGrain, "Day", decCol, 10)
    println(retVal)

    val intCol = new TestCol {
      override def dataType : DataType = IntType(1, 0)
    }
    val decColLen = new TestCol {
      override def dataType : DataType = DecType(1, 0)
    }
    val decColScale = new TestCol {
      override def dataType : DataType = DecType(1, 1)
    }
    val decColBoth = new TestCol {
      override def dataType : DataType = DecType(1, 1)
    }

    val retVal2 = bigDecimalTransformer.transform(DailyGrain, "Day", intCol, "15")
    println(retVal2)
    val retVal3 = bigDecimalTransformer.transform(DailyGrain, "Day", decColLen, 10)
    println(retVal3)
    val retVal4 = bigDecimalTransformer.transform(DailyGrain, "Day", decColScale, 10)
    println(retVal4)
    val retVal5 = bigDecimalTransformer.transform(DailyGrain, "Day", decColBoth, 10)
    println(retVal5)

    assert(!ResultSetTransformer.DEFAULT_TRANSFORMS.isEmpty)
    assert(bigDecimalTransformer.transform(DailyGrain, "NOT_DAY", decCol, 10) == 10)
  }


  abstract class TestCol extends Column {
    override def alias: Option[String] = None
    override def filterOperationOverrides: Set[FilterOperation] = Set.empty
    override def isDerivedColumn: Boolean = false
    override def name: String = "test"
    override def annotations: Set[ColumnAnnotation] = Set.empty
    override def columnContext: ColumnContext = null
    override def dataType: DataType = ???
  }
}
