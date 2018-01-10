// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.fact

import com.yahoo.maha.core.DruidDerivedFunction.GET_INTERVAL_DATE
import com.yahoo.maha.core._
import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.DruidPostResultFunction.POST_RESULT_DECODE
import com.yahoo.maha.core.ddl.OracleDDLAnnotation
import com.yahoo.maha.core.dimension.{DimCol, DruidFuncDimCol, OracleDerDimCol}

/**
 * Created by jians on 10/20/15.
 */
class NewFactTest extends BaseFactTest {
  test("newFact should fail with duplicate columns") {
    intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        Fact.newFact(
          "fact1", DailyGrain, HiveEngine, Set(AdvertiserSchema),
          Set(
            DimCol("account_id", IntType(), annotations = Set(ForeignKey("cache_advertiser_metadata")))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
            , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          ),
          Set(
            FactCol("impressions", IntType())
            , FactCol("clicks", IntType())
          )
        )
      }
    }
  }

  test("newFact should succeed with no duplicate columns") {
    val fact = fact1
    assert(fact != null)
  }

  test("newFact should fail with columns with different engine requirement") {
    val thrown = intercept[IllegalArgumentException] {
      val fact : FactBuilder = {
        import com.yahoo.maha.core.OracleExpression._
        ColumnContext.withColumnContext { implicit cc =>
          Fact.newFact("dim1", DailyGrain, HiveEngine, Set(AdvertiserSchema),
            Set(
              DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
              , DimCol("dimcol2", IntType())
              , OracleDerDimCol("Campaign Start Date Full", StrType(), TIMESTAMP_TO_FORMATTED_DATE("{start_time}", "YYYY-MM-dd HH:mm:ss"))
            ), Set(
              FactCol("factcol1", StrType())
            )
          )
        }
      }
    }
    thrown.getMessage should startWith ("requirement failed: Failed engine requirement")
  }

  test("newFact should fail with fact annotated with different engine requirement") {
    val thrown = intercept[IllegalArgumentException] {
      val fact :  FactBuilder= {
        ColumnContext.withColumnContext { implicit cc =>
          Fact.newFact("dim1",  DailyGrain, OracleEngine, Set(AdvertiserSchema),
            Set(
              DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
              , DimCol("dimcol2", IntType())
            ), Set(
              FactCol("factCol1", IntType())
              , FactCol("factCol2", IntType())
            )
            , annotations = Set(HiveStorageFormatAnnotation("format"))
          )
        }
      }
    }
    thrown.getMessage should startWith ("requirement failed: Failed engine requirement")
  }

  test("newFact should fail with column annotated with different engine requirement") {
    val thrown = intercept[IllegalArgumentException] {
      val fact : FactBuilder = {
        ColumnContext.withColumnContext { implicit cc =>
          Fact.newFact("dim1", DailyGrain, OracleEngine, Set(AdvertiserSchema),
            Set(
              DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
              , DimCol("dimcol2", IntType(), annotations = Set(HiveSnapshotTimestamp))
            ),
            Set(
              FactCol("factCol1", IntType())
              , FactCol("factCol2", IntType())
            )
          )
        }
      }
    }
    thrown.getMessage should startWith ("requirement failed: Failed engine requirement")
  }

  test("newFact should fail if there's no reference column fo derived fact column") {
    val thrown = intercept[IllegalArgumentException] {
      val fact : FactBuilder = {
        import com.yahoo.maha.core.OracleExpression._
        ColumnContext.withColumnContext { implicit cc =>
          Fact.newFact("dim1", DailyGrain, OracleEngine, Set(AdvertiserSchema),
            Set(
              DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
              , DimCol("dimcol2", IntType())
            ), Set(
              FactCol("factcol1", StrType())
              , OracleDerFactCol("Campaign Start Date Full", StrType(), TIMESTAMP_TO_FORMATTED_DATE("{start_time}", "YYYY-MM-dd HH:mm:ss"))

            )
          )
        }
      }
    }
    thrown.getMessage should startWith ("requirement failed: Failed derived expression validation, unknown referenced column in")
  }

  test("newFact should fail if there's no reference column fo derived dim column") {
    val thrown = intercept[IllegalArgumentException] {
      val fact : FactBuilder = {
        import com.yahoo.maha.core.OracleExpression._
        ColumnContext.withColumnContext { implicit cc =>
          Fact.newFact("dim1", DailyGrain, OracleEngine, Set(AdvertiserSchema),
            Set(
              DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
              , DimCol("dimcol2", IntType())
              , OracleDerDimCol("Campaign Start Date Full", StrType(), TIMESTAMP_TO_FORMATTED_DATE("{start_time}", "YYYY-MM-dd HH:mm:ss"))
            ), Set(
              FactCol("factcol1", StrType())
            )
          )
        }
      }
    }
    thrown.getMessage should startWith ("requirement failed: Failed derived expression validation, unknown referenced column in")
  }

  test("newFact should fail if ddl annotation is of a different engine other than the engine of the fact") {
    val thrown = intercept[IllegalArgumentException] {
      val fact : FactBuilder = {
        ColumnContext.withColumnContext { implicit cc =>
          Fact.newFact("fact", DailyGrain, HiveEngine, Set(AdvertiserSchema),
            Set(
              DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
            ),
            Set(
              FactCol("factcol1", IntType())
            ),
            ddlAnnotation = Option(OracleDDLAnnotation())
          )
        }
      }
    }
    thrown.getMessage should startWith ("requirement failed: Failed engine requirement fact=fact, engine=Hive, ddlAnnotation=Some(OracleDDLAnnotation(Set(),Set()))")
  }

  test("newFact should fail if derived expression is not defined for derived columns") {
    val thrown = intercept[IllegalArgumentException] {
      val fact : FactBuilder = {
        ColumnContext.withColumnContext { implicit cc =>
          Fact.newFact("fact", DailyGrain, OracleEngine, Set(AdvertiserSchema),
            Set(
              DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
              , OracleDerDimCol("derived_dim", IntType(), null)
            ),
            Set(
              FactCol("factcol1", IntType())
            ),
            ddlAnnotation = Option(OracleDDLAnnotation())
          )
        }
      }
    }
    thrown.getMessage should startWith ("requirement failed: Derived expression should be defined for a derived column")
  }

  test("newFact should fail if columns have difference column context") {
    import org.mockito.Mockito._
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc =>
        val mockedDimCol : DimCol = mock(classOf[DimCol])
        val mockedCC : ColumnContext = mock(classOf[ColumnContext])
        when(mockedDimCol.name).thenReturn("mockedCol")
        when(mockedDimCol.alias).thenReturn(None)
        when(mockedDimCol.annotations).thenReturn(Set.empty[ColumnAnnotation])
        when(mockedDimCol.columnContext).thenReturn(mockedCC)

        Fact.newFact("fact1", DailyGrain, HiveEngine, Set(AdvertiserSchema),
          Set(
            DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
            , mockedDimCol
            , DimCol("dimcol2", IntType())),
          Set(
            FactCol("factcol1", StrType())
          ))
      }
    }
    thrown.getMessage should startWith ("requirement failed: fact=fact1 : ColumnContext must be same for all columns : mockedCol")
  }

  test("Validation on FactCol with DruidFilteredRollup should fail if filter refers to invalid dimension column") {

    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc =>
        new FactTable("base_fact", 9999, DailyGrain, DruidEngine, Set(AdvertiserSchema),
          Set(
            DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
            , DimCol("dimcol2", IntType())),
          Set(
            FactCol("factcol1", StrType()),
            FactCol("factcol2", StrType(), DruidFilteredRollup(EqualityFilter("dimcol3", "1"), "factcol1", SumRollup))
          ), None, Set.empty, None, Fact.DEFAULT_COST_MULTIPLIER_MAP,Set.empty, 10,100, None, None, None, None)
      }
    }
    thrown.getMessage should startWith ("requirement failed: Column doesn't exist: dimcol3")
  }

  test("Validation on FactCol with DruidFilteredListRollup should fail if filter refers to invalid dimension column") {

    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc =>
        new FactTable("base_fact", 9999, DailyGrain, DruidEngine, Set(AdvertiserSchema),
          Set(
            DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
            , DimCol("dimcol2", IntType())),
          Set(
            FactCol("factcol1", StrType()),
            FactCol("factcol2", StrType(), DruidFilteredListRollup(List(EqualityFilter("dimcol3", "1")), "factcol1", SumRollup))
          ), None, Set.empty, None, Fact.DEFAULT_COST_MULTIPLIER_MAP,Set.empty, 10,100, None, None, None, None)
      }
    }
    thrown.getMessage should startWith ("requirement failed: Column doesn't exist: dimcol3")
  }

  test("Validation on FactCol with DruidFilteredRollup should fail if it refers to non-existent fact column") {

    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc =>
        new FactTable("base_fact", 9999, DailyGrain, DruidEngine, Set(AdvertiserSchema),
          Set(
            DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
            , DimCol("dimcol2", IntType())),
          Set(
            FactCol("factcol1", StrType()),
            FactCol("factcol2", StrType(), DruidFilteredRollup(EqualityFilter("dimcol2", "1"), "nonfactcol", SumRollup))
          ), None, Set.empty, None, Fact.DEFAULT_COST_MULTIPLIER_MAP,Set.empty,10,100,None, None, None, None)
      }
    }
    thrown.getMessage should startWith ("requirement failed: Column doesn't exist: nonfactcol")
  }

  test("Validation on DimCol with DruidFuncDimCol should fail if it refers to non-existent Dim column") {

    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc =>
        new FactTable("base_fact", 9999, DailyGrain, DruidEngine, Set(AdvertiserSchema),
          Set(
            DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
            , DimCol("dimcol2", IntType())
            , DruidFuncDimCol("Week", DateType(), GET_INTERVAL_DATE("{dimcol3}", "w"))),
          Set(
            FactCol("factcol1", StrType()),
            FactCol("factcol2", StrType())
          ), None, Set.empty, None, Fact.DEFAULT_COST_MULTIPLIER_MAP,Set.empty,10,100,None, None, None, None)
      }
    }
    thrown.getMessage should startWith ("requirement failed: Column doesn't exist: dimcol3")
  }

  test("Validation on PostResultDerivedFactCol should fail if it refers to non-existent column") {

    val thrown = intercept[IllegalArgumentException] {
      import DruidExpression._
      ColumnContext.withColumnContext { implicit cc =>
        new FactTable("base_fact", 9999, DailyGrain, DruidEngine, Set(AdvertiserSchema),
          Set(
            DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
            , DimCol("dimcol2", IntType())
            ),
          Set(
            FactCol("factcol1", StrType()),
            FactCol("factcol2", StrType()),
            DruidPostResultDerivedFactCol("factcol3", StrType(), "{factcol1}" /- "{factcol2}", postResultFunction = POST_RESULT_DECODE("{dimcol3}", "0", "N/A"))
          ), None, Set.empty, None, Fact.DEFAULT_COST_MULTIPLIER_MAP,Set.empty,10,100,None, None, None, None)
      }
    }
    thrown.getMessage should startWith ("requirement failed: Column doesn't exist: dimcol3")
  }
}


