// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.fact

import com.yahoo.maha.core._
import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.ddl.HiveDDLAnnotation
import com.yahoo.maha.core.dimension.{DimCol, OracleDerDimCol}
import com.yahoo.maha.core.request.{AsyncRequest, SyncRequest}

/**
 * Created by jians on 10/20/15.
 */
class WithAlternativeEngineFactTest extends BaseFactTest {

  test("withAlternativeEngine should success with new name and new engine") {
    val fact = fact1
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withAlternativeEngine("fact2", "fact1", OracleEngine, maxDaysWindow = Some(Map(AsyncRequest -> 31, SyncRequest -> 31)), maxDaysLookBack = Some(Map(AsyncRequest -> 31, SyncRequest -> 31)))
    }
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.values.exists( f => f.fact.name == "fact2") === true)
  }

  test("Reject the facts which are running out of lookback days and days window") {
    val fact = fact1WithFactLevelMaxLookbackWindow
    // Requesting Data for more days > limit
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 100, 100, EqualityFilter("Day", s"$toDate"))
    
    require(bcOption.get.facts.isEmpty, "Failed to reject the fact  out of lookback days and days window ")
  }

  test("withAlternativeEngine should success with new name and new engine without copying force filters") {
    val fact = fact1WithForceFilters(Set(ForceFilter(InFilter("Pricing Type", List("1"), isForceFilter = true))))
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withAlternativeEngine("fact2", "fact1", OracleEngine)
    }
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.values.exists( f => f.fact.name == "fact2") === true)
    assert(bcOption.get.facts.values.find( f => f.fact.name == "fact2").get.fact.forceFilters.isEmpty)
  }

  test("withAlternativeEngine should fail with non existing from") {
    val fact = fact1
    intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withAlternativeEngine("fact2", "factblah", OracleEngine)
      }
    }
  }

  test("withAlternativeEngine should fail with same engine") {
    val fact = fact1
    intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withAlternativeEngine("fact2", "fact1", HiveEngine)
      }
    }
  }

  test("withAlternativeEngine should fail with existing name") {
    val fact = fact1
    intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withAlternativeEngine("fact1", "fact2", OracleEngine)
      }
    }
  }

  test("withAlternativeEngine should fail with existing name after a rollup") {
    val fact = fact1
    intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.newRollUp("fact1_1", "fact1", Set("ad_id"))
        fact.withAlternativeEngine("fact1", "fact1_1", OracleEngine)
      }
    }
  }
  
  test("withAlternativeEngine should fail if tableMap is empty") {
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc =>
        val base_fact = new FactTable("base_fact", 9999, DailyGrain, OracleEngine, Set(AdvertiserSchema),
          Set(
            DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
            , DimCol("dimcol2", IntType())),
          Set(
            FactCol("factcol1", StrType())
          ), None, Set.empty, None, Fact.DEFAULT_COST_MULTIPLIER_MAP,Set.empty,10,100,None, None, None, None, None)
        val fb = new FactBuilder(base_fact, Map(), None)
        fb.withAlternativeEngine("fact2", "base_fact", HiveEngine)
      }
    }
    thrown.getMessage should startWith ("requirement failed: no tables found")
  }

  test("withAlternativeEngine should fail with existing engine") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withAlternativeEngine("fact2", "fact1", OracleEngine)
        fact.withAlternativeEngine("fact3", "fact1", OracleEngine)
      }
    }
    thrown.getMessage should startWith("requirement failed: Alternate must have different engine from existing alternate engines")
  }

  test("withAlternativeEngine should fail with missing override for fact column annotation with engine requirement") {
    val fact = fact1WithAnnotationWithEngineRequirement
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withAlternativeEngine("fact2", "fact1", OracleEngine, overrideFactCols = Set(
        ))
      }
    }
    thrown.getMessage should include("missing fact annotation overrides = List((clicks,Set(HiveSnapshotTimestamp))),")
  }

  test("withAlternativeEngine should fail with missing override for fact column rollup with engine requirement") {
    val fact = fact1WithRollupWithEngineRequirement
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withAlternativeEngine("fact2", "fact1", OracleEngine, overrideFactCols = Set(
        ))
      }
    }
    thrown.getMessage should include("missing fact rollup overrides = List((clicks,HiveCustomRollup(")
  }

  test("withAlternativeEngine should fail if ddl annotation is of a different engine other than the engine of the fact") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit  cc : ColumnContext =>
        fact.withAlternativeEngine("fact2", "fact1", OracleEngine, overrideDDLAnnotation = Option(HiveDDLAnnotation()))
      }
    }
    thrown.getMessage should startWith ("requirement failed: Failed engine requirement fact=fact2, engine=Oracle, ddlAnnotation=Some(HiveDDLAnnotation(Map(),Vector()))")
  }

  test("withAlternativeEngine should fail if derived expression is not defined for derived columns") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext {implicit cc : ColumnContext =>
        fact.withAlternativeEngine("fact2", "fact1", OracleEngine,
          Set(
            OracleDerDimCol("derived_col", IntType(), null)
          )
        )
      }
    }
    thrown.getMessage should startWith ("requirement failed: Derived expression should be defined for a derived column")
  }

  test("withAlternativeEngine should fail if override dim columns have static mapping") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext {implicit cc : ColumnContext =>
        import OracleExpression._
        fact.withAlternativeEngine("fact2", "fact1", OracleEngine,
          Set(
              OracleDerDimCol("price_type", IntType(3, (Map(1 -> "One"), "NONE")), DECODE_DIM("{account_id}", "0", "zero", "{account_id}"))
          )
        )
      }
    }
    thrown.getMessage should startWith ("requirement failed: Override column cannot have static mapping")
  }

  test("withAlternativeEngine should fail if override fact columns have static mapping") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext {implicit cc : ColumnContext =>
        import OracleExpression._
        fact.withAlternativeEngine("fact2", "fact1", OracleEngine,
          overrideFactCols = Set(
              OracleDerFactCol("clicks", IntType(3, (Map(1 -> "One"), "NONE")), SUM("{account_id}"))
          )
        )
      }
    }
    thrown.getMessage should startWith ("requirement failed: Override column cannot have static mapping")
  }

  test("withAlternativeEngine: dim column override failure case") {
    val fact = factDerivedWithFailingDimCol
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withAlternativeEngine("fact2", "fact1", OracleEngine
          , availableOnwardsDate = Option("2017-09-30"))
      }
    }
    thrown.getMessage should startWith ("requirement failed: name=fact2, from=fact1, engine=Oracle, missing dim overrides = List((price_type,Set(HiveSnapshotTimestamp)))")
  }

  test("withAlternativeEngine: dim column override success case") {
    val fact = factDerivedWithFailingDimCol
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withAlternativeEngine("fact2", "fact1", OracleEngine,
        overrideDimCols = Set(
          DimCol("price_type", IntType(), annotations = Set.empty)
        )
        , availableOnwardsDate = Option("2017-09-30"), maxDaysWindow = Some(Map(AsyncRequest -> 31, SyncRequest -> 31)), maxDaysLookBack = Some(Map(AsyncRequest -> 31, SyncRequest -> 31)))
    }
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.values.exists( f => f.fact.name == "fact2") === true)
    assert(bcOption.get.facts.values.find( f => f.fact.name == "fact2").get.fact.forceFilters.isEmpty)
  }

  test("withAlternativeEngine: invalid static mapped Fact") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext {implicit cc : ColumnContext =>
        import OracleExpression._
        fact.withAlternativeEngine("fact2", "fact1", OracleEngine,
          overrideFactCols = Set(
            OracleDerFactCol("clicks", IntType(3, (Map(1 -> "One"), "NONE")), SUM("{impressions}"))
          ), availableOnwardsDate = Option("2017-09-30")
        )
      }
    }
    thrown.getMessage should startWith ("requirement failed: Override column cannot have static mapping :")
  }

  test("withAlternativeEngine: invalid static mapped Dimension") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext {implicit cc : ColumnContext =>
        fact.withAlternativeEngine("fact2", "fact1", OracleEngine,
          overrideDimCols = Set(
            DimCol("stats_source", IntType(1, (Map(1->"One"), "NONE")))
          ),
          overrideFactCols = Set(
          ), availableOnwardsDate = Option("2017-09-30")
        )
      }
    }
    thrown.getMessage should startWith ("requirement failed: Override column cannot have static mapping :")
  }

  test("withAlternativeEngine: Static Mapped dim/fact success cases") {
    val fact = fact1
    ColumnContext.withColumnContext {implicit cc : ColumnContext =>
      import OracleExpression._
      fact.withAlternativeEngine("fact2", "fact1", OracleEngine,
        overrideDimCols = Set(
          DimCol("temp", IntType(1, (Map(1->"One"), "NONE")))
          , DimCol("static_1", IntType(1, (Map(1->"Temp"), "Nothing")))
        ),
        overrideFactCols = Set(
          OracleDerFactCol("temp2", IntType(3, (Map(1 -> "One"), "NONE")), SUM("{impressions}"))
          , FactCol("static_2", IntType(1, (Map(1->"Temp"), "Nothing")))
        ), availableOnwardsDate = Option("2017-09-30")
      )
      val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
      require(bcOption.isDefined, "Failed to get candidates!")
      assert(bcOption.get.facts.values.exists( f => f.fact.name == "fact2") === true)
      assert(bcOption.get.facts.values.find( f => f.fact.name == "fact2").get.fact.forceFilters.isEmpty)
    }
  }

  test("withAlternativeEngine: Static Mapped dim failure to overwrite case") {
    val fact = factDerivedWithOverridableStaticMaps
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withAlternativeEngine("fact2", "fact1", OracleEngine,
          overrideDimCols = Set(
            DimCol("static_1", IntType(1, (Map(1 -> "One", 2 -> "Two"), "NONE")))
          ),
          overrideFactCols = Set(
          ), availableOnwardsDate = Option("2017-09-30")
        )
      }
    }
    thrown.getMessage should startWith("requirement failed: Override column cannot have static mapping :")
  }

  test("withAlternativeEngine: Static Mapped fact failure to overwrite case") {
    val fact = factDerivedWithOverridableStaticMaps
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext {implicit cc : ColumnContext =>
        fact.withAlternativeEngine("fact2", "fact1", OracleEngine,
          overrideFactCols = Set(
            FactCol("static_2", IntType(3, (Map(1 -> "One", 2 -> "Two"), "NONE")))
          ), availableOnwardsDate = Option("2017-09-30")
        )
      }
    }
    thrown.getMessage should startWith ("requirement failed: Override column cannot have static mapping :")
  }

  test("withAlternativeEngine should succeed after discarding the given columns") {
    val fact = fact1
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withAlternativeEngine(
        "fact2",
        "fact1",
        OracleEngine,
        maxDaysWindow = Some(Map(AsyncRequest -> 31, SyncRequest -> 31)),
        maxDaysLookBack = Some(Map(AsyncRequest -> 31, SyncRequest -> 31)),
        discarding = Set("stats_source"))
    }
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions", "Source"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.values.exists( f => f.fact.name == "fact1") === true)
    assert(bcOption.get.facts.values.exists( f => f.fact.name == "fact2") === false)
  }

  test("withAlternativeEngine should fail for invalid columns in discard set") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withAlternativeEngine(
          "fact2",
          "fact1",
          OracleEngine,
          maxDaysWindow = Some(Map(AsyncRequest -> 31, SyncRequest -> 31)),
          maxDaysLookBack = Some(Map(AsyncRequest -> 31, SyncRequest -> 31)),
          discarding = Set("invalid_column"))
      }
    }
    thrown.getMessage should startWith ("requirement failed: Discarding columns should be present in fromTable")
  }
}


