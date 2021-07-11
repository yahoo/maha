// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.fact

import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.FilterOperation.{Equality, In, InEquality}
import com.yahoo.maha.core._
import com.yahoo.maha.core.ddl.HiveDDLAnnotation
import com.yahoo.maha.core.dimension.{DimCol, OracleDerDimCol, PubCol}
import com.yahoo.maha.core.request.{AsyncRequest, SyncRequest}

/**
 * Created by jians on 10/20/15.
 */
class WithAvailableOnwardsDateTest extends BaseFactTest {

  test("withAvailableOnwardsDate: should pass with new engine (Oracle)") {
    val fact = fact1
      ColumnContext.withColumnContext {implicit cc : ColumnContext =>
        fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, OracleEngine
          , availableOnwardsDate = Option("2017-09-25")
        )
      }

    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.values.exists( f => f.fact.name == "fact2") === true)
    assert(bcOption.get.facts.values.find( f => f.fact.name == "fact2").get.fact.forceFilters.isEmpty)
  }

  test("withAvailableOnwardsDate: should pass with redefined FactCol and DimCol") {
    val fact = fact1
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, HiveEngine,
        overrideDimCols =  Set(
          DimCol("account_id", IntType(), annotations = Set(ForeignKey("cache_advertiser_metadata")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3))
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
        ),
        overrideFactCols = Set(
          FactCol("impressions", IntType())
        ), availableOnwardsDate = Option("2017-09-25")
      ).toPublicFact("temp",
        Set(
          PubCol("account_id", "Advertiser Id", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InEquality)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack
      )

    }
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.keys.exists(_ == "fact2") === true)
  }

  test("withAvailableOnwardsDate: Should succeed with 2 availableOnwardsDates on the same Engine") {
    val fact = fact1
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, OracleEngine,
        overrideDimCols = Set(
          DimCol("account_id", IntType(), annotations = Set(ForeignKey("cache_advertiser_metadata")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3))
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
        ),
        overrideFactCols = Set(
          FactCol("impressions", IntType())
        ), availableOnwardsDate = Option("2017-09-25")
      ).toPublicFact("temp",
        Set(
          PubCol("account_id", "Advertiser Id", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InEquality)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack
      )
    }
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withAvailableOnwardsDate("fact3", "fact1", Set.empty, OracleEngine,
        overrideDimCols =  Set(
          DimCol("account_id", IntType(), annotations = Set(ForeignKey("cache_advertiser_metadata")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3))
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
        ),
        overrideFactCols = Set(
          FactCol("impressions", IntType())
        ), availableOnwardsDate = Option("2017-09-26")
      ).toPublicFact("temp",
        Set(
          PubCol("account_id", "Advertiser Id", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InEquality)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack
      )

    }
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.keys.exists(_ == "fact2") === true, "first onwardsDate failed")
    assert(bcOption.get.facts.keys.exists(_ == "fact3") === true, "second onwardsDate failed")
  }

  test("withAvailableOnwardsDate: Should fail with 1 availableOnwardsDate on the same Engine") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, OracleEngine,
          overrideDimCols = Set(
            DimCol("account_id", IntType(), annotations = Set(ForeignKey("cache_advertiser_metadata")))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
            , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
            , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("stats_source", IntType(3))
            , DimCol("price_type", IntType(3))
            , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          ),
          overrideFactCols = Set(
            FactCol("impressions", IntType())
          ), availableOnwardsDate = Option("2017-09-25")
        ).toPublicFact("temp",
          Set(
            PubCol("account_id", "Advertiser Id", InEquality),
            PubCol("stats_source", "Source", Equality),
            PubCol("price_type", "Pricing Type", In),
            PubCol("landing_page_url", "Destination URL", Set.empty)
          ),
          Set(
            PublicFactCol("impressions", "Impressions", InEquality)
          ),
          Set.empty,
          getMaxDaysWindow, getMaxDaysLookBack
        )
      }
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withAvailableOnwardsDate("fact3", "fact1", Set.empty, OracleEngine,
          overrideDimCols = Set(
            DimCol("account_id", IntType(), annotations = Set(ForeignKey("cache_advertiser_metadata")))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
            , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
            , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("stats_source", IntType(3))
            , DimCol("price_type", IntType(3))
            , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          ),
          overrideFactCols = Set(
            FactCol("impressions", IntType())
          ), availableOnwardsDate = Option("2017-09-25")
        ).toPublicFact("temp",
          Set(
            PubCol("account_id", "Advertiser Id", InEquality),
            PubCol("stats_source", "Source", Equality),
            PubCol("price_type", "Pricing Type", In),
            PubCol("landing_page_url", "Destination URL", Set.empty)
          ),
          Set(
            PublicFactCol("impressions", "Impressions", InEquality)
          ),
          Set.empty,
          getMaxDaysWindow, getMaxDaysLookBack
        )

      }
    }
    thrown.getMessage should startWith ("requirement failed: Base date 2017-09-25 in fact fact3 is already defined in fact2")
  }

  test("withAvailableOnwardsDate: Should succeed with identical availableOnwardsDates on the different Engines") {
    val fact = fact1
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, HiveEngine,
        overrideDimCols = Set(
          DimCol("account_id", IntType(), annotations = Set(ForeignKey("cache_advertiser_metadata")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3))
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
        ),
        overrideFactCols = Set(
          FactCol("impressions", IntType())
        ), availableOnwardsDate = Option("2017-09-25")
      ).toPublicFact("temp",
        Set(
          PubCol("account_id", "Advertiser Id", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InEquality)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack
      )
    }
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withAvailableOnwardsDate("fact3", "fact1", Set.empty, OracleEngine,
        overrideDimCols =  Set(
          DimCol("account_id", IntType(), annotations = Set(ForeignKey("cache_advertiser_metadata")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3))
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
        ),
        overrideFactCols = Set(
          FactCol("impressions", IntType())
        ), availableOnwardsDate = Option("2017-09-25")
      ).toPublicFact("temp",
        Set(
          PubCol("account_id", "Advertiser Id", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InEquality)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack
      )

    }
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.keys.exists(_ == "fact2") === true, "first onwardsDate failed")
    assert(bcOption.get.facts.keys.exists(_ == "fact3") === true, "second onwardsDate failed")
  }

  test("withAvailableOnwardsDate: should fail if no tableMap to pull from") {
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
        fb.withAvailableOnwardsDate("fact2", "base_fact", Set.empty, OracleEngine
          , availableOnwardsDate = Option("2017-09-25")
        )
      }
    }
    thrown.getMessage should startWith ("requirement failed: no tables found")
  }

  test("withAvailableOnwardsDate: should fail if source fact is wrong") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withAvailableOnwardsDate("fact2", "fact", Set.empty, OracleEngine
          , availableOnwardsDate = Option("2017-09-25")
        )
      }
    }
    thrown.getMessage should startWith ("requirement failed: from table not found : fact")
  }

  test("withAvailableOnwardsDate: should fail if destination fact name is already in use") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, OracleEngine
          , availableOnwardsDate = Option("2017-09-25")
        )
      }
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, OracleEngine
          , availableOnwardsDate = Option("2017-09-30")
        )
      }
    }
    thrown.getMessage should startWith ("requirement failed: should not export with existing table name fact2")
  }

  test("withAvailableOnwardsDate: availableOnwardsDate must be defined") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, OracleEngine
        )
      }
    }
    thrown.getMessage should startWith ("requirement failed: availableOnwardsDate parameter must be defined in withAvailableOnwardsDate in fact2")
  }

  test("withAvailableOnwardsDate: should fail if override dim columns are self referential") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext {implicit cc : ColumnContext =>
        import OracleExpression._
        fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, OracleEngine,
          Set(
            OracleDerDimCol("dimcol1", IntType(3, (Map(1 -> "One"), "NONE")), DECODE_DIM("{dimcol1}", "0", "zero", "{dimcol1}"))
          ), availableOnwardsDate = Option("2017-09-30")
        )
      }
    }
    thrown.getMessage should startWith ("requirement failed: Derived column is referring to itself, this will cause infinite loop :")
  }

  test("withAvailableOnwardsDate: can't reference a column that does not exist") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext {implicit cc : ColumnContext =>
        import OracleExpression._
        fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, OracleEngine,
          overrideFactCols = Set(
            OracleDerFactCol("factcol1", IntType(3, (Map(1 -> "One"), "NONE")), SUM("{dimcol1}"))
          ), availableOnwardsDate = Option("2017-09-30")
        )
      }
    }
    thrown.getMessage should startWith ("requirement failed: Failed derived expression validation, unknown referenced column in fact=fact2, dimcol1")
  }

  test("withAvailableOnwardsDate: invalid static mapped Fact") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext {implicit cc : ColumnContext =>
        import OracleExpression._
        fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, OracleEngine,
          overrideFactCols = Set(
            OracleDerFactCol("clicks", IntType(3, (Map(1 -> "One"), "NONE")), SUM("{impressions}"))
          ), availableOnwardsDate = Option("2017-09-30")
        )
      }
    }
    thrown.getMessage should startWith ("requirement failed: Override column cannot have static mapping :")
  }

  test("withAvailableOnwardsDate: invalid static mapped Dimension") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext {implicit cc : ColumnContext =>
        fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, OracleEngine,
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

  test("withAvailableOnwardsDate: Static Mapped dim/fact success cases") {
    val fact = factDerivedWithOverridableStaticMaps
    ColumnContext.withColumnContext {implicit cc : ColumnContext =>
      import OracleExpression._
      fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, OracleEngine,
        overrideDimCols = Set(
          DimCol("temp", IntType(1, (Map(1->"One"), "NONE")))
          , DimCol("static_3", IntType(1, (Map(1->"Temp"), "Nothing")), alias = Option("static_1"))
        ),
        overrideFactCols = Set(
          OracleDerFactCol("temp2", IntType(3, (Map(1 -> "One"), "NONE")), SUM("{impressions}"))
          , FactCol("static_4", IntType(1, (Map(1->"Temp"), "Nothing")), alias = Option("static_2"))
        ), availableOnwardsDate = Option("2017-09-30")
      )
      val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
      require(bcOption.isDefined, "Failed to get candidates!")
      assert(bcOption.get.facts.values.exists( f => f.fact.name == "fact2") === true)
      assert(bcOption.get.facts.values.find( f => f.fact.name == "fact2").get.fact.forceFilters.isEmpty)
    }
  }

  test("withAvailableOnwardsDate: should discard the new table if the availableOnwardsDate > requested date") {
    val fact = fact1
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, OracleEngine, availableOnwardsDate = Some(s"$toDate"))
    }
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$fromDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.keys.exists(_ == "fact2") === false, "should discard best candidate of new rollup based on the availableOnwardsDate")
  }

  test("withAvailableOnwardsDate: should fail with missing override for fact column annotation with engine requirement") {
    val fact = fact1WithAnnotationWithEngineRequirement
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, OracleEngine, overrideFactCols = Set(
        ), availableOnwardsDate = Option("2017-09-30"))
      }
    }
    thrown.getMessage should include("missing fact annotation overrides = List((clicks,Set(HiveSnapshotTimestamp))),")
  }

  test("withAvailableOnwardsDate: should fail with missing override for fact column rollup with engine requirement") {
    val fact = fact1WithRollupWithEngineRequirement
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, OracleEngine, overrideFactCols = Set(
        ), availableOnwardsDate = Option("2017-09-30"))
      }
    }
    thrown.getMessage should include("missing fact rollup overrides = List((clicks,HiveCustomRollup(")
  }

  test("withAvailableOnwardsDate: should succeed with override for fact column rollup with engine requirement") {
    val fact = fact1WithRollupWithEngineRequirement
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, OracleEngine, overrideFactCols = Set(
          FactCol("clicks", IntType(), OracleCustomRollup("rollup"))
        ), availableOnwardsDate = Option("2017-09-30"))
      }
  }

  test("withAvailableOnwardsDate: should fail if ddl annotation is of a different engine other than the engine of the fact") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit  cc : ColumnContext =>
        fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, OracleEngine, overrideDDLAnnotation = Option(HiveDDLAnnotation()), availableOnwardsDate = Option("2017-09-30"))
      }
    }
    thrown.getMessage should startWith ("requirement failed: Failed engine requirement fact=fact2, engine=Oracle, ddlAnnotation=Some(HiveDDLAnnotation(Map(),Vector()))")
  }

  test("withAvailableOnwardsDate: column annotation override success case") {
    val fact = fact1WithAnnotationWithEngineRequirement
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, OracleEngine
        , overrideFactCols = Set(
          FactCol("clicks", IntType(), annotations = Set(EscapingRequired))
        )
      , availableOnwardsDate = Option("2017-09-30"))
    }
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.values.exists( f => f.fact.name == "fact2") === true)
    assert(bcOption.get.facts.values.find( f => f.fact.name == "fact2").get.fact.forceFilters.isEmpty)
  }

  test("withAvailableOnwardsDate: dim column override failure case") {
    val fact = factDerivedWithFailingDimCol
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, OracleEngine
          , availableOnwardsDate = Option("2017-09-30"))
      }
    }
    thrown.getMessage should startWith ("requirement failed: name=fact2, from=fact1, engine=Oracle, missing dim overrides = List((price_type,Set(HiveSnapshotTimestamp)))")
  }

  test("withAvailableOnwardsDate: invalid discard") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withAvailableOnwardsDate("fact2", "fact1", Set("fake_col"), OracleEngine
          , availableOnwardsDate = Option("2017-09-30"))
      }
    }
    thrown.getMessage should startWith ("requirement failed: column fake_col does not exist")
  }

  test("withAvailableOnwardsDate: dim column override success case") {
    val fact = factDerivedWithFailingDimCol
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, OracleEngine,
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



  test("discarding column from old cube, adding in new one") {
    val fact = factDerivedWithFailingDimCol
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withAvailableOnwardsDate("fact2", "fact1", discarding = Set(
        "clicks", "price_type"
      ), OracleEngine,
        overrideDimCols = Set(
          DimCol("price_type", IntType(), annotations = Set.empty)
          , DimCol("new_type", IntType())
        ),
        overrideFactCols = Set(
          OracleDerFactCol("clicks", IntType(), "{impressions}")
        )
        , availableOnwardsDate = Option("2017-09-30"), maxDaysWindow = Some(Map(AsyncRequest -> 31, SyncRequest -> 31)), maxDaysLookBack = Some(Map(AsyncRequest -> 31, SyncRequest -> 31)))
    }
    fact.toPublicFact("publicFact",
      Set(
        PubCol("account_id", "Advertiser Id", InEquality),
        PubCol("stats_source", "Source", Equality),
        PubCol("price_type", "Pricing Type", In),
        PubCol("landing_page_url", "Destination URL", Set.empty)
      ),
      Set(
        PublicFactCol("impressions", "Impressions", InEquality),
        PublicFactCol("clicks", "Clicks", In)
      ),
      Set.empty,
      Map(
          (SyncRequest, DailyGrain) -> 20, (AsyncRequest, DailyGrain) -> 20
      ),
      Map(
        (SyncRequest, DailyGrain) -> 30, (AsyncRequest, DailyGrain) -> 30
      ))
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions", "Clicks"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.values.exists( f => f.fact.name == "fact2") === true)
    assert(bcOption.get.facts.values.find( f => f.fact.name == "fact2").get.fact.forceFilters.isEmpty)
  }

  test("withAvailableOnwardsDate: verify overrideDDLAnnotation overrides on new table") {
    val fact = factDerivedWithFailingDimCol
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, HiveEngine,
        overrideDimCols = Set(
          DimCol("price_type", IntType(), annotations = Set.empty)
        ), overrideDDLAnnotation = Option(new HiveDDLAnnotation(columnOrdering =
          IndexedSeq(
            "account_id",
            "campaign_id",
            "ad_group_id",
            "ad_id",
            "impressions",
            "clicks",
            "engagement_count",
            "stats_source",
            "price_type",
            "landing_page_url",
            "engagement_type"
          )))
        , availableOnwardsDate = Option("2017-09-30"), maxDaysWindow = Some(Map(AsyncRequest -> 31, SyncRequest -> 31)), maxDaysLookBack = Some(Map(AsyncRequest -> 31, SyncRequest -> 31)))
    }
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.values.exists( f => f.fact.name == "fact2") === true)
    assert(bcOption.get.facts.values.find( f => f.fact.name == "fact2").get.fact.forceFilters.isEmpty)
  }

  test("withAvailableOnwardsDate: column alias should be preserved") {
    val fact = factWithColumnAliasing
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withAvailableOnwardsDate("fact2", "fact1", Set.empty, HiveEngine,
        overrideDimCols =  Set.empty,
        overrideFactCols = Set.empty,
        availableOnwardsDate = Option("2017-09-25")
      ).toPublicFact("temp",
        Set(
          PubCol("account_id", "Advertiser Id", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InEquality)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack
      )

    }
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts("fact1").fact.dimCols.count(d => d.name.equals("cmp_id_alias") && d.alias.get.equals("campaign_id")) == 1)
    assert(bcOption.get.facts("fact2").fact.dimCols.count(d => d.name.equals("cmp_id_alias") && d.alias.get.equals("campaign_id")) == 1)
  }
}