// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.fact

import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core.dimension.{PubCol, DimCol}
import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.lookup.LongRangeLookup
import com.yahoo.maha.core.request.{AsyncRequest, SyncRequest, RequestType}
import org.scalatest.{Matchers, FunSuite}

import com.yahoo.maha.core._
/**
 * Created by jians on 10/20/15.
 */
class PublicFactTest extends FunSuite with Matchers {

  CoreSchema.register()

  protected[this] def getMaxDaysWindow: Map[(RequestType, Grain), Int] = {
    val result = 20
    Map(
      (SyncRequest, DailyGrain) -> result, (AsyncRequest, DailyGrain) -> result
    )
  }

  protected[this] def getMaxDaysLookBack: Map[(RequestType, Grain), Int] = {
    val result = 30
    Map(
      (SyncRequest, DailyGrain) -> result, (AsyncRequest, DailyGrain) -> result
    )
  }
  
  //create a new fact builder for each call
  def fact1(forceFilters: Set[ForceFilter] = Set.empty) : FactBuilder = {
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      Fact.newFact(
        "fact1", DailyGrain, HiveEngine, Set(AdvertiserSchema),
        Set(
          DimCol("account_id", IntType(), annotations = Set(ForeignKey("cache_advertiser_metadata")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3))
          , DimCol("prod", IntType(3), alias = Option("price_type"))
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
        ),
        Set(
          FactCol("impressions", IntType())
          , FactCol("clicks", IntType())
        )
        , forceFilters = forceFilters
        , dimCardinalityLookup = Option(LongRangeLookup.full(Map(AsyncRequest -> Map(HiveEngine -> 1, OracleEngine -> 2))))
      )
    }
  }


  test("toPublicFact should fail with no underlying facts will fail") {
    val fact: FactBuilder = {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        FactBuilder(new FactTable("fact2", 1, DailyGrain, OracleEngine, Set(AdvertiserSchema)
          , Set(
            DimCol("account_id", IntType(), annotations = Set(ForeignKey("cache_advertiser_metadata")))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          )
          , Set(
            FactCol("impressions", IntType())
            , FactCol("clicks", IntType())
          ), None, Set.empty, None, Fact.DEFAULT_COST_MULTIPLIER_MAP, Set.empty,10,100,None, None, None, None, None), Map(), None)
      }
    }

    intercept[IllegalArgumentException] {
      fact.toPublicFact("publicfact",
        Set(
          PubCol("account_id", "Advertiser Id", InEquality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InEquality)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack
      )
    }

  }

  test("toPublicFact should fail with two dim columns with same alias") {
    val fact = fact1()
    intercept[IllegalArgumentException] {
      fact.toPublicFact("publicfact",
        Set(
          PubCol("account_id", "Advertiser Id", InEquality),
          PubCol("stats_source", "Advertiser Id", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InEquality),
          PublicFactCol("clicks", "Clicks", In)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack
      )
    }

  }

  test("toPublicFact should fail with two dim columns with same name") {
    val fact = fact1()
    intercept[IllegalArgumentException] {
      fact.toPublicFact("publicfact",
        Set(
          PubCol("account_id", "Advertiser Id", InEquality),
          PubCol("account_id", "Source", InEquality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InEquality),
          PublicFactCol("clicks", "Clicks", In)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack
      )
    }
  }

  test("toPublicFact should fail with two fact columns with same alias") {
    val fact = fact1()
    intercept[IllegalArgumentException] {
      fact.toPublicFact("publicfact",
        Set(
          PubCol("account_id", "Account Id", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InEquality),
          PublicFactCol("clicks", "Impressions", In)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack
      )
    }
  }

//  test("toPublicFact should fail with two fact columns with same name") {
//    val fact = fact1()
//    intercept[IllegalArgumentException] {
//      fact.toPublicFact("publicfact",
//        Set(
//          PubCol("account_id", "Account ID", InEquality),
//          PubCol("stats_source", "Advertiser Id", Equality),
//          PubCol("price_type", "Pricing Type", In),
//          PubCol("landing_page_url", "Destination URL", Set.empty)
//        ),
//        Set(
//          PublicFactCol("impressions", "Impressions", InEquality),
//          PublicFactCol("impressions", "Clicks", In)
//        ),
//        Set.empty
//      )
//    }
//  }

  test("toPublicFact should fail with one dim and one fact columns with same alias") {
    val fact = fact1()
    intercept[IllegalArgumentException] {
      fact.toPublicFact("publicfact",
        Set(
          PubCol("account_id", "Same Alias", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty)
        ),
        Set(
          PublicFactCol("impressions", "Same Alias", InEquality),
          PublicFactCol("clicks", "Clicks", In)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack
      )
    }
  }


  test("toPublicFact should fail with dim column name referencing non-existing column in fact") {
    val fact = fact1()
    intercept[IllegalArgumentException] {
      fact.toPublicFact("publicfact",
        Set(
          PubCol("non_existing", "Account Id", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InEquality),
          PublicFactCol("clicks", "Clicks", In)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack
      )
    }
  }

  test("toPublicFact should fail with fact column name referencing non-existing column in fact") {
    val fact = fact1()
    intercept[IllegalArgumentException] {
      fact.toPublicFact("publicfact",
        Set(
          PubCol("account_id", "Account Id", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty)
        ),
        Set(
          PublicFactCol("non_existing", "Impressions", InEquality),
          PublicFactCol("clicks", "Clicks", In)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack
      )
    }
  }

  test("toPublicFact should fail with forced filter referencing non-existing column in fact") {
    val fact = fact1()
    val thrown = intercept[IllegalStateException] {
      fact.toPublicFact("publicfact",
        Set(
          PubCol("account_id", "Account Id", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty)
        ),
        Set(
          PublicFactCol("clicks", "Clicks", In)
        ),
        Set(EqualityFilter("field", "value", isForceFilter = true)),
        getMaxDaysWindow, getMaxDaysLookBack
      )
    }
    thrown.getMessage should startWith("requirement failed: field doesn't have a known alias")
  }

  test("toPublicFact should fail with empty max days window") {
    val fact = fact1()
    val thrown = intercept[IllegalArgumentException] {
      fact.toPublicFact("publicfact",
        Set(
          PubCol("account_id", "Account Id", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty)
        ),
        Set(
          PublicFactCol("clicks", "Clicks", In)
        ),
        Set.empty,
        Map.empty, getMaxDaysLookBack
      )
    }
    thrown.getMessage should startWith("requirement failed: No max days window defined")
  }

  test("toPublicFact should fail with empty max days look back window") {
    val fact = fact1()
    val thrown = intercept[IllegalArgumentException] {
      fact.toPublicFact("publicfact",
        Set(
          PubCol("account_id", "Account Id", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty)
        ),
        Set(
          PublicFactCol("clicks", "Clicks", In)
        ),
        Set.empty,
        getMaxDaysWindow, Map.empty
      )
    }
    thrown.getMessage should startWith("requirement failed: No max days look back window defined")
  }

  test("toPublicFact should succeed with forced filter at fact level which exists") {
    val fact = fact1(Set(ForceFilter(InFilter("Pricing Type", List("1"), isForceFilter = true))))

    val pf = fact.toPublicFact("publicfact",
      Set(
        PubCol("account_id", "Account Id", InEquality),
        PubCol("stats_source", "Source", Equality),
        PubCol("price_type", "Pricing Type", In),
        PubCol("landing_page_url", "Destination URL", Set.empty)
      ),
      Set(
        PublicFactCol("clicks", "Clicks", In)
      ),
      Set.empty,
      getMaxDaysWindow, getMaxDaysLookBack
    )
    
    assert(pf.factList.head.forceFilters.exists(_.filter.field == "Pricing Type"))
  }

  test("toPublicFact should fail with forced filter at fact level which does not exist") {
    val fact = fact1(Set(ForceFilter(EqualityFilter("blah", "1", isForceFilter = true, isOverridable = false))))
    val thrown = intercept[IllegalArgumentException] {
      fact.toPublicFact("publicfact",
        Set(
          PubCol("account_id", "Account Id", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty)
        ),
        Set(
          PublicFactCol("clicks", "Clicks", In)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack
      )
    }
    thrown.getMessage should startWith("requirement failed: Invalid fact forced filter : alias=blah , fact=fact1, publicFact=publicfact")
  }

  test("toPublicFact should fail with forced filter at fact level which does not support filter operation") {
    val fact = fact1(Set(ForceFilter(EqualityFilter("Pricing Type", "1", isForceFilter = true))))
    val thrown = intercept[IllegalArgumentException] {
      fact.toPublicFact("publicfact",
        Set(
          PubCol("account_id", "Account Id", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty)
        ),
        Set(
          PublicFactCol("clicks", "Clicks", In)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack
      )
    }
    thrown.getMessage should startWith("requirement failed: Invalid fact forced filter operation on alias=Pricing Type , operations supported : Set(In) operation not supported : =")
  }

  test("toPublicFact should succeed with forced filter at fact level which does support filter operation") {
    val fact = fact1(Set(ForceFilter(InFilter("Pricing Type", List("1", "2"), isForceFilter = true))))
      fact.toPublicFact("publicfact",
        Set(
          PubCol("account_id", "Account Id", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty)
        ),
        Set(
          PublicFactCol("clicks", "Clicks", In)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack
      )

  }

  test("toPublicFact should fail if forced filter columns ARE in the base dim, but resolve to the same base column (duplicate where clause)") {
    val fact = fact1(Set.empty)
    val thrown = intercept[IllegalArgumentException] {
      fact.toPublicFact("publicfact",
        Set(
          PubCol("account_id", "Account Id", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", InEquality, incompatibleColumns = Set("Sale")),
          PubCol("prod", "Sale", InEquality, incompatibleColumns = Set("Pricing Type")),
          PubCol("landing_page_url", "Destination URL", Set.empty)
        ),
        Set(
          PublicFactCol("clicks", "Clicks", In)
        ),
        Set(EqualityFilter("Pricing Type", "1", isForceFilter = true), InFilter("Sale", List("1", "2"), isForceFilter = true)),
        getMaxDaysWindow, getMaxDaysLookBack
      )
    }
    thrown.getMessage should startWith("requirement failed: Forced Filters public fact and map of forced base cols differ in size")
  }

  test("toPublicFact should fail if a declared forced filter doesn't have isForcedFilter=true") {
    val fact = fact1(Set.empty)
    val thrown = intercept[IllegalArgumentException] {
      fact.toPublicFact("publicfact",
        Set(
          PubCol("price_type", "Pricing Type", InEquality)
        ),
        Set(
        ),
        Set(EqualityFilter("Pricing Type", "1")),
        getMaxDaysWindow, getMaxDaysLookBack
      )
    }
    thrown.getMessage should startWith("requirement failed: Forced Filter boolean false, expected true")
  }

  test("toPublicFact should succeed with forced filter at public fact level which is overridden at fact level") {
    val fact = fact1(Set(ForceFilter(InFilter("Pricing Type", List("1"), isForceFilter = true))))

    val pf = fact.toPublicFact("publicfact",
      Set(
        PubCol("account_id", "Account Id", InEquality),
        PubCol("stats_source", "Source", Equality),
        PubCol("price_type", "Pricing Type", In),
        PubCol("landing_page_url", "Destination URL", Set.empty)
      ),
      Set(
        PublicFactCol("clicks", "Clicks", In)
      ),
      Set(InFilter("Pricing Type", List("2"), isForceFilter = true)),
      getMaxDaysWindow, getMaxDaysLookBack
    )

    assert(pf.factList.head.forceFilters.exists(_.filter.field == "Pricing Type"))
  }
}
