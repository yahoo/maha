// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.fact

import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.ddl.HiveDDLAnnotation
import com.yahoo.maha.core.dimension.DimCol
import com.yahoo.maha.core.request.SyncRequest
import com.yahoo.maha.core.{ColumnContext, DailyGrain, InFilter, InFilterOperation, IntType, OracleEngine, PrimaryKey, StrType}

/**
 * Created by shengyao on 2/10/16.
 */

class createSubsetTest extends BaseFactTest {
  test("createSubSet should be successful with discarding fact columns") {
    val fact = fact1
    fact.createSubset("fact2", "fact1", Set("clicks"), Set.empty)
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, InFilter("Day", List(s"$toDate")))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.keys.exists(_ == "fact2") === true, "create subset failed")
  }

  test("createSubset should fail if trying to discard dim columns") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      fact.createSubset("fact2", "fact1", Set("campaign_id"), Set(AdvertiserLowLatencySchema))
    }
    thrown.getMessage should startWith ("requirement failed: Cannot discard dim column campaign_id with createSubset, use newRollup to discard")
  }

  test("createSubset should fail discarding non-exist columns") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      fact.createSubset("fact2", "fact1", Set("column"), Set.empty)
    }
    thrown.getMessage should startWith ("requirement failed: column column does not exist")
  }

  test("createSubset should discard column in hive ddl annotation") {
    val fact = fact1
    fact.createSubset("fact2", "fact1", Set("clicks"), Set.empty)
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, InFilter("Day", List(s"$toDate")))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.keys.exists(_ == "fact2") === true, "create subset failed")
    assert(!bcOption.get.facts("fact2").fact.ddlAnnotation.get.asInstanceOf[HiveDDLAnnotation].columnOrdering.contains("clicks"),
      "Failed to discard column: clicks in hive ddl column ordering")
  }

  test("createSubset should be able to create a subset fact with availableOnwardsDate") {
    val fact = fact1
    fact.createSubset("fact2", "fact1", Set("clicks"), Set.empty, availableOnwardsDate = Some(s"$fromDate"))
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, InFilter("Day", List(s"$toDate")))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.keys.exists(_ == "fact2") === true, "create subset failed")
    assert(bcOption.get.facts.get("fact2").get.fact.availableOnwardsDate.isDefined, "Should list the best candidate based on availableOnwardsDate")
  }

  test("should discard a subset fact with availableOnwardsDate > requested date") {
    val fact = fact1
    fact.createSubset("fact2", "fact1", Set("clicks"), Set.empty, availableOnwardsDate = Some(s"$toDate"))
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, InFilter("Day", List(s"$fromDate")))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.keys.exists(_ == "fact2") === false, "Should discard fact2 based on the availableOnwardsDate")
    assert(bcOption.get.facts.size ==1, "Should list the best candidate based on availableOnwardsDate")
  }

  test("should discard a subset fact with availableOnwardsDate > at least one of the date in requested dates list") {
    val fact = fact1
    fact.createSubset("fact2", "fact1", Set("clicks"), Set.empty, availableOnwardsDate = Some(s"$toDate"))
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, InFilter("Day", List(s"$fromDate", s"$toDate")))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.keys.exists(_ == "fact2") === false, "Should discard fact2 based on the availableOnwardsDate")
    assert(bcOption.get.facts.size ==1, "Should list the best candidate based on availableOnwardsDate")
  }

  test("should not discard a subset fact with availableOnwardsDate == requested date") {
    val fact = fact1
    fact.createSubset("fact2", "fact1", Set("clicks"), Set.empty, availableOnwardsDate = Some(s"$toDate"))
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, InFilter("Day", List(s"$toDate")))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.keys.exists(_ == "fact2") === true, "create subset failed")
    assert(bcOption.get.facts.get("fact2").get.fact.availableOnwardsDate.isDefined, "Should list the best candidate based on availableOnwardsDate")
  }
  test("createSubsetTest: should fail if source fact is invalid") {
    val thrown = intercept[IllegalArgumentException] {
        val fact = fact1
        fact.createSubset("fact2", "base_fact", Set("clicks"), Set.empty, availableOnwardsDate = Some(toDate))
      }
    thrown.getMessage should startWith ("requirement failed: from table not valid base_fact")
  }

  test("createSubsetTest: should fail if source table isn't there") {
    val fact = fact1
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
        fb.createSubset("fact2", "base_fact", Set("clicks"), Set.empty, availableOnwardsDate = Some(toDate))
      }
    }
    thrown.getMessage should startWith ("requirement failed: no table to create subset from")
  }

  test("createSubset: should fail if destination fact name is already in use") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      fact.createSubset("fact2", "fact1", Set("clicks"), Set.empty, availableOnwardsDate = Some(toDate))
      fact.createSubset("fact2", "fact1", Set("clicks"), Set.empty, availableOnwardsDate = Some(toDate))
    }
    thrown.getMessage should startWith ("requirement failed: table fact2 already exists")
  }

  test("createSubset: should fail if no columns are discarded") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      fact.createSubset("fact2", "fact1", Set(), Set.empty, availableOnwardsDate = Some(toDate))
    }
    thrown.getMessage should startWith ("requirement failed: discarding set should never be empty")
  }

  test("createSubset: should fail if source and destination schemas are the same") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      fact.createSubset("fact2", "fact1", Set("clicks"), schemas = Set(AdvertiserSchema), availableOnwardsDate = Some(toDate))
    }
    thrown.getMessage should startWith ("requirement failed: schemas are the same! from: Set(advertiser), to: Set(advertiser)")
  }

  test("createSubset: should pass if source and destination schemas are different") {
    val fact = fact1
    fact.createSubset("fact2", "fact1", Set("clicks"), schemas = Set(AdvertiserSchema, ResellerSchema), availableOnwardsDate = Some(toDate))
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, InFilter("Day", List(s"$toDate")))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.keys.exists(_ == "fact2") === true, "create subset failed")
  }

  test("createSubset: should pass if new subset has forceFilters") {
    val fact = fact1
    fact.createSubset("fact2", "fact1", Set("clicks"), schemas = Set(AdvertiserSchema, ResellerSchema), availableOnwardsDate = Some(toDate))
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, InFilter("Day", List(s"$toDate")))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.keys.exists(_ == "fact2") === true, "create subset failed")
  }
}
