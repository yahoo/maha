// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.fact

import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.{InFilter, InFilterOperation}
import com.yahoo.maha.core.ddl.HiveDDLAnnotation
import com.yahoo.maha.core.request.SyncRequest

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
}
