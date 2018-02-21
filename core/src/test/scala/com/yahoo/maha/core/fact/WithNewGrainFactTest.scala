// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.fact

import com.yahoo.maha.core._
import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.request.SyncRequest

/**
 * Created by jians on 10/20/15.
 */
class WithNewGrainFactTest extends BaseFactTest {

  test("withNewGrain should be successful given a different grain") {
    val fact = fact1
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withNewGrain("fact2", "fact1", HourlyGrain, resetAliasIfNotPresent = true)
    }
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.values.exists( f => f.fact.name == "fact2") === true)

  }

  test("withNewGrain should be successful given a different grain on Oracle") {
    val fact = facto
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withNewGrain("fact2", "facto", HourlyGrain, resetAliasIfNotPresent = true)
    }
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withNewGrain("fact3", "facto", HourlyGrain, resetAliasIfNotPresent = false)
    }
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.values.exists( f => f.fact.name == "fact2") === true)

  }

  test("withNewGrain should be successful given a different grain on Druid") {
    val fact = factd
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withNewGrain("fact2", "factd", HourlyGrain, resetAliasIfNotPresent = true)
    }
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withNewGrain("fact3", "factd", HourlyGrain, resetAliasIfNotPresent = false)
    }
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.values.exists( f => f.fact.name == "fact2") === true)

  }

  test("withNewGrain should be successful given a different grain on Presto") {
    val fact = factp
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withNewGrain("fact2", "factp", HourlyGrain, resetAliasIfNotPresent = true)
    }
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withNewGrain("fact3", "factp", HourlyGrain, resetAliasIfNotPresent = false)
    }
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.values.exists( f => f.fact.name == "fact2") === true)

  }

  test("withNewGrain should be successful given a different grain and should copy force filters") {
    val fact = fact1WithForceFilters(Set(ForceFilter(InFilter("Pricing Type", List("1"), isForceFilter = true))))
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withNewGrain("fact2", "fact1", HourlyGrain)
    }
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.values.exists( f => f.fact.name == "fact2") === true)
    assert(bcOption.get.facts.values.find( f => f.fact.name == "fact2").get.fact.forceFilters.nonEmpty)

  }

  test("withNewGrain should fail given the same grain") {
    val fact = fact1
    intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withNewGrain("fact2", "fact1", DailyGrain)
      }
    }
  }
  test("withNewGrain should fail given non-existing from") {
    val fact = fact1
    intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withNewGrain("fact2", "fact1_2", HourlyGrain)
      }
    }
  }
  test("withNewGrain should fail given the same name") {
    val fact = fact1
    intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withNewGrain("fact1", "fact1", HourlyGrain)
      }
    }
  }

  test("withNewGrain should fail if to table already exists") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext{ implicit  cc: ColumnContext =>
        fact.withNewGrain("fact2", "fact1", HourlyGrain)
        fact.withNewGrain("fact1", "fact2", DailyGrain)
      }
    }
    thrown.getMessage should startWith("requirement failed: to table should not exist")
  }

}


