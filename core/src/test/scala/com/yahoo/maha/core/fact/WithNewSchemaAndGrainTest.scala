// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.fact

import com.yahoo.maha.core._
import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.request.SyncRequest

/**
 * Created by shengyao on 2/10/16.
 */
class WithNewSchemaAndGrainTest extends BaseFactTest {
  test("withNewSchemaAndGrain should succeed with new grain and new schema") {
    val fact = fact1
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withNewSchemaAndGrain("fact2", "fact1", Set(ResellerSchema), HourlyGrain)
    }
    val bcOption = publicFact(fact).getCandidatesFor(ResellerSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.values.exists( f => f.fact.name == "fact2") === true)
  }

  test("withNewSchemaAndGrain should succeed with new grain and new schema and copy force filters") {
    val fact = fact1WithForceFilters(Set(ForceFilter(InFilter("Pricing Type", List("1"), isForceFilter = true))))
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withNewSchemaAndGrain("fact2", "fact1", Set(ResellerSchema), HourlyGrain)
    }
    val bcOption = publicFact(fact).getCandidatesFor(ResellerSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.values.exists( f => f.fact.name == "fact2") === true)
    assert(bcOption.get.facts.values.find( f => f.fact.name == "fact2").get.fact.forceFilters.nonEmpty)
  }

  test("withNewSchemaAndGrain should succeed with new grain and new schema and override force filters") {
    val fact = fact1WithForceFilters(Set(ForceFilter(InFilter("Pricing Type", List("1"), isForceFilter = true))))
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withNewSchemaAndGrain("fact2", "fact1", Set(ResellerSchema), HourlyGrain, forceFilters = Set(ForceFilter(InFilter("Pricing Type", List("1"), isForceFilter = true))))
    }
    val bcOption = publicFact(fact).getCandidatesFor(ResellerSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.values.exists( f => f.fact.name == "fact2") === true)
    assert(bcOption.get.facts.values.find( f => f.fact.name == "fact2").get.fact.forceFilters.nonEmpty)
  }

  test("withNewSchemaAndGrain should fail given the same schema") {
    val fact = fact1
    intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withNewSchemaAndGrain("fact2", "fact1", Set(AdvertiserSchema), HourlyGrain)
      }
    }
  }

  test("wtihNewSchemaAndGrain should fail if grain is the same") {
    val fact = fact1
    intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withNewSchemaAndGrain("fact2", "fact1", Set(AdvertiserLowLatencySchema), DailyGrain)
      }
    }
  }
}
