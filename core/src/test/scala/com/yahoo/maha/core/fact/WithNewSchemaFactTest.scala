// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.fact

import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core._
import com.yahoo.maha.core.request.SyncRequest

/**
 * Created by jians on 10/20/15.
 */
class WithNewSchemaFactTest extends BaseFactTest {

  test("withNewSchema should success given different schemas") {
    val fact = fact1
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withNewSchema("fact2", "fact1", Set(ResellerSchema))
    }
    val bcOption = publicFact(fact).getCandidatesFor(ResellerSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.values.exists( f => f.fact.name == "fact2") === true)
  }

  test("withNewSchema should success given different schemas and copy force filters") {
    val fact = fact1WithForceFilters(Set(ForceFilter(InFilter("Pricing Type", List("1"), isForceFilter = true))))
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      fact.withNewSchema("fact2", "fact1", Set(ResellerSchema))
    }
    val bcOption = publicFact(fact).getCandidatesFor(ResellerSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, InFilter("Day", List(s"$toDate")))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.values.exists( f => f.fact.name == "fact2") === true)
    assert(bcOption.get.facts.values.find( f => f.fact.name == "fact2").get.fact.forceFilters.nonEmpty)
  }

  test("withNewSchema should fail given the same schemas") {
    val fact = fact1
    intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withNewSchema("fact2", "fact1", Set(AdvertiserSchema))
      }
    }
  }

  test("withNewSchema should fail given a non-existing from") {
    val fact = fact1
    intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withNewSchema("fact2", "fact1_1", Set(ResellerSchema))
      }
    }
  }

  test("withNewSchema should fail given the same names") {
    val fact = fact1
    intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        fact.withNewSchema("fact1", "fact1", Set(ResellerSchema))
      }
    }
  }
}


