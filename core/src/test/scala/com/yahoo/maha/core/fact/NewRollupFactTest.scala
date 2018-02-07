// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.fact

import com.yahoo.maha.core._
import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.ddl.HiveDDLAnnotation
import com.yahoo.maha.core.dimension.DimCol
import com.yahoo.maha.core.request.{RequestType, SyncRequest}

/**
 * Created by jians on 10/20/15.
 */
class NewRollupFactTest extends BaseFactTest {

  test("newRollup should succeed with discarding foreign key") {
    val fact = fact1
    fact.newRollUp("fact2", "fact1", Set("ad_id"))
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.keys.exists(_ == "fact2") === true)
    assert(bcOption.get.facts.find(_._1 == "fact2").get._2.fact.maxDaysLookBack === None)
    assert(bcOption.get.facts.find(_._1 == "fact2").get._2.fact.maxDaysWindow === None)
  }
  test("newRollup should success with discarding foreign key and change of max days and max lookback") {
    val fact = fact1
    val maxWindow: Option[Map[RequestType, Int]] = Option(Map(SyncRequest -> 11))
    val maxLookBack: Option[Map[RequestType, Int]] = Option(Map(SyncRequest -> 12))
    fact.newRollUp("fact2", "fact1", Set("ad_id"), maxDaysLookBack = maxLookBack, maxDaysWindow = maxWindow)
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.keys.exists(_ == "fact2") === true)
    assert(bcOption.get.facts.find(_._1 == "fact2").get._2.fact.maxDaysLookBack === maxLookBack)
    assert(bcOption.get.facts.find(_._1 == "fact2").get._2.fact.maxDaysWindow === maxWindow)
  }

  test("newRollUp should always discard some columns") {
    val fact = fact1
    intercept[IllegalArgumentException] {
      fact.newRollUp("fact2", "fact1", Set())
    }
  }

  test("newRollup should succeed with 2 rollups") {
    val fact = fact1
    fact.newRollUp("fact2", "fact1", Set("ad_id"))
    fact.newRollUp("fact3", "fact2", Set("ad_group_id"))
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.keys.exists(_ == "fact2") === true, "first roll up failed")
    assert(bcOption.get.facts.keys.exists(_ == "fact3") === true, "second roll up failed")
  }


  test("newRollup should succeed when discarding column that is not a foreign key dim col") {
    val fact = fact1
    fact.newRollUp("fact2", "fact1", Set("stats_source"))
  }

/*  test("newRollup should fail when discarding column that is not a foreign key fact col") {
    val fact = fact1
    intercept[IllegalArgumentException] {
      fact.newRollUp("fact2", "fact1", Set("clicks"))
    }

  }
*/

  test("newRollup should fail when discarding non-existing column") {
    val fact = fact1
    intercept[IllegalArgumentException] {
      fact.newRollUp("fact2", "fact1", Set("columnc"))
    }
  }

  test("newRollup should fail when discarding last remaining foreign key") {
    val fact = fact1

    fact.newRollUp("fact2", "fact1", Set("account_id"))
    fact.newRollUp("fact3", "fact2", Set("campaign_id"))
    fact.newRollUp("fact4", "fact3", Set("ad_group_id"))


    intercept[IllegalArgumentException] {
      fact.newRollUp("fact5", "fact4", Set("ad_id"))
    }
  }

  test("newRollup should fail when from table is does not exist") {
    val fact = fact1
    intercept[IllegalArgumentException] {
      fact.newRollUp("fact2", "fact3", Set("ad_group_id"))
    }
  }

  test("newRollup should fail when rollup to an existing table") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      fact.newRollUp("fact2", "fact1", Set("account_id"))
      fact.newRollUp("fact1", "fact2", Set("ad_group_id"))
    }
    thrown.getMessage should startWith("requirement failed: table")
  }

  test("newRollup should fail if tableMap is empty") {
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
        fb.newRollUp("fact2", "base_fact", Set("dimcol2"))
      }
    }
    thrown.getMessage should startWith ("requirement failed: no table to roll up from")
  }

  test("newRollup should fail if dim column context is not the same as source") {
    import org.mockito.Matchers._
    import org.mockito.Mockito._
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc =>
        val mockedDimCol : DimCol = mock(classOf[DimCol])

        when(mockedDimCol.name).thenReturn("mockedCol")
        when(mockedDimCol.alias).thenReturn(None)
        when(mockedDimCol.annotations).thenReturn(Set.empty[ColumnAnnotation])
        when(mockedDimCol.columnContext).thenReturn(cc)
        when(mockedDimCol.dataType).thenReturn(IntType())
        when(mockedDimCol.copyWith(any(classOf[ColumnContext]), any(classOf[Map[String,String]]), any())).thenReturn(mockedDimCol)

        Fact.newFact("fact1", DailyGrain, HiveEngine, Set(AdvertiserSchema),
          Set(
            DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
            , mockedDimCol
            , DimCol("dimcol2", IntType())),
          Set(
            FactCol("factcol1", StrType())
          )).newRollUp("fact2", "fact1", Set("dimcol2"))
      }
    }
    thrown.getMessage should startWith ("requirement failed: Failed to copy dim columns with new column context!")
  }

  test("newRollup should fail if fact column context is not the same as source") {
    import org.mockito.Matchers._
    import org.mockito.Mockito._
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc =>
        val mockedFactCol : FactCol = mock(classOf[FactCol])

        when(mockedFactCol.name).thenReturn("mockedCol")
        when(mockedFactCol.alias).thenReturn(None)
        when(mockedFactCol.annotations).thenReturn(Set.empty[ColumnAnnotation])
        when(mockedFactCol.columnContext).thenReturn(cc)
        when(mockedFactCol.dataType).thenReturn(IntType())
        when(mockedFactCol.copyWith(any(classOf[ColumnContext]), any(classOf[Map[String,String]]), any())).thenReturn(mockedFactCol)

        Fact.newFact("fact1", DailyGrain, HiveEngine, Set(AdvertiserSchema),
          Set(
            DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
            , DimCol("dimcol2", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("dimcol3", IntType())),
          Set(
            FactCol("factcol1", StrType())
            , mockedFactCol
          )).newRollUp("fact2", "fact1", Set("dimcol3"))
      }
    }
    thrown.getMessage should startWith ("requirement failed: Failed to copy fact columns with new column context!")
  }

  test("newRollup should fail if trying to discard fact columns") {
    val fact = fact1
    val thrown = intercept[IllegalArgumentException] {
      fact.newRollUp("fact2", "fact1", Set("clicks"))
    }
    thrown.getMessage should startWith ("requirement failed: Cannot discard fact column clicks with newRollup, use createSubset to discard")
  }

  test("newRollup should successfully rollup discarding all derived columns associated the discarding columns") {
    val fact = fact1
    fact.newRollUp("fact2", "fact1", Set("engagement_type"))
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.keys.exists(_ == "fact2") === true, "Roll up failed")
  }

  test("newRollup should successfully rollup with column alias map") {
    val fact = fact1
    fact.newRollUp("fact2", "fact1", discarding = Set("engagement_type"), columnAliasMap = Map("price_type" -> "pricing_type"))
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions", "Pricing Type"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.keys.exists(_ == "fact2") === true, "Roll up failed")
    assert(bcOption.get.facts.find(_._1 == "fact2").get._2.fact.columnsByNameMap("price_type").alias.get === "pricing_type", "column alias failed")
  }

  test("newRollup should discard column in hive ddl column rendering") {
    val fact = fact1
    fact.newRollUp("fact2", "fact1", Set("ad_group_id"))
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$toDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.keys.exists(_ == "fact2") === true, "Roll up failed")
    assert(!bcOption.get.facts("fact2").fact.ddlAnnotation.get.asInstanceOf[HiveDDLAnnotation].columnOrdering.contains("ad_group_id"),
      "Failed to discarding column: ad_group_id in ddl column ordering")
  }

  test("should discard the new rollup if the availableOnwardsDate > requested date") {
    val fact = fact1
    fact.newRollUp("fact2", "fact1", Set("ad_group_id"), availableOnwardsDate = Some(s"$toDate"))
    val bcOption = publicFact(fact).getCandidatesFor(AdvertiserSchema, SyncRequest, Set("Advertiser Id", "Impressions"), Set.empty, Map("Advertiser Id" -> InFilterOperation), 1, 1, EqualityFilter("Day", s"$fromDate"))
    require(bcOption.isDefined, "Failed to get candidates!")
    assert(bcOption.get.facts.keys.exists(_ == "fact2") === false, "should discard best candidate of new rollup based on the availableOnwardsDate")
  }

  test("newViewTableRollUp: Creating a rollup with invalid from table should fail.") {
    val thrown = intercept[IllegalArgumentException] {
      unionViewRollupBuilder.newViewTableRollUp(unionViewCampaign, "this_does_not_exist", Set.empty)
    }
    assert(thrown.getMessage.contains("from table not valid this_does_not_exist"), "Invalid from-table should not be accepted")
  }

  test("newViewTableRollUp: Can't duplicate an existing view.") {
    val thrown = intercept[IllegalArgumentException] {
      unionViewRollupBuilder.newViewTableRollUp(unionViewCampaign, "campaign_adjustment_view", Set.empty)
    }
    assert(thrown.getMessage.contains("table campaign_adjustment_view already exists"), "Duplication of a table should not be possible.")
  }

  test("newViewTableRollUp: New rollup must have discardings and should fail otherwise.") {
    val thrown = intercept[IllegalArgumentException] {
      unionViewRollupBuilder.newViewTableRollUp(newUnionToMerge, "account_adjustment_view", Set.empty)
    }
    assert(thrown.getMessage.contains("discardings should never be empty in rollup"), "Something must be discarded for a rollup.")
  }

  test("newViewTableRollUp: Fail to discard required fact with foreign keys.") {
    val thrown = intercept[IllegalArgumentException] {
      unionViewRollupBuilder.newViewTableRollUp(newUnionToMerge, "account_adjustment_view", Set("advertiser_id"))
    }
    assert(thrown.getMessage.contains("Fact has no foreign keys after discarding Set(advertiser_id)"), "Discarding foreign key should fail.")
  }

  test("newViewTableRollUp: Fail to discard required fact.") {
    val thrown = intercept[IllegalArgumentException] {
      unionViewRollupBuilder.newViewTableRollUp(newUnionToMerge, "account_adjustment_view", Set("impressions"))
    }
    assert(thrown.getMessage.contains("Cannot discard fact column impressions with newRollup"), "Discarding necessary fact should fail.")
  }

  test("newViewTableRollUp: Fail to discard fake dim.") {
    val thrown = intercept[IllegalArgumentException] {
      unionViewRollupBuilder.newViewTableRollUp(newUnionToMerge, "account_adjustment_view", Set("fake_dim"))
    }
    assert(thrown.getMessage.contains("dim column fake_dim does not exist"), "Inexistent dim should throw an error.")
  }
}


