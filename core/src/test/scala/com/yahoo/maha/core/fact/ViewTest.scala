// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.fact

import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension.{ConstDimCol, DimCol, OracleDerDimCol}
import com.yahoo.maha.core.lookup.LongRangeLookup
import com.yahoo.maha.core.request.{AsyncRequest, RequestType, SyncRequest}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Created by jians on 10/20/15.
 */
class ViewTest extends AnyFunSuite with Matchers {

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

  def factForViewCampaignAdjustments = {
    import com.yahoo.maha.core.OracleExpression._
    ColumnContext.withColumnContext {
      implicit dc: ColumnContext =>
        Fact.newFactForView(
          "campaign_adjustments", DailyGrain, OracleEngine, Set(AdvertiserSchema, ResellerSchema),
          Set(
            DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
            , DimCol("stats_date", DateType("YYYY-MM-DD"))
            , OracleDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
            , OracleDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "w"))
          ),
          Set(
            FactCol("impressions", IntType(3, 1))
            , FactCol("clicks", IntType(3, 0, 1, 800))
            , FactCol("spend", DecType(0, "0.0"))
          )
        )
    }
  }

  def factForViewCampaignStats = {
    import com.yahoo.maha.core.OracleExpression._
    ColumnContext.withColumnContext {
      implicit dc: ColumnContext =>
        Fact.newFactForView(
          "campaign_stats", DailyGrain, OracleEngine, Set(AdvertiserSchema, ResellerSchema),
          Set(
            DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
            , DimCol("stats_date", DateType("YYYY-MM-DD"))
            , OracleDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
            , OracleDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "w"))
          ),
          Set(
            FactCol("impressions", IntType(3, 1))
            , FactCol("clicks", IntType(3, 0, 1, 800))
            , FactCol("spend", DecType(0, "0.0"))
          )
        )
    }
  }

  def factForViewCampaignStatsWithoutSpend = {
    import com.yahoo.maha.core.OracleExpression._
    ColumnContext.withColumnContext {
      implicit dc: ColumnContext =>
        Fact.newFactForView(
          "campaign_stats", DailyGrain, OracleEngine, Set(AdvertiserSchema, ResellerSchema),
          Set(
            DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
            , DimCol("stats_date", DateType("YYYY-MM-DD"))
            , OracleDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
            , OracleDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "w"))
          ),
          Set(
            FactCol("impressions", IntType(3, 1))
            , FactCol("clicks", IntType(3, 0, 1, 800))
          )
        )
    }
  }

  def factForViewCampaignStatsDifferentDim = {
    import com.yahoo.maha.core.OracleExpression._
    ColumnContext.withColumnContext {
      implicit dc: ColumnContext =>
        Fact.newFactForView(
          "campaign_stats", DailyGrain, OracleEngine, Set(AdvertiserSchema, ResellerSchema),
          Set(
            DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
            , DimCol("stats_date2", DateType("YYYY-MM-DD"))
            , OracleDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date2}", "M"))
            , OracleDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date2}", "w"))
          ),
          Set(
            FactCol("impressions", IntType(3, 1))
            , FactCol("clicks", IntType(3, 0, 1, 800))
            , FactCol("spend", DecType(0, "0.0"))
          )
        )
    }
  }

  def factForViewCampaignStatsDifferentFact = {
    import com.yahoo.maha.core.OracleExpression._
    ColumnContext.withColumnContext {
      implicit dc: ColumnContext =>
        Fact.newFactForView(
          "campaign_stats", DailyGrain, OracleEngine, Set(AdvertiserSchema, ResellerSchema),
          Set(
            DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
            , DimCol("stats_date", DateType("YYYY-MM-DD"))
            , OracleDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
            , OracleDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "w"))
          ),
          Set(
            FactCol("impressions", IntType(3, 1))
            , FactCol("clicks", IntType(3, 0, 1, 800))
            , FactCol("spend2", DecType(0, "0.0"))
          )
        )
    }
  }

  val accountStatsView = factForViewCampaignStats.copyWith("account_stats", Set("campaign_id"), Map.empty)
  val accountStatsViewWithoutSpend = factForViewCampaignStatsWithoutSpend.copyWith("account_stats", Set("campaign_id"), Map.empty)
  val accountStatsViewDifferentDim = factForViewCampaignStatsDifferentDim.copyWith("account_stats", Set("campaign_id"), Map.empty)
  val accountStatsViewDifferentFact = factForViewCampaignStatsDifferentFact.copyWith("account_stats", Set("campaign_id"), Map.empty)
  val accountAdjustmentView = factForViewCampaignAdjustments.copyWith("account_adjustment", Set("campaign_id"), Map.empty)


  //Create a ViewBaseTable to test
  val campaignAdjustment  = {
    import com.yahoo.maha.core.OracleExpression._
    ColumnContext.withColumnContext {
      implicit dc: ColumnContext =>
        Fact.newFactForView(
          "campaign_adjustments", DailyGrain, OracleEngine, Set(AdvertiserSchema, ResellerSchema),
          Set(
            DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
            , DimCol("stats_date", DateType("YYYY-MM-DD"))
            , ConstDimCol("constant_dim_one", IntType(), "1")
            , ConstDimCol("constant_dim_two", IntType(), "2")
            , OracleDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
            , OracleDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "w"))
          ),
          Set(
            FactCol("impressions", IntType(3, 1))
            , FactCol("clicks", IntType(3, 0, 1, 800))
            , FactCol("spend", DecType(0, "0.0"))
            , OracleDerFactCol("derived", IntType(), SUM("impressions"))
          ), forceFilters = Set(ForceFilter(EqualityFilter("Advertiser ID", "2", isForceFilter = true, isOverridable = true)))
        )
    }
  }

  test("ViewBaseTable: copyWith should fail if attempting to discard a fact.") {
    val caught = intercept[IllegalArgumentException] {
      val shouldFailWithInvalidDiscardingFact = {
        ColumnContext.withColumnContext {
          implicit dc: ColumnContext =>
            campaignAdjustment.copyWith("temp_adjust", Set("impressions"), Map.empty)
        }
      }
    }
    assert(caught.getMessage.contains("Can not discard fact col"), "Illegal fact discarding should throw a discarding error but threw " + caught.getMessage)
  }

  test("ViewBaseTable: copyWith should fail if attempting to discard a nonexistent Dim.") {
    val caught = intercept[IllegalArgumentException] {
      val shouldFailWithInvalidDiscardingFact = {
        ColumnContext.withColumnContext {
          implicit dc: ColumnContext =>
            campaignAdjustment.copyWith("temp_adjust", Set("this_is_a_fake_dim"), Map.empty)
        }
      }
    }
    assert(caught.getMessage.contains("Discarding col this_is_a_fake_dim is not existed in the base ViewTable campaign_adjustments"), "Illegal dim discarding should throw a discarding error but threw " + caught.getMessage)
  }

  test("ViewBaseTable: copyWith should fail to overwrite existing fact cols with newConstNameToValueMap.") {
    val caught = intercept[IllegalArgumentException] {
      val shouldFailWithInvalidDiscardingFact = {
        ColumnContext.withColumnContext {
          implicit dc: ColumnContext =>
            campaignAdjustment.copyWith("temp_adjust", Set("campaign_id"), Map("impressions" -> "not_impressions"))
        }
      }
    }
    assert(caught.getMessage.contains("Can not override the constant fact column impressions to override with new constant value"), "Illegal fact overwrite should throw a discarding error but threw " + caught.getMessage)
  }

  test("ViewBaseTable: copyWith should fail to overwrite non-existent dim cols with newConstNameToValueMap.") {
    val caught = intercept[IllegalArgumentException] {
      val shouldFailWithInvalidDiscardingFact = {
        ColumnContext.withColumnContext {
          implicit dc: ColumnContext =>
            campaignAdjustment.copyWith("temp_adjust", Set("campaign_id"), Map("this_is_a_fake_dimension" -> "fake_replacement"))
        }
      }
    }
    assert(caught.getMessage.contains("Unable to find the constant dim column this_is_a_fake_dimension to override with new constant value"), "Illegal fact overwrite should throw a discarding error but threw " + caught.getMessage)
  }

  test("ViewBaseTable: copyWith should fail to overwrite dim cols with non-constant values with newConstNameToValueMap.") {
    val caught = intercept[IllegalArgumentException] {
      val shouldFailWithInvalidDiscardingFact = {
        ColumnContext.withColumnContext {
          implicit dc: ColumnContext =>
            campaignAdjustment.copyWith("temp_adjust", Set("campaign_id"), Map("stats_date" -> "fake_replacement"))
        }
      }
    }
    assert(caught.getMessage.contains("present col should be instance of constant dim column stats_date to override with new constant value"), "Illegal fact overwrite should throw a discarding error but threw " + caught.getMessage)
  }

  test("ViewBaseTable: copyWith should succeed in adding valid constant values with newConstNameToValueMap.") {
      val shouldSucceed = {
        ColumnContext.withColumnContext {
          implicit dc: ColumnContext =>
            campaignAdjustment.copyWith("temp_adjust", Set("campaign_id"), Map("constant_dim_one" -> "15", "constant_dim_two" -> "25"))
        }
      }
    assert(shouldSucceed.columnsByNameMap("constant_dim_one").asInstanceOf[ConstDimCol].constantValue == "15", "Mapped column constant_dim_one should hold new constant value 15")
    assert(shouldSucceed.columnsByNameMap("constant_dim_two").asInstanceOf[ConstDimCol].constantValue == "25", "Mapped column constant_dim_two should hold new constant value 25")


  }

  test("ViewBaseTable: instantiation should fail without a valid input schema.") {
    val thrown = intercept[IllegalArgumentException] {
      val missingSchema = {
        ColumnContext.withColumnContext {
          implicit dc: ColumnContext =>
            Fact.newFactForView(
              "missing_schemas", DailyGrain, OracleEngine, Set(),
              Set(
                DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
                , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
                , DimCol("stats_date", DateType("YYYY-MM-DD"))
              ),
              Set(
                FactCol("impressions", IntType(3, 1))
                , FactCol("clicks", IntType(3, 0, 1, 800))
              )
            )
        }
      }
    }

    assert(thrown.getMessage.contains("Failed schema requirement fact=missing_schemas, engine=Oracle, schemas=Set()"))
  }

  test("ViewBaseTable: Incorrect engine should throw an error.") {
    val thrown = intercept[IllegalArgumentException] {
      val missingSchema = {
        ColumnContext.withColumnContext {
          implicit dc: ColumnContext =>
            Fact.newFactForView(
              "missing_schemas", DailyGrain, OracleEngine, Set(AdvertiserSchema),
              Set(
                DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
                , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
                , DimCol("stats_date", DateType("YYYY-MM-DD"))
              ),
              Set(
                FactCol("impressions", IntType(3, 1))
                , FactCol("clicks", IntType(3, 0, 1, 800))
                , HiveDerFactCol("wrong_engine", StrType(), "")
              )
            )
        }
      }
    }

    assert(thrown.getMessage.contains("Failed engine requirement fact=missing_schemas, engine=Oracle, col=HiveDerFactCol(wrong_engine"))
  }

  test("ViewBaseTable: Throw error if declared derived expression not derived.") {
    val thrown = intercept[IllegalArgumentException] {
      val missingSchema = {
        ColumnContext.withColumnContext {
          implicit dc: ColumnContext =>
            Fact.newFactForView(
              "missing_schemas", DailyGrain, OracleEngine, Set(AdvertiserSchema),
              Set(
                DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
                , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
                , DimCol("stats_date", DateType("YYYY-MM-DD"))
              ),
              Set(
                FactCol("impressions", IntType(3, 1))
                , FactCol("clicks", IntType(3, 0, 1, 800))
                , OracleDerFactCol("empty_col", StrType(), null)
              )
            )
        }
      }
    }

    assert(thrown.getMessage.contains("Derived expression should be defined for a derived column OracleDerFactCol(empty_col"))
  }

  test("ViewBaseTable: Throw error if declared derived expression uses false columns.") {
    val thrown = intercept[IllegalArgumentException] {
      val missingSchema = {
        ColumnContext.withColumnContext {
          implicit dc: ColumnContext =>
            Fact.newFactForView(
              "missing_schemas", DailyGrain, OracleEngine, Set(AdvertiserSchema),
              Set(
                DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
                , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
                , DimCol("stats_date", DateType("YYYY-MM-DD"))
              ),
              Set(
                FactCol("impressions", IntType(3, 1))
                , FactCol("clicks", IntType(3, 0, 1, 800))
                , OracleDerFactCol("empty_col", StrType(), "{false_col}" ++ "{another_false_col}")
              )
            )
        }
      }
    }

    assert(thrown.getMessage.contains("Failed derived expression validation, unknown referenced column in fact=missing_schemas, false_col"))
  }

  test("ViewBaseTable: Throw error if declared derived expression references itself (infinite loop case).") {
    val thrown = intercept[IllegalArgumentException] {
      val missingSchema = {
        ColumnContext.withColumnContext {
          implicit dc: ColumnContext =>
            Fact.newFactForView(
              "missing_schemas", DailyGrain, OracleEngine, Set(AdvertiserSchema),
              Set(
                DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
                , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
                , DimCol("stats_date", DateType("YYYY-MM-DD"))
              ),
              Set(
                FactCol("impressions", IntType(3, 1))
                , FactCol("clicks", IntType(3, 0, 1, 800))
                , OracleDerFactCol("empty_col", StrType(), "{empty_col}" ++ "{empty_col}")
              )
            )
        }
      }
    }

    assert(thrown.getMessage.contains("Derived column is referring to itself, this will cause infinite loop : fact=missing_schemas, column=empty_col, sourceColumns=Set(empty_col)"))
  }

  test("ViewBaseTable: Incorrect engine in annotations should throw an error.") {
    val thrown = intercept[IllegalArgumentException] {
      {
        ColumnContext.withColumnContext {
          implicit dc: ColumnContext =>
            Fact.newFactForView(
              "missing_schemas", DailyGrain, OracleEngine, Set(AdvertiserSchema),
              Set(
                DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
                , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
                , DimCol("stats_date", DateType("YYYY-MM-DD"))
              ),
              Set(
                FactCol("impressions", IntType(3, 1))
                , FactCol("clicks", IntType(3, 0, 1, 800))
                , FactCol("wrong_engine", IntType(), annotations = Set(HiveShardingExpression("impressions" ++ "impressions")))
              )
            )
        }
      }
    }

    assert(thrown.getMessage.contains("Failed engine requirement fact=missing_schemas, engine=Oracle, col=wrong_engine, annotation=HiveShardingExpression"))
  }

  test("Should fail to create UnionView with differing Fact Columns") {
    val thrown = intercept[IllegalArgumentException] {
      UnionView("account_adjustment_view", Seq(accountStatsViewWithoutSpend, accountAdjustmentView))
    }
    assert(thrown.getMessage.contains("should have same number of fact columns"))
  }

  test("Should fail to create UnionView with discarded dims difference") {
    val accountStatsViewDiscardDim = factForViewCampaignStats.copyWith("account_stats", Set("campaign_id", "stats_date"), Map.empty)
    val thrown = intercept[IllegalArgumentException] {
      UnionView("account_adjustment_view", Seq(accountStatsViewDiscardDim, accountAdjustmentView))
    }
    assert(thrown.getMessage.contains("should have same number of dim columns"))
  }

  test("UnionView takes two or more facts") {
    val thrown = intercept[IllegalArgumentException] {
      UnionView("account_adjustment_view", Seq(accountAdjustmentView))
    }
    assert(thrown.getMessage.contains("Require 2 or more facts in order to create view"))
  }

  test("UnionView should fail with different dimCol names") {
    val thrown = intercept[IllegalArgumentException] {
      UnionView("account_adjustment_view", Seq(accountAdjustmentView, accountStatsViewDifferentDim))
    }
    assert(thrown.getMessage.contains("Col mismatch stats_date2 in view account_stats, account_adjustment"))
  }

  test("UnionView should fail with different factCol names") {
    val thrown = intercept[IllegalArgumentException] {
      UnionView("account_adjustment_view", Seq(accountAdjustmentView, accountStatsViewDifferentFact))
    }
    assert(thrown.getMessage.contains("Col mismatch spend2 in view account_stats, account_adjustment"))
  }

}
