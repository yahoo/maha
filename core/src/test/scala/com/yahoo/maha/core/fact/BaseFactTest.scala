// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.fact

import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core._
import com.yahoo.maha.core.ddl.HiveDDLAnnotation
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.request.{AsyncRequest, RequestType, SyncRequest}
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.{FunSuite, Matchers}

/**
 * Created by jians on 10/20/15.
 */
trait BaseFactTest extends FunSuite with Matchers {

  protected[this] val fromDate = DailyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC).minusDays(7))
  protected[this] val toDate = DailyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC))

  CoreSchema.register()
  implicit def toSetRequestCol(s: String) : RequestCol = BaseRequestCol(s)

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

  def fact1 : FactBuilder = {
    fact1WithForceFilters(Set.empty)
  }

  def facto : FactBuilder = {
    factOWithOracle()
  }

  def factd : FactBuilder = {
    factDWithDruid()
  }

  def factp : FactBuilder = {
    factPWithPresto()
  }

  //create a new fact builder for each call
  def fact1WithForceFilters(forceFilters: Set[ForceFilter]) : FactBuilder = {
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      import com.yahoo.maha.core.BaseExpressionTest._
      import com.yahoo.maha.core.HiveExpression._
      Fact.newFact(
        "fact1", DailyGrain, HiveEngine, Set(AdvertiserSchema),
        Set(
          DimCol("account_id", IntType(), annotations = Set(ForeignKey("cache_advertiser_metadata")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3))
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("engagement_type", IntType(3))
        ),
        Set(
          FactCol("impressions", IntType())
          , FactCol("clicks", IntType())
          , FactCol("engagement_count", IntType(0, 0))
          , ConstFactCol("constant_count", IntType(), "1")
          , HiveDerFactCol("Video Starts", IntType(), SUM(DECODE("{engagement_type}", "5", "{engagement_count}", "0")))
        )
        , forceFilters = forceFilters
        , ddlAnnotation = Option(new HiveDDLAnnotation(columnOrdering =
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
            "engagement_type",
            "constant_count"
          )))
      )
    }
  }

  def factOWithOracle(forceFilters: Set[ForceFilter] = Set.empty) : FactBuilder = {
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      import com.yahoo.maha.core.OracleExpression._
      Fact.newFact(
        "facto", DailyGrain, OracleEngine, Set(AdvertiserSchema),
        Set(
          DimCol("account_id", IntType(), annotations = Set(ForeignKey("cache_advertiser_metadata")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3))
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("engagement_type", IntType(3))
        ),
        Set(
          FactCol("impressions", IntType())
          , FactCol("clicks", IntType())
          , FactCol("engagement_count", IntType(0, 0))
          , ConstFactCol("constant_count", IntType(), "1")
          , OracleDerFactCol("Video Starts", IntType(), SUM("{engagement_count}"))
        )
        , forceFilters = forceFilters
      )
    }
  }

  def factDWithDruid(forceFilters: Set[ForceFilter] = Set.empty) : FactBuilder = {
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      Fact.newFact(
        "factd", DailyGrain, DruidEngine, Set(AdvertiserSchema),
        Set(
          DimCol("account_id", IntType(), annotations = Set(ForeignKey("cache_advertiser_metadata")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3))
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("engagement_type", IntType(3))
        ),
        Set(
          FactCol("impressions", IntType())
          , FactCol("clicks", IntType())
          , FactCol("engagement_count", IntType(0, 0))
          , ConstFactCol("constant_count", IntType(), "1")
          , DruidDerFactCol("Video Starts", IntType(), ("{engagement_count}"))
        )
        , forceFilters = forceFilters
      )
    }
  }

  def factPWithPresto(forceFilters: Set[ForceFilter] = Set.empty) : FactBuilder = {
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      Fact.newFact(
        "factp", DailyGrain, PrestoEngine, Set(AdvertiserSchema),
        Set(
          DimCol("account_id", IntType(), annotations = Set(ForeignKey("cache_advertiser_metadata")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3))
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("engagement_type", IntType(3))
        ),
        Set(
          FactCol("impressions", IntType())
          , FactCol("clicks", IntType())
          , FactCol("engagement_count", IntType(0, 0))
          , ConstFactCol("constant_count", IntType(), "1")
          , PrestoDerFactCol("Video Starts", IntType(), ("{engagement_count}"))
        )
        , forceFilters = forceFilters
      )
    }
  }

  def publicFact(fb: FactBuilder, forcedFilters: Set[ForcedFilter] = Set.empty, powerSetStorage: PowerSetStorage = new DefaultPowerSetStorage): PublicFact = {
    fb.toPublicFact("publicFact",
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
      forcedFilters,
      getMaxDaysWindow, getMaxDaysLookBack,
      powerSetStorage = powerSetStorage
    )
  }

  def fact1WithAnnotationWithEngineRequirement : FactBuilder = {
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
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
        ),
        Set(
          FactCol("impressions", IntType())
          , FactCol("clicks", IntType(), annotations = Set(HiveSnapshotTimestamp))
        )
      )
    }
  }
  
  def fact1WithRollupWithEngineRequirement : FactBuilder = {
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
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
        ),
        Set(
          FactCol("impressions", IntType())
          , FactCol("clicks", IntType(), HiveCustomRollup("rollup"))
        )
      )
    }
  }

  def fact1WithFactLevelMaxLookbackWindow : FactBuilder = {
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
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
        ),
        Set(
          FactCol("impressions", IntType())
          , FactCol("clicks", IntType(), HiveCustomRollup("rollup"))
        ),
        Set.empty, None, Map.empty, Set.empty, 0, 0, None, None,
        maxDaysWindow = Option(Map(SyncRequest -> 31, AsyncRequest -> 400)),
        Option(Map(SyncRequest -> 50, AsyncRequest -> 50))
      )
    }
  }

  def factDerivedWithFailingDimCol : FactBuilder = {
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      //import HiveExpression._
      Fact.newFact(
        "fact1", DailyGrain, HiveEngine, Set(AdvertiserSchema),
        Set(
          DimCol("account_id", IntType(), annotations = Set(ForeignKey("cache_advertiser_metadata")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("cache_campaign_metadata")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3), annotations = Set(HiveSnapshotTimestamp))
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
        ),
        Set(
          FactCol("impressions", IntType())
          , FactCol("clicks", IntType())
        )
      )
    }
  }

  def factDerivedWithOverridableStaticMaps : FactBuilder = {
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
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("static_1", IntType(1, (Map(1->"Temp"), "Nothing")))
        ),
        Set(
          FactCol("impressions", IntType())
          , FactCol("clicks", IntType())
          , FactCol("static_2", IntType(1, (Map(1->"Temp"), "Nothing")))
        )
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

  val accountStatsView = factForViewCampaignStats.copyWith("account_stats", Set("campaign_id"), Map.empty)
  val accountAdjustmentView = factForViewCampaignAdjustments.copyWith("account_adjustment", Set("campaign_id"), Map.empty)
  val newStatsView = factForViewCampaignAdjustments.copyWith("new_stats", Set("Month"), Map.empty)
  val newAdjustmentView = factForViewCampaignAdjustments.copyWith("new_adjustment", Set("Month"), Map.empty)

  val unionViewCampaign = UnionView("campaign_adjustment_view", Seq(factForViewCampaignStats, factForViewCampaignAdjustments))
  val unionViewAccount = UnionView("account_adjustment_view", Seq(accountStatsView, accountAdjustmentView))
  val newUnionToMerge = UnionView("new_stats_view", Seq(newStatsView, newAdjustmentView))

  val unionViewRollupBuilder = {
    ColumnContext.withColumnContext {
      import com.yahoo.maha.core.OracleExpression._
      implicit dc: ColumnContext =>
        Fact.newUnionView(unionViewCampaign, DailyGrain, OracleEngine, Set(AdvertiserSchema, ResellerSchema),
          Set(
            DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("stats_date", DateType("YYYY-MM-DD"))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
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
      .newViewTableRollUp(unionViewAccount, "campaign_adjustment_view", Set("campaign_id"))
  }

}
