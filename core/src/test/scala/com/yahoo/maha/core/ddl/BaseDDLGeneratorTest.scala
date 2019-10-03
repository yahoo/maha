// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.ddl

import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core._
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.request.{AsyncRequest, SyncRequest, RequestType}
import org.scalatest.{BeforeAndAfterAll, Matchers, FunSuite}

/**
 * Created by shengyao on 3/10/16.
 */
class BaseDDLGeneratorTest extends FunSuite with Matchers with BeforeAndAfterAll {
  CoreSchema.register()

  protected[this] def getMaxDaysWindow: Map[(RequestType, Grain), Int] = {
    Map((SyncRequest, DailyGrain) -> 31, (AsyncRequest, DailyGrain) -> 400)
  }

  protected[this] def getMaxDaysLookBack: Map[(RequestType, Grain), Int] = {
    val result = 400
    Map((SyncRequest, DailyGrain) -> result, (AsyncRequest, DailyGrain) -> result)
  }

  val factBuilder : FactBuilder = {
    import com.yahoo.maha.core.HiveExpression._
    import com.yahoo.maha.core.BaseExpressionTest._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "ad_k_stats", DailyGrain, HiveEngine, Set(AdvertiserSchema, AdvertiserLowLatencySchema, ResellerSchema),
        Set(
          DimCol("account_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
          , DimCol("term_id", IntType(), annotations = Set(ForeignKey("keyword")))
          , DimCol("Stats_Date_Test", DateType("YYYY-MM-dd"), alias = Option("stats_date"))
          , DimCol("stats_date_old", DateType("YYYY-MM-dd"), alias = Option("stats_date"))
          , DimCol("stats_dates", DateType("YYYY-MM-dd"), alias = Option("stats_date"))
          , HiveDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date_old}", "M"))
          , HiveDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date_old}", "W"))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("device_type_id", IntType(10, (Map(1 -> "Desktop", 2 -> "Tablet", 3 -> "SmartPhone", -1 -> "UNKNOWN", -3 -> ""), "NONE")))
        ),
        Set(
          FactCol("impressions", IntType(10, 0))
          , FactCol("clicks", IntType(10, 0))
          , FactCol("conversions", IntType(0, 0))
          , FactCol("post_imp_conversions", IntType(0, 0))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          , FactCol("avg_pos", DecType(8, "1.0", "500.0", "1.0"), HiveCustomRollup(SUM("{avg_pos}" * "{impressions}") /- SUM("{impressions}")))
          , HiveDerFactCol("total_conversions", IntType(), SUM(COALESCE("{conversions}", "0")) ++ COALESCE("{post_imp_conversions}", "0"))
          , HiveDerFactCol("Conversions", IntType(), SUM(COALESCE("{conversions}", "0")) ++ COALESCE("{post_imp_conversions}", "0"))
          , HiveDerFactCol("average_cpc", DecType(), "{spend}" /- "{clicks}")
          , HiveDerFactCol("average_cost_per_install", DecType(), "{spend}" /- "{total_conversions}")
          , HiveDerFactCol("average_cpm", DecType(), "{spend}" /- "{impressions}" * "1000")
          , HiveDerFactCol("ctr", DecType(), "{clicks}" /- "{impressions}" * "1000")
        ),
        ddlAnnotation = Option(HiveDDLAnnotation(Map(("hive.fieldDelimiter", "\\001"))
          , columnOrdering = IndexedSeq(
              "account_id",
              "campaign_id",
              "ad_group_id",
              "ad_id",
              "term_id",
              "Stats_Date_Test",
              "stats_date_old",
              "stats_dates",
              "impressions",
              "clicks",
              "conversions",
              "post_imp_conversions",
              "spend",
              "stats_source",
              "price_type",
              "landing_page_url",
              "device_type_id",
              "max_bid",
              "avg_pos"
              ))),
        defaultCardinality = 10,
        defaultRowCount = 100
      )

    }

  }

  ColumnContext.withColumnContext { implicit dc: ColumnContext =>
    import com.yahoo.maha.core.OracleExpression._
    factBuilder
      .withAlternativeEngine("cb_ad_k_stats", "ad_k_stats", OracleEngine,
        Set(
          OracleDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date_old}", "M")),
          OracleDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date_old}", "W"))
        ),
        Set(
          FactCol("impressions", IntType(10, 0))
          , FactCol("avg_pos", DecType(), OracleCustomRollup(SUM("{avg_pos}" * "{impressions}") /- SUM("{impressions}")))
          , OracleDerFactCol("total_conversions", IntType(), SUM(COALESCE("{conversions}", "0")) ++ COALESCE("{post_imp_conversions}", "0"))
          , OracleDerFactCol("Conversions", IntType(), SUM(COALESCE("{conversions}", "0")) ++ COALESCE("{post_imp_conversions}", "0"))
          , OracleDerFactCol("average_cpc", DecType(), "{spend}" /- "{clicks}")
          , OracleDerFactCol("average_cost_per_install", DecType(), "{spend}" /- "{total_conversions}")
          , OracleDerFactCol("average_cpm", DecType(), "{spend}" /- "{impressions}" * "1000")
          , OracleDerFactCol("ctr", DecType(), "{clicks}" /- "{impressions}" * "1000")
        ),
        overrideAnnotations = Set(
          OracleFactStaticHint("PARALLEL_INDEX(cb_ad_k_stats 4)"),
          OracleFactDimDrivenHint("PUSH PRED PARALLEL_INDEX(cb_ad_k_stats 4)")
        ),
        overrideDDLAnnotation = Option( OracleDDLAnnotation(pks = Set("account_id", "stats_date_old", "stats_source", "price_type"),
          partCols = Set("stats_date_old")) ),
        columnAliasMap = Map("stats_date_old" -> "stats_date")
      )
  }

  ColumnContext.withColumnContext { implicit dc: ColumnContext =>
    import com.yahoo.maha.core.PostgresExpression._
    factBuilder
      .withAlternativeEngine("pg_ad_k_stats", "ad_k_stats", PostgresEngine,
        Set(
          PostgresDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date_old}", "M")),
          PostgresDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date_old}", "W"))
        ),
        Set(
          FactCol("impressions", IntType(10, 0))
          , FactCol("avg_pos", DecType(), PostgresCustomRollup(SUM("{avg_pos}" * "{impressions}") /- SUM("{impressions}")))
          , PostgresDerFactCol("total_conversions", IntType(), SUM(COALESCE("{conversions}", "0")) ++ COALESCE("{post_imp_conversions}", "0"))
          , PostgresDerFactCol("Conversions", IntType(), SUM(COALESCE("{conversions}", "0")) ++ COALESCE("{post_imp_conversions}", "0"))
          , PostgresDerFactCol("average_cpc", DecType(), "{spend}" /- "{clicks}")
          , PostgresDerFactCol("average_cost_per_install", DecType(), "{spend}" /- "{total_conversions}")
          , PostgresDerFactCol("average_cpm", DecType(), "{spend}" /- "{impressions}" * "1000")
          , PostgresDerFactCol("ctr", DecType(), "{clicks}" /- "{impressions}" * "1000")
        ),
        overrideAnnotations = Set(
          PostgresFactStaticHint("PARALLEL_INDEX(cb_ad_k_stats 4)"),
          PostgresFactDimDrivenHint("PUSH PRED PARALLEL_INDEX(cb_ad_k_stats 4)")
        ),
        overrideDDLAnnotation = Option( PostgresDDLAnnotation(pks = Set("account_id", "stats_date_old", "stats_source", "price_type"),
          partCols = Set("stats_date_old")) ),
        columnAliasMap = Map("stats_date_old" -> "stats_date")
      )
  }

  def pubFact = factBuilder
    .toPublicFact("k_stats",
      Set(
        PubCol("account_id", "Advertiser ID", InEquality),
        PubCol("campaign_id", "Campaign ID", In),
        PubCol("ad_group_id", "Ad Group ID", In),
        PubCol("ad_id", "Ad ID", In),
        PubCol("term_id", "Keyword ID", In),
        PubCol("Month", "Month", Set.empty),
        PubCol("Week", "Week", Set.empty),
        PubCol("stats_dates", "Day", InBetweenEquality),
        PubCol("stats_date_old", "Dayeeee", InBetweenEquality),
        PubCol("Stats_Date_Test", "Stats Date Test", InBetweenEquality),

        PubCol("stats_source", "Source", Equality),
        PubCol("price_type", "Pricing Type", In),
        PubCol("landing_page_url", "Destination URL", Set.empty),
        PubCol("device_type_id", "Device Type", InEquality)
      ),
      Set(
        PublicFactCol("impressions", "Impressions", InEquality),
        PublicFactCol("clicks", "Clicks", In),
        PublicFactCol("conversions", "Post Click Conversions", In),
        PublicFactCol("post_imp_conversions", "Post Impression Conversions", In),
        PublicFactCol("total_conversions", "Total Conversions", In),
        PublicFactCol("Conversions", "Conversions", Set.empty),
        PublicFactCol("spend", "Spend", Set.empty),
        PublicFactCol("avg_pos", "Average Position", Set.empty),
        PublicFactCol("max_bid", "Max Bid", Set.empty),
        PublicFactCol("average_cpc", "Average CPC", Set.empty),
        PublicFactCol("average_cost_per_install", "Average Cost-per-install", Set.empty),
        PublicFactCol("average_cpm", "Average CPM", Set.empty),
        PublicFactCol("ctr", "CTR", Set.empty)
      ),
      Set(EqualityFilter("Source", "2", isForceFilter = true)),
      getMaxDaysWindow, getMaxDaysLookBack
    )

  val dimensionBuilder : DimensionBuilder = {
    import HiveExpression._
    import com.yahoo.maha.core.BaseExpressionTest._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Dimension.newDimension(
        "cache_advertiser", HiveEngine, LevelFour,
        Set(AdvertiserSchema, AdvertiserLowLatencySchema, ResellerSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey)),
          DimCol("mdm_company_name", StrType(255)),
          DimCol("mdm_id", IntType()),
          DimCol("status", StrType(255)),
          DimCol("am_contact", StrType(1000)),
          DimCol("website_url", StrType(1000)),
          DimCol("tier", StrType()),
          DimCol("channel", StrType()),
          DimCol("timezone", StrType()),
          DimCol("currency", StrType()),
          DimCol("vat_id", StrType()),
          DimCol("booking_country", StrType()),
          DimCol("adv_type", StrType()),
          DimCol("managed_by", IntType()),
          DimCol("crm_id", StrType()),
          DimCol("billing_country", StrType()),
          DimCol("account_type", StrType()),
          DimCol("is_test", IntType()),
          DimCol("created_by_user", StrType()),
          DimCol("created_date", DateType()),
          DimCol("last_updated_by_user", StrType()),
          DimCol("last_updated", DateType()),
          DimCol("snapshot_ts", IntType(10), annotations = Set(HiveSnapshotTimestamp)),
          DimCol("shard_id", IntType(3)),
          HiveDerDimCol("Advertiser Status", StrType(), DECODE_DIM("{status}", "'ON'", "'ON'", "'OFF'")),
          HiveDerDimCol("Advertiser Date Created", StrType(), TIMESTAMP_TO_FORMATTED_DATE("{created_date}", "YYYY-MM-DD")),
          HiveDerDimCol("Advertiser Date Modified", StrType(), TIMESTAMP_TO_FORMATTED_DATE("{created_date}", "YYYY-MM-DD")),
          HivePartDimCol("load_time", StrType()),
          HivePartDimCol("type", StrType()),
          HivePartDimCol("shard", StrType())
        )
        , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        , schemaColMap = Map(
          AdvertiserSchema -> "id",
          AdvertiserLowLatencySchema -> "id",
          ResellerSchema -> "managed_by")
        , ddlAnnotation = Option(
          HiveDDLAnnotation(Map(
            ("hive.storage", "ORC"),
            ("hive.LOCATION", "$(hivePath)/reports/${name}/")
          ), columnOrdering =
            IndexedSeq(
              "id",
              "mdm_company_name",
              "mdm_id",
              "status",
              "am_contact",
              "website_url",
              "tier",
              "channel",
              "timezone",
              "currency",
              "vat_id",
              "booking_country",
              "adv_type",
              "managed_by",
              "crm_id",
              "billing_country",
              "account_type",
              "is_test",
              "created_by_user",
              "created_date",
              "last_updated_by_user",
              "last_updated",
              "snapshot_ts",
              "shard_id"
            )))
      )

    }
  }

  {
    import OracleExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      dimensionBuilder.withAlternateEngine (
        "oracle_advertiser",
        "cache_advertiser",
        OracleEngine,
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey)),
          DimCol("mdm_company_name", StrType(255)),
          DimCol("mdm_id", IntType()),
          DimCol("status", StrType(255)),
          DimCol("am_contact", StrType(1000)),
          DimCol("website_url", StrType(1000)),
          DimCol("tier", StrType()),
          DimCol("channel", StrType(1024)),
          DimCol("timezone", StrType()),
          DimCol("currency", StrType()),
          DimCol("vat_id", StrType()),
          DimCol("booking_country", StrType()),
          DimCol("adv_type", StrType()),
          DimCol("managed_by", IntType()),
          DimCol("crm_id", StrType()),
          DimCol("billing_country", StrType()),
          DimCol("account_type", StrType()),
          DimCol("is_test", IntType()),
          DimCol("created_by_user", StrType()),
          DimCol("created_date", StrType()),
          DimCol("last_updated_by_user", StrType()),
          DimCol("last_updated", StrType()),
          DimCol("snapshot_ts", IntType(10)),
          DimCol("shard_id", IntType(3)),
          OracleDerDimCol("Advertiser Status", StrType(), DECODE_DIM("{status}", "'ON'", "'ON'", "'OFF'")),
          OracleDerDimCol("Advertiser Date Created", StrType(), FORMAT_DATE("{created_date}", "YYYY-MM-DD")),
          OracleDerDimCol("Advertiser Date Modified", StrType(), FORMAT_DATE("{created_date}", "YYYY-MM-DD"))
        )
        , None
        , annotations = Set(OracleHashPartitioning)
        , ddlAnnotation = Option(OracleDDLAnnotation(pks = Set("id")))
      )
    }
  }


  {
    import PostgresExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      dimensionBuilder.withAlternateEngine (
        "postgres_advertiser",
        "cache_advertiser",
        PostgresEngine,
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey)),
          DimCol("mdm_company_name", StrType(255)),
          DimCol("mdm_id", IntType()),
          DimCol("status", StrType(255)),
          DimCol("am_contact", StrType(1000)),
          DimCol("website_url", StrType(1000)),
          DimCol("tier", StrType()),
          DimCol("channel", StrType(1024)),
          DimCol("timezone", StrType()),
          DimCol("currency", StrType()),
          DimCol("vat_id", StrType()),
          DimCol("booking_country", StrType()),
          DimCol("adv_type", StrType()),
          DimCol("managed_by", IntType()),
          DimCol("crm_id", StrType()),
          DimCol("billing_country", StrType()),
          DimCol("account_type", StrType()),
          DimCol("is_test", IntType()),
          DimCol("created_by_user", StrType()),
          DimCol("created_date", StrType()),
          DimCol("last_updated_by_user", StrType()),
          DimCol("last_updated", StrType()),
          DimCol("snapshot_ts", IntType(10)),
          DimCol("shard_id", IntType(3)),
          PostgresDerDimCol("Advertiser Status", StrType(), DECODE_DIM("{status}", "'ON'", "'ON'", "'OFF'")),
          PostgresDerDimCol("Advertiser Date Created", StrType(), FORMAT_DATE("{created_date}", "YYYY-MM-DD")),
          PostgresDerDimCol("Advertiser Date Modified", StrType(), FORMAT_DATE("{created_date}", "YYYY-MM-DD"))
        )
        , None
        , annotations = Set(PostgresHashPartitioning)
        , ddlAnnotation = Option(PostgresDDLAnnotation(pks = Set("id")))
      )
    }
  }

  val pubDim = dimensionBuilder.toPublicDimension("advertiser","advertiser",
    Set(
      PubCol("id", "Advertiser ID", InEquality),
      PubCol("mdm_company_name", "Advertiser Name", InEquality),
      PubCol("timezone", "Advertiser Timezone", InEquality),
      PubCol("currency", "Advertiser Currency", InEquality),
      PubCol("managed_by", "Reseller ID", InEquality),
      PubCol("status", "Advertiser Status Full", InEquality),
      PubCol("Advertiser Status", "Advertiser Status", InEquality),
      PubCol("Advertiser Date Created", "Advertiser Date Created", InBetweenEquality),
      PubCol("Advertiser Date Modified", "Advertiser Date Modified", InBetweenEquality)
    ), Set.empty
  )


}
