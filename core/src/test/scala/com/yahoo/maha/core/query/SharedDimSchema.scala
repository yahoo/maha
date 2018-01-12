// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.DruidDerivedFunction.{LOOKUP, LOOKUP_WITH_DECODE, LOOKUP_WITH_DECODE_ON_OTHER_COLUMN, LOOKUP_WITH_DECODE_RETAIN_MISSING_VALUE}
import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core._
import com.yahoo.maha.core.ddl.HiveDDLAnnotation
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.registry.RegistryBuilder
import com.yahoo.maha.core.request.{AsyncRequest, SyncRequest}

/**
 * Created by hiral on 1/15/16.
 */
trait SharedDimSchema {
  
  this : BaseQueryGeneratorTest =>

  CoreSchema.register()

  def keyword_dim: PublicDimension = {
    val builder : DimensionBuilder = {
      import HiveExpression._
      import com.yahoo.maha.core.BaseExpressionTest._
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        Dimension.newDimension(
          "cache_targeting_attribute", HiveEngine, LevelFive, Set(AdvertiserSchema, AdvertiserLowLatencySchema, ResellerSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("parent_type", StrType())
            , DimCol("parent_id", IntType(), annotations = Set(ForeignKey("ad_group")))
            , DimCol("value", StrType(255))
            , DimCol("status", StrType(255))
            , DimCol("match_type", StrType(64))
            , DimCol("ad_param_value_1", StrType(2048), annotations = Set(EscapingRequired))
            , DimCol("ad_param_value_2", StrType(200), annotations = Set(EscapingRequired))
            , DimCol("ad_param_value_3", StrType(200), annotations = Set(EscapingRequired))
            , DimCol("editorial_results", StrType(256), annotations = Set(EscapingRequired))
            , DimCol("cpc", DecType())
            , DimCol("device_id", IntType(3, (Map(1 -> "Desktop", 2 -> "Tablet", 3 -> "SmartPhone", -1 -> "UNKNOWN"), "UNKNOWN")))
            , DimCol("landing_url", StrType())
            , DimCol("deleted_date", IntType())
            , DimCol("modifier", DecType())
            , DimCol("hidden", IntType())
            , DimCol("created_by_user", StrType())
            , DimCol("created_date", IntType())
            , DimCol("last_updated_by_user", StrType())
            , DimCol("last_updated", IntType())
            , HiveDerDimCol("Keyword Date Created", StrType(), TIMESTAMP_TO_FORMATTED_DATE("{created_date}", "YYYY-MM-dd"), annotations = Set.empty)
            , HiveDerDimCol("Keyword Date Modified", StrType(), TIMESTAMP_TO_FORMATTED_DATE("{last_updated}", "YYYY-MM-dd"), annotations = Set.empty)
            , HiveDimCol("snapshot_ts", IntType(10), annotations = Set(HiveSnapshotTimestamp))
            , HiveDimCol("shard_id", IntType(3))
            , HivePartDimCol("load_time", StrType(), annotations = Set.empty)
            , HivePartDimCol("shard", StrType(10, default="all"), annotations = Set.empty)
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        )
      }
    }

    {
      import OracleExpression._
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        builder.withAlternateEngine(
          "targetingattribute",
          "cache_targeting_attribute",
          OracleEngine,
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , OraclePartDimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("parent_type", StrType(64))
            , DimCol("parent_id", IntType(), annotations = Set(ForeignKey("ad_group")))
            , DimCol("value", StrType(255))
            , DimCol("status", StrType(255))
            , DimCol("match_type", StrType(64))
            , DimCol("ad_param_value_1", StrType(2048), annotations = Set(EscapingRequired))
            , DimCol("ad_param_value_2", StrType(200), annotations = Set(EscapingRequired))
            , DimCol("ad_param_value_3", StrType(200), annotations = Set(EscapingRequired))
            , DimCol("editorial_results", StrType(256), annotations = Set(EscapingRequired))
            , DimCol("cpc", DecType())
            , DimCol("device_id", IntType(3, (Map(1 -> "Desktop", 2 -> "Tablet", 3 -> "SmartPhone", -1 -> "UNKNOWN"), "UNKNOWN")))
            , DimCol("landing_url", StrType(2048))
            , DimCol("deleted_date", TimestampType())
            , DimCol("modifier", DecType())
            , DimCol("hidden", IntType())
            , DimCol("created_by_user", StrType(255))
            , DimCol("created_date", TimestampType())
            , DimCol("last_updated_by_user", StrType(255))
            , DimCol("last_updated", TimestampType())
            , OracleDerDimCol("Keyword Date Created", StrType(), FORMAT_DATE("{created_date}", "YYYY-MM-DD"), annotations = Set.empty)
            , OracleDerDimCol("Keyword Date Modified", StrType(),FORMAT_DATE("{last_updated}", "YYYY-MM-DD"), annotations = Set.empty)
          )
          , None
          , annotations = Set(OracleAdvertiserHashPartitioning,PKCompositeIndex("AD_ID"))
        )
      }
    }

    builder.toPublicDimension(
      "keyword","keyword",
      Set(
        PubCol("advertiser_id", "Advertiser ID", InEquality)
        , PubCol("id", "Keyword ID", InEquality)
        , PubCol("value", "Keyword Value", InEquality)
        , PubCol("match_type", "Keyword Match Type Full", InEquality)
        , PubCol("match_type", "Keyword Match Type", InEquality)
        , PubCol("ad_param_value_1", "Keyword Param 1", InEquality)
        , PubCol("ad_param_value_2", "Keyword Param 2", InEquality)
        , PubCol("ad_param_value_3", "Keyword Param 3", InEquality)
        , PubCol("status", "Keyword Status Full", InEquality)
        , PubCol("status", "Keyword Status", InEquality)
        , PubCol("landing_url", "Keyword Landing URL", InEquality)
        , PubCol("parent_type", "Parent Type", InEquality)
        , PubCol("parent_id", "Ad Group ID", InEquality)
        , PubCol("device_id", "Device ID", InEquality)
        , PubCol("cpc", "CPC", InEquality)
        , PubCol("Keyword Date Created", "Keyword Date Created", InBetweenEquality)
        , PubCol("Keyword Date Modified", "Keyword Date Modified", InBetweenEquality)
      ), highCardinalityFilters = Set(NotInFilter("Keyword Status", List("DELETED")), EqualityFilter("Keyword Status", "ON"))
    )
  }

  def ad_dim: PublicDimension = {
    val builder : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        import OracleExpression._
        Dimension.newDimension("ad_dim_oracle", OracleEngine, LevelFour, Set(AdvertiserSchema, ResellerSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("device_id", IntType(3, (Map(1 -> "Desktop", 2 -> "Tablet", 3 -> "SmartPhone", -1 -> "UNKNOWN"), "UNKNOWN")))
            , DimCol("title", StrType(), annotations = Set(EscapingRequired, CaseInsensitive))
            , OraclePartDimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
            , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
            , DimCol("status", StrType())
            , OracleDerDimCol("Ad Status", StrType(), DECODE_DIM("{status}", "'ON'", "'ON'", "'OFF'"))
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , annotations = Set(OracleAdvertiserHashPartitioning,PKCompositeIndex("AD_ID"))
        )
      }
    }
    {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        import HiveExpression._
        import com.yahoo.maha.core.BaseExpressionTest._
        builder
          .withAlternateEngine("ad_dim_hive", "ad_dim_oracle", HiveEngine,
              Set(
                DimCol("id", IntType(), annotations = Set(PrimaryKey))
                , DimCol("title", StrType(), annotations = Set(EscapingRequired, CaseInsensitive))
                , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
                , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
                , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
                , DimCol("status", StrType())
                , HiveDerDimCol("Ad Status", StrType(), DECODE_DIM("{status}", "'ON'", "'ON'", "'OFF'"))
                , HivePartDimCol("load_time", StrType())
                , HivePartDimCol("shard", StrType(10, default="all"))
              )
          )
      }
    }
    builder
      .toPublicDimension("ad","ad",
        Set(
          PubCol("id", "Ad ID", InEquality)
          , PubCol("title", "Ad Title", InEqualityLike)
          , PubCol("advertiser_id", "Advertiser ID", InEquality)
          , PubCol("campaign_id", "Campaign ID", InEquality)
          , PubCol("ad_group_id", "Ad Group ID", InEquality)
          , PubCol("Ad Status", "Ad Status", InEquality)
        ), highCardinalityFilters = Set(NotInFilter("Ad Status", List("DELETED")))
      )
  }

  def ad_group_dim: PublicDimension = {
    val builder : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        import OracleExpression._
        Dimension.newDimension("ad_group_oracle", OracleEngine, LevelThree, Set(AdvertiserSchema, ResellerSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("name", StrType(), annotations = Set(EscapingRequired, CaseInsensitive))
            , DimCol("device_id", IntType(3, (Map(1 -> "Desktop", 2 -> "Tablet", 3 -> "SmartPhone", -1 -> "UNKNOWN"), "UNKNOWN")))
            , OraclePartDimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
            , DimCol("status", StrType())
            , DimCol("column2_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned_with_singleton")))
            , OracleDerDimCol("Ad Group Status", StrType(), DECODE_DIM("{status}", "'ON'", "'ON'", "'OFF'"))
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , annotations = Set(OracleAdvertiserHashPartitioning,PKCompositeIndex("AD_ID"))
        )
      }
    }
    
    {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        import HiveExpression._
        import com.yahoo.maha.core.BaseExpressionTest._
        builder.withAlternateEngine("ad_group_hive", "ad_group_oracle", HiveEngine,
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("name", StrType(), annotations = Set(EscapingRequired, CaseInsensitive))
            , DimCol("device_id", IntType(3, (Map(1 -> "Desktop", 2 -> "Tablet", 3 -> "SmartPhone", -1 -> "UNKNOWN"), "UNKNOWN")))
            , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
            , DimCol("status", StrType())
            , DimCol("column2_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned_with_singleton")))
            , HiveDerDimCol("Ad Group Status", StrType(), DECODE_DIM("{status}", "'ON'", "'ON'", "'OFF'"))
            , HivePartDimCol("load_time", StrType())
            , HivePartDimCol("shard", StrType(10, default="all"))
          )
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        import PrestoExpression._
        import com.yahoo.maha.core.BasePrestoExpressionTest._
        builder.withAlternateEngine("ad_group_presto", "ad_group_hive", PrestoEngine,
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("name", StrType(), annotations = Set(EscapingRequired, CaseInsensitive))
            , DimCol("device_id", IntType(3, (Map(1 -> "Desktop", 2 -> "Tablet", 3 -> "SmartPhone", -1 -> "UNKNOWN"), "UNKNOWN")))
            , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
            , DimCol("status", StrType())
            , DimCol("column2_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned_with_singleton")))
            , PrestoDerDimCol("Ad Group Status", StrType(), DECODE_DIM("{status}", "'ON'", "'ON'", "'OFF'"))
            , PrestoPartDimCol("load_time", StrType())
            , PrestoPartDimCol("shard", StrType(10, default="all"))
          )
        )
      }
    }
      
    builder
      .toPublicDimension("ad_group","ad_group",
        Set(
          PubCol("id", "Ad Group ID", InEquality)
          , PubCol("name","Ad Group Name", InEqualityLike)
          , PubCol("advertiser_id", "Advertiser ID", InEquality)
          , PubCol("campaign_id", "Campaign ID", InEquality)
          , PubCol("Ad Group Status", "Ad Group Status", InEquality)
          , PubCol("column2_id", "Column2 ID", InEquality)
        ), highCardinalityFilters = Set(NotInFilter("Ad Group Status", List("DELETED")), InFilter("Ad Group Status", List("ON")), EqualityFilter("Ad Group Status", "ON"))
      )
  }

  def campaign_dim: PublicDimension = {
    val builder : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        import OracleExpression._
        Dimension.newDimension("campaign_oracle", OracleEngine, LevelTwo, Set(AdvertiserSchema, ResellerSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , OraclePartDimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("device_id", IntType(3, (Map(1 -> "Desktop", 2 -> "Tablet", 3 -> "SmartPhone", -1 -> "UNKNOWN"), "UNKNOWN")))
            , DimCol("campaign_name", StrType(), annotations = Set(EscapingRequired, CaseInsensitive))
            , DimCol("status", StrType())
            , OracleDerDimCol("Campaign Status", StrType(), DECODE_DIM("{status}", "'ON'", "'ON'", "'OFF'"))
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , annotations = Set(OracleAdvertiserHashPartitioning, DimensionOracleStaticHint("CampaignHint"),PKCompositeIndex("AD_ID"))
        )
      }
    }
    
    {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        import HiveExpression._
        import com.yahoo.maha.core.BaseExpressionTest._
        builder.withAlternateEngine("campaing_hive", "campaign_oracle", HiveEngine,
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("campaign_name", StrType(), annotations = Set(EscapingRequired, CaseInsensitive))
            , DimCol("status", StrType())
            , HiveDerDimCol("Campaign Status", StrType(), DECODE_DIM("{status}", "'ON'", "'ON'", "'OFF'"))
            , HivePartDimCol("load_time", StrType())
            , HivePartDimCol("shard", StrType(10, default="all"))
          )
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        import PrestoExpression._
        import com.yahoo.maha.core.BasePrestoExpressionTest._
        builder.withAlternateEngine("campaign_presto", "campaing_hive", PrestoEngine,
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DimCol("campaign_name", StrType(), annotations = Set(EscapingRequired, CaseInsensitive))
            , DimCol("status", StrType())
            , PrestoDerDimCol("Campaign Status", StrType(), DECODE_DIM("{status}", "'ON'", "'ON'", "'OFF'"))
            , PrestoPartDimCol("load_time", StrType())
            , PrestoPartDimCol("shard", StrType(10, default="all"))
          ), None, Set.empty, None, Set.empty, Some("campaign_presto_underlying")
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        builder.withAlternateEngine("campaign_druid", "campaign_oracle", DruidEngine,
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
            , DruidFuncDimCol("campaign_name", StrType(), LOOKUP("campaign_lookup", "name"))
          )
          , Option(Map(AsyncRequest -> 14, SyncRequest -> 14))
        )
      }
    }
    
    builder
      .toPublicDimension("campaign","campaign",
        Set(
          PubCol("id", "Campaign ID", InEquality)
          , PubCol("advertiser_id", "Advertiser ID", InEquality)
          , PubCol("campaign_name", "Campaign Name", InEqualityLike)
          , PubCol("Campaign Status", "Campaign Status", InNotInEquality)
          , PubCol("device_id", "Campaign Device ID", InEquality)
        ), highCardinalityFilters = Set(NotInFilter("Campaign Status", List("DELETED")))
      )
  }

  def advertiser_dim: PublicDimension = {
    val builder : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        import OracleExpression._
        Dimension.newDimension("advertiser_oracle", OracleEngine, LevelOne, Set(AdvertiserSchema, ResellerSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("name", StrType())
            , DimCol("status", StrType())
            , DimCol("managed_by", IntType())
            , DimCol("currency", StrType())
            , DimCol("device_id", IntType(3, (Map(1 -> "Desktop", 2 -> "Tablet", 3 -> "SmartPhone", -1 -> "UNKNOWN"), "UNKNOWN")))
            , OracleDerDimCol("Advertiser Status", StrType(), DECODE_DIM("{status}", "'ON'", "'ON'", "'OFF'"))
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , schemaColMap = Map(AdvertiserSchema -> "id", ResellerSchema -> "managed_by")
          , annotations = Set(OracleAdvertiserHashPartitioning,PKCompositeIndex("AD_ID"))
        )
      }
    }
    
    {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        import HiveExpression._
        import com.yahoo.maha.core.BaseExpressionTest._
        builder.withAlternateEngine("advertiser_hive", "advertiser_oracle", HiveEngine,
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("name", StrType())
            , DimCol("status", StrType())
            , DimCol("managed_by", IntType())
            , DimCol("currency", StrType())
            , HiveDerDimCol("Advertiser Status", StrType(), DECODE_DIM("{status}", "'ON'", "'ON'", "'OFF'"))
            , HivePartDimCol("load_time", StrType())
            , HivePartDimCol("shard", StrType(10, default="all"))
          )
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        import PrestoExpression._
        import com.yahoo.maha.core.BasePrestoExpressionTest._
        builder.withAlternateEngine("advertiser_presto", "advertiser_hive", PrestoEngine,
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("name", StrType())
            , DimCol("status", StrType())
            , DimCol("managed_by", IntType())
            , PrestoDerDimCol("Advertiser Status", StrType(), DECODE_DIM("{status}", "'ON'", "'ON'", "'OFF'"))
            , PrestoPartDimCol("load_time", StrType())
            , PrestoPartDimCol("shard", StrType(10, default="all"))
          )
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        builder.withAlternateEngine (
          "advertiser_druid",
          "advertiser_oracle",
          DruidEngine,
          Set(
            DimCol("id", IntType(10), annotations = Set(PrimaryKey)),
            DruidFuncDimCol("name", StrType(), LOOKUP("advertiser_lookup", "name")),
            DruidFuncDimCol("Advertiser Status", StrType(), LOOKUP_WITH_DECODE("advertiser_lookup", "status", "ON", "ON", "OFF")),
            DruidFuncDimCol("managed_by", StrType(), LOOKUP("advertiser_lookup", "managed_by"))
          )
          , Option(Map(AsyncRequest -> 14, SyncRequest -> 14))
        )
      }
    }
    
    builder
      .toPublicDimension("advertiser","advertiser",
        Set(
          PubCol("id", "Advertiser ID", InEquality)
          , PubCol("managed_by", "Reseller ID", InEquality)
          , PubCol("name", "Advertiser Name", Equality)
          , PubCol("Advertiser Status", "Advertiser Status", InEquality)
          , PubCol("currency", "Advertiser Currency", InEquality)
          , PubCol("device_id", "Advertiser Device ID", InEquality)
        ), highCardinalityFilters = Set(NotInFilter("Advertiser Status", List("DELETED")))
      )
  }

  def advertiser_dim_v2: PublicDimension = {
    val builder : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        import OracleExpression._
        Dimension.newDimension("advertiser_oracle", OracleEngine, LevelOne, Set(AdvertiserSchema, ResellerSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("name", StrType())
            , DimCol("status", StrType())
            , DimCol("currency", StrType())
            , DimCol("managed_by", IntType())
            , DimCol("timezone", StrType())
            , OracleDerDimCol("Advertiser Status", StrType(), DECODE_DIM("{status}", "'ON'", "'ON'", "'OFF'"))
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , schemaColMap = Map(AdvertiserSchema -> "id", ResellerSchema -> "managed_by")
          , annotations = Set(OracleAdvertiserHashPartitioning,PKCompositeIndex("AD_ID"))
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        import HiveExpression._
        import com.yahoo.maha.core.BaseExpressionTest._
        builder.withAlternateEngine("advertiser_hive", "advertiser_oracle", HiveEngine,
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("name", StrType())
            , DimCol("status", StrType())
            , DimCol("managed_by", IntType())
            , DimCol("timezone", StrType())
            , HiveDerDimCol("Advertiser Status", StrType(), DECODE_DIM("{status}", "'ON'", "'ON'", "'OFF'"))
            , HivePartDimCol("load_time", StrType())
            , HivePartDimCol("shard", StrType(10, default="all"))
          )
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        builder.withAlternateEngine (
          "advertiser_druid",
          "advertiser_oracle",
          DruidEngine,
          Set(
            DimCol("id", IntType(10), annotations = Set(PrimaryKey)),
            DruidFuncDimCol("Advertiser Status", StrType(), LOOKUP("advertiser_lookup", "status")),
            DruidFuncDimCol("currency", StrType(), LOOKUP("advertiser_lookup", "currency")),
            DruidFuncDimCol("managed_by", StrType(), LOOKUP("advertiser_lookup", "managed_by")),
            DruidFuncDimCol("timezone", StrType(), LOOKUP_WITH_DECODE_ON_OTHER_COLUMN("advertiser_lookup", "timezone", "US", "timezone", "currency"))
          )
          , Option(Map(AsyncRequest -> 14, SyncRequest -> 14))
        )
      }
    }

    builder
      .toPublicDimension("advertiser","advertiser",
        Set(
          PubCol("id", "Advertiser ID", InEquality)
          , PubCol("managed_by", "Reseller ID", InEquality)
          , PubCol("currency", "Currency", Equality)
          , PubCol("timezone", "Timezone", Equality)
          , PubCol("Advertiser Status", "Advertiser Status", InEquality)
        ), revision = 2, highCardinalityFilters = Set(NotInFilter("Advertiser Status", List("DELETED")))
      )
  }

  def non_hash_partitioned_dim: PublicDimension = {
    ColumnContext.withColumnContext { implicit  cc: ColumnContext =>
      Dimension.newDimension("non_hash_paritioned_dim", OracleEngine, LevelTwo, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("name", StrType())
          , DimCol("status", StrType())
        )
        , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
      ).toPublicDimension("non_hash_partitioned","non_hash_partitioned",
          Set(
            PubCol("id", "Column ID", InEquality)
            , PubCol("name", "Column Name", Equality)
            , PubCol("status", "Column Status", InEquality)
          ), highCardinalityFilters = Set(NotInFilter("Column Status", List("DELETED")))
        )
    }
  }

  def non_hash_partitioned_with_singleton_dim: PublicDimension = {
    ColumnContext.withColumnContext { implicit  cc: ColumnContext =>
      Dimension.newDimension("non_hash_paritioned_with_singleton_dim", OracleEngine, LevelTwo, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("name", StrType(), annotations = Set(OracleSnapshotTimestamp))
          , DimCol("status", StrType())
        )
        , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        , annotations = Set(DimensionOracleStaticHint("HintHintHint"))
      ).toPublicDimension("non_hash_partitioned_with_singleton","non_hash_partitioned_with_singleton",
          Set(
            PubCol("id", "Column2 ID", InEquality)
            , PubCol("name", "Column2 Name", Equality)
            , PubCol("status", "Column2 Status", InEquality)
          ), highCardinalityFilters = Set(NotInFilter("Column2 Status", List("DELETED")))
        )
    }
  }

  def woeidDruidDim : PublicDimension = {
    val builder : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        Dimension.newDimension(
          "woeid_mapping", DruidEngine, LevelOne,
          Set(AdvertiserSchema),
          Set(
            DimCol("country_woeid", IntType(10), annotations = Set(PrimaryKey)),
            DruidFuncDimCol("value", StrType(), LOOKUP("woeid_lookup", "value"))
          )
          , Option(Map(AsyncRequest -> 14, SyncRequest -> 14))
        )
      }
    }

    builder.toPublicDimension(
      "woeid",
      "woeid",
      Set(
        PubCol("country_woeid", "Country WOEID", InEquality, hiddenFromJson = true),
        PubCol("value", "Country Name", InEqualityLike, hiddenFromJson = true)
      ), Set.empty
    )
  }

  def section_dim: PublicDimension = {

    val builder : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        Dimension.newDimension(
          "dim_section_complete", HiveEngine, LevelThree,
          Set(PublisherSchema, PublisherLowLatencySchema, InternalSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey)),
            DimCol("snapshot_ts", IntType(10)),
            DimCol("publisher_id", IntType(), annotations = Set(ForeignKey("publishers"))),
            DimCol("name", StrType(),annotations = Set(EscapingRequired), alias = Option("section_name")),
            DimCol("status", StrType(255, (Map("ON" -> "ON"),"OFF"))),
            DimCol("vertical", StrType(255)),
            DimCol("site_id", IntType(), annotations = Set(ForeignKey("sites"))),
            DimCol("source_tag", StrType(),annotations = Set(EscapingRequired)),
            DimCol("created_ts", IntType()),
            DimCol("last_update_ts", IntType()),
            DimCol("rtb_enabled", IntType()),
            DimCol("auction_type", StrType(),annotations = Set(EscapingRequired)),
            DimCol("rtb_section_group", StrType(),annotations = Set(EscapingRequired)),
            DimCol("rtb_section_group_mobile", StrType(),annotations = Set(EscapingRequired)),
            DimCol("rtb_section_group_tablet", StrType(),annotations = Set(EscapingRequired)),
            HivePartDimCol("load_time", StrType(), partitionLevel = FirstPartitionLevel),
            HivePartDimCol("shard", StrType(10, default="all"), partitionLevel = SecondPartitionLevel)
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , ddlAnnotation = Option(
            HiveDDLAnnotation(Map(),
              columnOrdering =
                IndexedSeq(
                  "id",
                  "name",
                  "status",
                  "vertical",
                  "publisher_id",
                  "site_id",
                  "source_tag",
                  "created_ts",
                  "last_update_ts",
                  "snapshot_ts",
                  "rtb_enabled",
                  "auction_type",
                  "rtb_section_group",
                  "rtb_section_group_mobile",
                  "rtb_section_group_tablet"
                )))
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        builder.withAlternateEngine (
          "section",
          "dim_section_complete",
          OracleEngine,
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey)),
            DimCol("snapshot_ts", IntType(10)),
            OraclePartDimCol("publisher_id", IntType(), annotations = Set(ForeignKey("publishers"))),
            DimCol("name", StrType(1000),annotations = Set(EscapingRequired)),
            DimCol("vertical", StrType(1000),annotations = Set(EscapingRequired)),
            DimCol("status", StrType(255, (Map("ON" -> "ON", "OFF" -> "OFF"),"NONE"))),
            DimCol("site_id", IntType(), annotations = Set(ForeignKey("sites"))),
            DimCol("source_tag", StrType(),annotations = Set(EscapingRequired)),
            DimCol("created_ts", IntType()),
            DimCol("last_update_ts", IntType()),
            DimCol("rtb_enabled", IntType()),
            DimCol("auction_type", StrType(),annotations = Set(EscapingRequired)),
            DimCol("rtb_section_group", StrType(),annotations = Set(EscapingRequired)),
            DimCol("rtb_section_group_mobile", StrType(),annotations = Set(EscapingRequired)),
            DimCol("rtb_section_group_tablet", StrType(),annotations = Set(EscapingRequired))
            // DimCol("partition_id", IntType())  // Oracle Partition Derived ID Column
            // annotations('oracle.partitionColumn','oracle.virtual.column','derivedExpression': "partitionIdFor(:snapshot_ts)
          )
          , None
          , annotations = Set(OracleAdvertiserHashPartitioning)
        )
      }
    }

    builder.toPublicDimension(
        "sections",
        "section",
        Set(
          PubCol("id", "Section ID", InNotInEquality),
          PubCol("site_id", "Site ID", InNotInEquality),
          PubCol("publisher_id", "Publisher ID", InNotInEquality),
          PubCol("name", "Section Name", InNotInEqualityLike),
          PubCol("status", "Section Status", InNotInEquality),
          PubCol("vertical", "Section Vertical", InNotInEquality)
        )
      )

  }

  def publisher_dim: PublicDimension = {

    val builder : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        Dimension.newDimension(
          "dim_publisher_complete", HiveEngine, LevelOne,
          Set(PublisherSchema, PublisherLowLatencySchema, InternalSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey)),
            DimCol("snapshot_ts", IntType(10)),
            DimCol("name", StrType(1000), alias = Option("publisher_name")),
            DimCol("status", StrType(255, (Map("ON" -> "ON"),"OFF"))),
            DimCol("timezone", StrType()),
            DimCol("created_ts", IntType()),
            DimCol("last_update_ts", IntType()),
            DimCol("rev_share", DecType()),
            DimCol("source_type", StrType()),
            HivePartDimCol("load_time", StrType(), partitionLevel = FirstPartitionLevel),
            HivePartDimCol("shard", StrType(10, default="all"), partitionLevel = SecondPartitionLevel)
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , schemaColMap = Map(
            PublisherSchema -> "id",
            PublisherLowLatencySchema -> "id")
          , ddlAnnotation = Option(
            HiveDDLAnnotation(Map(),
              columnOrdering =
                IndexedSeq(
                  "id",
                  "name",
                  "status",
                  "timezone",
                  "created_ts",
                  "last_update_ts",
                  "rev_share",
                  "source_type",
                  "snapshot_ts"
                )))
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        builder.withAlternateEngine (
          "publisher",
          "dim_publisher_complete",
          OracleEngine,
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey)),
            DimCol("snapshot_ts", IntType(10)),
            DimCol("name", StrType(1000)),
            DimCol("status", StrType(255, (Map("ON" -> "ON", "OFF" -> "OFF"),"NONE"))),
            DimCol("timezone", StrType()),
            DimCol("created_ts", IntType()),
            DimCol("last_update_ts", IntType()),
            DimCol("rev_share", DecType(12,8)),
            DimCol("source_type", StrType())
            //DimCol("partition_id", IntType())  // Oracle Partition Derived ID Column
            // annotations('oracle.partitionColumn','oracle.virtual.column','derivedExpression': "partitionIdFor(:snapshot_ts)
          )
          , None
          , annotations = Set(OracleAdvertiserHashPartitioning)
        )
      }
    }

    builder.toPublicDimension(
        "publishers",
        "publisher",
        Set(
          PubCol("id", "Publisher ID", InNotInEquality),
          PubCol("name", "Publisher Name", InNotInEqualityLike),
          PubCol("status", "Publisher Status", InNotInEquality),
          PubCol("timezone", "Publisher Timezone", InNotInEquality),
          PubCol("source_type", "Supply Type", InNotInEquality)
        )
      )
  }


  def site_dim: PublicDimension = {

    val builder : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        Dimension.newDimension(
          "dim_site_complete", HiveEngine, LevelTwo,
          Set(PublisherSchema, PublisherLowLatencySchema, InternalSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey)),
            DimCol("snapshot_ts", IntType(10)),
            DimCol("name", StrType(255), annotations = Set(EscapingRequired), alias = Option("site_name")),
            DimCol("status", StrType(255, (Map("ON" -> "ON"),"OFF"))),
            DimCol("publisher_id", IntType(), annotations = Set(ForeignKey("publishers"))),
            DimCol("rmx_section_id", IntType()),
            DimCol("created_ts", IntType()),
            DimCol("last_update_ts", IntType()),
            DimCol("platform", StrType(), annotations = Set(EscapingRequired)),
            HivePartDimCol("load_time", StrType(), partitionLevel = FirstPartitionLevel),
            HivePartDimCol("shard", StrType(10, default="all"), partitionLevel = SecondPartitionLevel)
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , ddlAnnotation = Option(
            HiveDDLAnnotation(Map(),
              columnOrdering =
                IndexedSeq(
                  "id",
                  "publisher_id",
                  "name",
                  "status",
                  "rmx_section_id",
                  "created_ts",
                  "last_update_ts",
                  "platform",
                  "snapshot_ts"
                )))
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        builder.withAlternateEngine (
          "site",
          "dim_site_complete",
          OracleEngine,
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey)),
            DimCol("snapshot_ts", IntType(10)),
            DimCol("name", StrType(1000), annotations = Set(EscapingRequired)),
            DimCol("status", StrType(255, (Map("ON" -> "ON"),"OFF"))),
            OraclePartDimCol("publisher_id", IntType(), annotations = Set(ForeignKey("publishers"))),
            DimCol("rmx_section_id", IntType()),
            DimCol("created_ts", IntType()),
            DimCol("last_update_ts", IntType()),
            DimCol("platform", StrType(), annotations = Set(EscapingRequired))
            // DimCol("partition_id", IntType())  // Oracle Partition Derived ID Column
            // annotations('oracle.partitionColumn','oracle.virtual.column','derivedExpression': "partitionIdFor(:snapshot_ts)
          )
          , None
          , annotations = Set(OracleAdvertiserHashPartitioning)
        )
      }
    }


    builder.toPublicDimension(
        "sites",
        "site",
        Set(
          PubCol("id", "Site ID", InNotInEquality),
          PubCol("name", "Site Name", InNotInEquality),
          PubCol("publisher_id", "Publisher ID", InNotInEquality),
          PubCol("status", "Site Status", InNotInEquality),
          PubCol("platform", "Platform", InNotInEquality)
        )
      )

  }

  def external_site_dim: PublicDimension = {

    val builder : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        Dimension.newDimension(
          "dim_site_complete", HiveEngine, LevelTwo,
          Set(AdvertiserSchema, InternalSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey), alias = Option("external_id"))
            , DimCol("external_site_name", StrType(60, default="Others"))
            , HivePartDimCol("load_time", StrType(), partitionLevel = FirstPartitionLevel)
            , HivePartDimCol("shard", StrType(10, default="all"), partitionLevel = SecondPartitionLevel)
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , ddlAnnotation = Option(
            HiveDDLAnnotation(Map(),
              columnOrdering =
                IndexedSeq(
                  "id",
                  "external_site_name"
                )))
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        builder.withAlternateEngine (
          "dr_site_performance_stats",
          "dim_site_complete",
          DruidEngine,
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DruidFuncDimCol("external_site_name", StrType(), LOOKUP_WITH_DECODE_RETAIN_MISSING_VALUE("site_lookup", "external_site_name", true, true, "null", "Others", "", "Others"))
          ), None
        )
      }
    }


    builder.toPublicDimension(
      "site_externals",
      "site_external",
      Set(
        PubCol("id", "External Site ID", InNotInEquality)
        , PubCol("external_site_name", "External Site Name", InBetweenEquality)
      )
    )
  }

  def restaurant_dim: PublicDimension = {
    val builder : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        Dimension.newDimension("restaurant_oracle", OracleEngine, LevelOne, Set(AdvertiserSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("address", StrType(1000))
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , annotations = Set(OracleAdvertiserHashPartitioning, PKCompositeIndex("AD_ID"))
        )
      }
    }

    builder
      .toPublicDimension("restaurant","restaurant",
        Set(
          PubCol("id", "Restaurant ID", InEquality)
          , PubCol("address", "Address", InEquality)
        )
      )
  }

  override protected[this] def registerDims(registryBuilder : RegistryBuilder): Unit = {
    registryBuilder.register(keyword_dim)
    registryBuilder.register(ad_dim)
    registryBuilder.register(advertiser_dim)
    registryBuilder.register(advertiser_dim_v2)
    registryBuilder.register(campaign_dim)
    registryBuilder.register(ad_group_dim)
    registryBuilder.register(non_hash_partitioned_dim)
    registryBuilder.register(non_hash_partitioned_with_singleton_dim)
    registryBuilder.register(woeidDruidDim)
    registryBuilder.register(site_dim)
    registryBuilder.register(external_site_dim)
    registryBuilder.register(section_dim)
    registryBuilder.register(publisher_dim)
    registryBuilder.register(restaurant_dim)
    registryBuilder.build()
  }
}
