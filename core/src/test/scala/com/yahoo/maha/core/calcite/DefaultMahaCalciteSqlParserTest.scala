package com.yahoo.maha.core.calcite

import com.yahoo.maha.core.CoreSchema.{AdvertiserSchema, InternalSchema}
import com.yahoo.maha.core.FilterOperation.{Equality, In, InEquality}
import com.yahoo.maha.core.NoopSchema.NoopSchema
import com.yahoo.maha.core.{ColumnContext, CoreSchema, DailyGrain, DecType, EscapingRequired, ForeignKey, Grain, HiveEngine, HourlyGrain, IntType, NotInFilter, PrimaryKey, StrType}
import com.yahoo.maha.core.dimension.{DimCol, Dimension, LevelOne, PubCol, PublicDimension}
import com.yahoo.maha.core.fact.{Fact, FactCol, PublicFact, PublicFactCol}
import com.yahoo.maha.core.registry.RegistryBuilder
import com.yahoo.maha.core.request.{AsyncRequest, RequestType, SyncRequest}
import org.scalatest.funsuite.AnyFunSuite

class DefaultMahaCalciteSqlParserTest extends AnyFunSuite {

  CoreSchema.register()

  def pubfact: PublicFact = {
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "fact", DailyGrain, HiveEngine, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("is_adjustment", StrType(1, (Map("Y" -> "Y", "N" -> "N"),"NONE")))
          , DimCol("aliased_dim", StrType())
        ),
        Set(
          FactCol("impressions", IntType())
          , FactCol("clicks", IntType())
          , FactCol("spend", DecType())
          , FactCol("aliased_met", IntType())
        )
      )
    }
      .newRollUp("intermediate_rollup", "fact", Set("is_adjustment"), schemas = Set(NoopSchema))
      .toPublicFact("publicFact",
        Set(
          PubCol("id", "Fact ID", Equality),
          PubCol("advertiser_id", "Advertiser ID", Equality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty),
          PubCol("is_adjustment", "Is Adjustment", Equality, incompatibleColumns = Set("Destination URL"), restrictedSchemas = Set(InternalSchema))
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InEquality),
          PublicFactCol("clicks", "Clicks", In, incompatibleColumns = Set("Pricing Type")),
          PublicFactCol("spend", "Spend", In, hiddenFromJson = true)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }
  protected[this] def getMaxDaysWindow: Map[(RequestType, Grain), Int] = {
    val result = 20
    Map(
      (SyncRequest, DailyGrain) -> result, (AsyncRequest, DailyGrain) -> result,
      (SyncRequest, HourlyGrain) -> result, (AsyncRequest, HourlyGrain) -> result
    )
  }

  protected[this] def getMaxDaysLookBack: Map[(RequestType, Grain), Int] = {
    val result = 30
    Map(
      (SyncRequest, DailyGrain) -> result, (AsyncRequest, DailyGrain) -> result
    )
  }

  val base_dim: PublicDimension = {
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Dimension.newDimension("base_dim", HiveEngine, LevelOne, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("status", StrType())
          , DimCol("email", StrType())
          , DimCol("ad_asset_json", StrType(), annotations = Set(EscapingRequired))
        )
        , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        , schemaColMap = Map(AdvertiserSchema -> "id")
      ).toPublicDimension("advertiser","advertiser",
        Set(
          PubCol("id", "Advertiser ID", Equality)
          , PubCol("status", "Advertiser Status", Equality)
          , PubCol("email", "Advertiser Email", Equality,  restrictedSchemas = Set(InternalSchema))
          , PubCol("ad_asset_json", "Ad Asset JSON", InEquality, hiddenFromJson = true)
        ), highCardinalityFilters = Set(NotInFilter("Advertiser Status", List("DELETED")))
      )
    }
  }


  val registry = new RegistryBuilder()
    .register(pubfact)
    .register(base_dim)
    .build();

  val defaultMahaCalciteSqlParser = DefaultMahaCalciteSqlParser(registry)

  test("Sql Parser test") {
    val sql = "select * from publicFact where 'Advertiser ID' = 123";
    val result = defaultMahaCalciteSqlParser.parse(sql)
  }



}
