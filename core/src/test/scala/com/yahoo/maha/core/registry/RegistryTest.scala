// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.registry

import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core.NoopSchema.NoopSchema
import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact.{Fact, FactCol, PublicFact, PublicFactCol, PublicFactTable}
import com.yahoo.maha.core.request.{AsyncRequest, RequestType, SyncRequest}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Created by jians on 10/21/15.
 */
class RegistryTest extends AnyFunSuite with Matchers {

  CoreSchema.register()

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

  def pubfact2: PublicFact = {
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "fact2", DailyGrain, HiveEngine, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("adgroup")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
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
      .toPublicFact("publicFact2",
        Set(
          PubCol("id", "Fact ID", Equality),
          PubCol("advertiser_id", "Advertiser ID", Equality),
          PubCol("ad_group_id", "Base Dim3 ID", Equality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InEquality),
          PublicFactCol("clicks", "Clicks", In),
          PublicFactCol("spend", "Spend", In, hiddenFromJson = true)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack, dimToRevisionMap = Map("advertiser" -> 0)
      )
  }

  def pubFact3: PublicFact = {
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "fact", DailyGrain, HiveEngine, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("is_adjustment", StrType(1, (Map("Y" -> "Y", "N" -> "N"),"NONE"))
          )
        ),
        Set(
          FactCol("impressions", IntType())
          , FactCol("clicks", IntType())
          , FactCol("spend", DecType())
        )
      )
    }
      .toPublicFact("publicFact",
        Set(
          PubCol("id", "Fact ID", Equality),
          PubCol("advertiser_id", "Advertiser ID", Equality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty),
          PubCol("is_adjustment", "Is Adjustment", Equality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InEquality),
          PublicFactCol("clicks", "Clicks", In),
          PublicFactCol("spend", "Spend", In, hiddenFromJson = true)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack, revision = 1, dimRevision = 1
      )
  }

  def pubFact4: PublicFact = {
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "fact", DailyGrain, HiveEngine, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("is_adjustment", StrType(1, (Map("Y" -> "Y", "N" -> "N"),"NONE"))
          )
        ),
        Set(
          FactCol("impressions", IntType())
          , FactCol("clicks", IntType())
          , FactCol("spend", DecType())
        )
      )
    }
      .toPublicFact("publicFact",
        Set(
          PubCol("id", "Fact ID", Equality),
          PubCol("advertiser_id", "Advertiser ID", Equality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty),
          PubCol("is_adjustment", "Is Adjustment", Equality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InEquality),
          PublicFactCol("clicks", "Clicks", In),
          PublicFactCol("spend", "Spend", In, hiddenFromJson = true)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack, dimRevision = 1
      )
  }

  def pubFact3WithSameRevision: PublicFact = {
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "fact", DailyGrain, HiveEngine, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("is_adjustment", StrType(1, (Map("Y" -> "Y", "N" -> "N"),"NONE"))
          )
        ),
        Set(
          FactCol("impressions", IntType())
          , FactCol("clicks", IntType())
          , FactCol("spend", DecType())
        )
      )
    }
      .toPublicFact("publicFact",
        Set(
          PubCol("id", "Fact ID", Equality),
          PubCol("advertiser_id", "Advertiser ID", Equality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty),
          PubCol("is_adjustment", "Is Adjustment", Equality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InEquality),
          PublicFactCol("clicks", "Clicks", In),
          PublicFactCol("spend", "Spend", In, hiddenFromJson = true)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack, revision = 1
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

  val base_dim_with_revision: PublicDimension = {
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Dimension.newDimension("base_dim", HiveEngine, LevelOne, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("status", StrType())
          , DimCol("ad_asset_json", StrType(), annotations = Set(EscapingRequired))
        )
        , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        , schemaColMap = Map(AdvertiserSchema -> "id")
      ).toPublicDimension("advertiser","advertiser",
          Set(
            PubCol("id", "Advertiser ID", Equality)
            , PubCol("status", "Advertiser Status", Equality)
            , PubCol("ad_asset_json", "Ad Asset JSON Copy", InEquality, hiddenFromJson = true)
          ), revision = 1, highCardinalityFilters = Set(NotInFilter("Advertiser Status", List("DELETED")))
        )
    }
  }

  val base_dim_with_same_revision: PublicDimension = {
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Dimension.newDimension("base_dim", HiveEngine, LevelOne, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("status", StrType())
          , DimCol("ad_asset_json", StrType(), annotations = Set(EscapingRequired))
        )
        , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        , schemaColMap = Map(AdvertiserSchema -> "id")
      ).toPublicDimension("advertiser","advertiser",
          Set(
            PubCol("id", "Advertiser ID", Equality)
            , PubCol("status", "Advertiser Status", Equality)
            , PubCol("ad_asset_json", "Ad Asset JSON", InEquality, hiddenFromJson = true)
          ), revision = 1, highCardinalityFilters = Set(NotInFilter("Advertiser Status", List("DELETED")))
        )
    }
  }

  val base_dim2_with_fk: PublicDimension = {
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Dimension.newDimension("base_dim2", HiveEngine, LevelOne, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("base_dim_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("status", StrType())
        )
        , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
      ).toPublicDimension("campaign","campaign",
          Set(
            PubCol("id", "Campaign ID", Equality)
            , PubCol("status", "Status", Equality)
          ), highCardinalityFilters = Set(NotInFilter("Status", List("DELETED")))
        )
    }
  }

  val base_dim2_with_fk_with_revision: PublicDimension = {
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Dimension.newDimension("base_dim2", HiveEngine, LevelOne, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("base_dim_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("status", StrType())
        )
        , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
      ).toPublicDimension("campaign","campaign",
          Set(
            PubCol("id", "Campaign ID", Equality)
            , PubCol("status", "Status", Equality)
          ), highCardinalityFilters = Set(NotInFilter("Status", List("DELETED")))
        )
    }
  }

  val base_dim3_with_duplicate_col: PublicDimension = {
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Dimension.newDimension("base_dim3", HiveEngine, LevelOne, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("base_dim_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("status", StrType())
        )
        , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
      ).toPublicDimension("adgroup","adgroup",
          Set(
            PubCol("id", "Base Dim3 ID", Equality)
            , PubCol("status", "Status", Equality)
          ), highCardinalityFilters = Set(NotInFilter("Status", List("DELETED")))
        )
    }
  }

  test("isCubeDefined should return true on valid registered cube") {
    val pubFact1 = pubfact
    val registryBuilder = new RegistryBuilder
    registryBuilder.register(pubFact1)
    registryBuilder.register(base_dim)
    registryBuilder.register(base_dim2_with_fk)
    val registry = registryBuilder.build()
    val allDomainColumnAliasToPublicColumnMap =  pubFact1.getAllDomainColumnAliasToPublicColumnMap(registry)
    assert(allDomainColumnAliasToPublicColumnMap.size == 12)
    assert(registry.isCubeDefined("publicFact"), "Cube is not registered!")
  }

  test("isCubeDefined with revision should return true on valid registered cube") {
    val pubFact1 = pubfact
    val registryBuilder = new RegistryBuilder
    registryBuilder.register(pubFact3)
    registryBuilder.register(base_dim_with_revision)
    val registry = registryBuilder.build()
    assert(registry.isCubeDefined("publicFact", Option.apply(1)), "Cube is not registered!")
  }

  test("isCubeDefined should return false on invalid cube") {
    val pubFact1 = pubfact
    val registryBuilder = new RegistryBuilder
    registryBuilder.register(pubFact3)
    registryBuilder.register(base_dim_with_revision)
    val registry = registryBuilder.build()
    assert(registry.isCubeDefined("blah") == false, "Cube is registered!")
  }

  test("build should fail on missing dimension table on foreign keys for fact") {
    val pubFact1 = pubfact
    val registryBuilder = new RegistryBuilder
    registryBuilder.register(pubFact1)
    val thrown = intercept[IllegalArgumentException] {
      registryBuilder.build()
    }
    thrown.getMessage should startWith ("requirement failed: Missing foreign key fact sources : List((publicFact,Set(advertiser)))")
  }

  test("build should fail on missing dimension table on foreign keys for dimension") {
    val registryBuilder = new RegistryBuilder
    registryBuilder.register(base_dim2_with_fk)
    val thrown = intercept[IllegalArgumentException] {
      registryBuilder.build()
    }
    thrown.getMessage should startWith ("requirement failed: Missing foreign key dimension sources")
  }

  test("getPrimaryKeyAlias should be able to return the primaryKey alias for specified attribute alias for dimensions") {
    val pubFact1 = pubfact
    val registryBuilder = new RegistryBuilder
    registryBuilder.register(pubFact1)
    registryBuilder.register(base_dim)
    registryBuilder.register(base_dim2_with_fk)
    val registry = registryBuilder.build()
    assert(registry.getPrimaryKeyAlias(pubFact1.name, None, "Advertiser Status").isDefined && registry.getPrimaryKeyAlias(pubFact1.name, None, "Advertiser Status").get == "Advertiser ID")
  }

  test("getPrimaryKeyAlias should be able to return the primaryKey alias for attribute alias which is duplicate for dimensions") {
    val pubFact1 = pubfact
    val pubFact2 = pubfact2
    val registryBuilder = new RegistryBuilder
    registryBuilder.register(pubFact1)
    registryBuilder.register(pubFact2)
    registryBuilder.register(base_dim)
    registryBuilder.register(base_dim2_with_fk)
    registryBuilder.register(base_dim3_with_duplicate_col)
    val registry = registryBuilder.build()
    assert(registry.getPrimaryKeyAlias(pubFact2.name, None, "Status").isDefined && registry.getPrimaryKeyAlias(pubFact2.name, None, "Status").get == "Base Dim3 ID")

  }

  test("domainJsonAsString should be able to return the correct json output") {
    val pubFact1 = pubfact
    val registryBuilder = new RegistryBuilder
    registryBuilder.register(pubFact1)
    registryBuilder.register(base_dim)
    registryBuilder.register(base_dim_with_revision)
    registryBuilder.register(pubFact3)
    val registry = registryBuilder.build()
    registry.domainJsonAsString shouldBe """{"dimensions":[{"name":"advertiser","fields":["Advertiser ID","Advertiser Status","Advertiser Email"],"fieldsWithSchemas":[{"name":"Advertiser ID","allowedSchemas":[]},{"name":"Advertiser Status","allowedSchemas":[]},{"name":"Advertiser Email","allowedSchemas":["internal"]}]}],"schemas":{"advertiser":["publicFact"]},"cubes":[{"name":"publicFact","mainEntityIds":{"advertiser":"Advertiser ID"},"maxDaysLookBack":[{"requestType":"SyncRequest","grain":"DailyGrain","days":30},{"requestType":"AsyncRequest","grain":"DailyGrain","days":30}],"maxDaysWindow":[{"requestType":"SyncRequest","grain":"DailyGrain","days":20},{"requestType":"AsyncRequest","grain":"DailyGrain","days":20},{"requestType":"SyncRequest","grain":"HourlyGrain","days":20},{"requestType":"AsyncRequest","grain":"HourlyGrain","days":20}],"fields":[{"field":"Advertiser ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"advertiser","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Destination URL","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":null,"filterable":false,"filterOperations":null,"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Fact ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Is Adjustment","type":"Dimension","dataType":{"type":"Enum","constraint":"N|Y"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":["Destination URL"],"isImageColumn":false,"allowedSchemas":["internal"]},{"field":"Pricing Type","type":"Dimension","dataType":{"type":"Enum","constraint":"CPA|CPC|CPCV|CPE|CPF|CPM|CPV"},"dimensionName":null,"filterable":true,"filterOperations":["IN"],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Source","type":"Dimension","dataType":{"type":"Number","constraint":"3"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Clicks","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN"],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":["Pricing Type"],"allowedSchemas":null},{"field":"Impressions","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null,"allowedSchemas":null}]}]}"""
  }

  test("versionedDomainJsonAsString should be able to return the correct json output") {
    val pubFact1 = pubfact
    val registryBuilder = new RegistryBuilder
    registryBuilder.register(pubFact1)
    registryBuilder.register(base_dim)
    registryBuilder.register(pubFact3)
    registryBuilder.register(base_dim_with_revision)
    val registry = registryBuilder.build()
    val versionedJson = registry.versionedDomainJsonAsString
    versionedJson shouldBe """{"dimensions":[{"name":"advertiser","fields":["Advertiser ID","Advertiser Status","Advertiser Email"],"fieldsWithSchemas":[{"name":"Advertiser ID","allowedSchemas":[]},{"name":"Advertiser Status","allowedSchemas":[]},{"name":"Advertiser Email","allowedSchemas":["internal"]}],"revision":0},{"name":"advertiser","fields":["Advertiser ID","Advertiser Status"],"fieldsWithSchemas":[{"name":"Advertiser ID","allowedSchemas":[]},{"name":"Advertiser Status","allowedSchemas":[]}],"revision":1}],"schemas":{"advertiser":["publicFact"]},"cubes":[{"name":"publicFact","mainEntityIds":{"advertiser":"Advertiser ID"},"maxDaysLookBack":[{"requestType":"SyncRequest","grain":"DailyGrain","days":30},{"requestType":"AsyncRequest","grain":"DailyGrain","days":30}],"maxDaysWindow":[{"requestType":"SyncRequest","grain":"DailyGrain","days":20},{"requestType":"AsyncRequest","grain":"DailyGrain","days":20},{"requestType":"SyncRequest","grain":"HourlyGrain","days":20},{"requestType":"AsyncRequest","grain":"HourlyGrain","days":20}],"fields":[{"field":"Advertiser ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"advertiser","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Destination URL","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":null,"filterable":false,"filterOperations":null,"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Fact ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Is Adjustment","type":"Dimension","dataType":{"type":"Enum","constraint":"N|Y"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":["Destination URL"],"isImageColumn":false,"allowedSchemas":["internal"]},{"field":"Pricing Type","type":"Dimension","dataType":{"type":"Enum","constraint":"CPA|CPC|CPCV|CPE|CPF|CPM|CPV"},"dimensionName":null,"filterable":true,"filterOperations":["IN"],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Source","type":"Dimension","dataType":{"type":"Number","constraint":"3"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Clicks","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN"],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":["Pricing Type"],"allowedSchemas":null},{"field":"Impressions","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null,"allowedSchemas":null}],"revision":0},{"name":"publicFact","mainEntityIds":{"advertiser":"Advertiser ID"},"maxDaysLookBack":[{"requestType":"SyncRequest","grain":"DailyGrain","days":30},{"requestType":"AsyncRequest","grain":"DailyGrain","days":30}],"maxDaysWindow":[{"requestType":"SyncRequest","grain":"DailyGrain","days":20},{"requestType":"AsyncRequest","grain":"DailyGrain","days":20},{"requestType":"SyncRequest","grain":"HourlyGrain","days":20},{"requestType":"AsyncRequest","grain":"HourlyGrain","days":20}],"fields":[{"field":"Advertiser ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"advertiser","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Destination URL","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":null,"filterable":false,"filterOperations":null,"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Fact ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Is Adjustment","type":"Dimension","dataType":{"type":"Enum","constraint":"N|Y"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Pricing Type","type":"Dimension","dataType":{"type":"Enum","constraint":"CPA|CPC|CPCV|CPE|CPF|CPM|CPV"},"dimensionName":null,"filterable":true,"filterOperations":["IN"],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Source","type":"Dimension","dataType":{"type":"Number","constraint":"3"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Clicks","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN"],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null,"allowedSchemas":null},{"field":"Impressions","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null,"allowedSchemas":null}],"revision":1}]}"""
  }

  test("getCubeJsonForName should be able to return the correct json for given cube name") {
    val pubFact1 = pubfact
    val registryBuilder = new RegistryBuilder
    registryBuilder.register(pubFact1)
    registryBuilder.register(base_dim)
    val registry = registryBuilder.build()
    registry.getCubeJsonAsStringForCube("publicFact") shouldBe """{"name":"publicFact","mainEntityIds":{"advertiser":"Advertiser ID"},"maxDaysLookBack":[{"requestType":"SyncRequest","grain":"DailyGrain","days":30},{"requestType":"AsyncRequest","grain":"DailyGrain","days":30}],"maxDaysWindow":[{"requestType":"SyncRequest","grain":"DailyGrain","days":20},{"requestType":"AsyncRequest","grain":"DailyGrain","days":20},{"requestType":"SyncRequest","grain":"HourlyGrain","days":20},{"requestType":"AsyncRequest","grain":"HourlyGrain","days":20}],"fields":[{"field":"Advertiser ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"advertiser","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Destination URL","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":null,"filterable":false,"filterOperations":null,"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Fact ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Is Adjustment","type":"Dimension","dataType":{"type":"Enum","constraint":"N|Y"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":["Destination URL"],"isImageColumn":false,"allowedSchemas":["internal"]},{"field":"Pricing Type","type":"Dimension","dataType":{"type":"Enum","constraint":"CPA|CPC|CPCV|CPE|CPF|CPM|CPV"},"dimensionName":null,"filterable":true,"filterOperations":["IN"],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Source","type":"Dimension","dataType":{"type":"Number","constraint":"3"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Clicks","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN"],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":["Pricing Type"],"allowedSchemas":null},{"field":"Impressions","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null,"allowedSchemas":null}]}"""
  }

  test("getCubeJsonForName should not contain hidden columns in json for given cube name") {
    val pubFact1 = pubfact
    val registryBuilder = new RegistryBuilder
    registryBuilder.register(pubFact1)
    registryBuilder.register(base_dim)
    val registry = registryBuilder.build()
    assert(registry.getFact("publicFact").get.columnsByAliasMap("Spend").hiddenFromJson, "Spend should be a hidden field")
    registry.getCubeJsonAsStringForCube("publicFact")  shouldNot contain("Spend")
  }

  test("Public Fact and Dimension with revision should work as expected") {
    val registryBuilder = new RegistryBuilder
    registryBuilder.register(pubFact3)
    registryBuilder.register(base_dim_with_revision)
    val registry = registryBuilder.build()
    assert(registry.getFact("publicFact", Option.apply(1)).isDefined, "publicFact with revision should exist")
    assert(registry.getDimension("advertiser", Option.apply(1)).isDefined, "publicDimension with revision should exist")
  }

  test("Public Fact with dimRevision of new Dimension revision should return new revision for dim") {
    val registryBuilder = new RegistryBuilder
    registryBuilder.register(pubFact4)
    registryBuilder.register(base_dim_with_revision)
    registryBuilder.register(base_dim)
    val registry = registryBuilder.build()
    assert(registry.getFact("publicFact").get.revision == 0, "publicFact with revision should exist")
    assert(registry.getDimension("advertiser", Option.apply(1)).get.revision == 1, "publicDimension with revision should exist")
  }

  test("Public Fact and Dimension with multiple revision should take lowest revision as default revision") {
    val registryBuilder = new RegistryBuilder
    registryBuilder.register(pubFact3)
    registryBuilder.register(pubfact)
    registryBuilder.register(base_dim_with_revision)
    registryBuilder.register(base_dim)
    val registry = registryBuilder.build()
    assert(registry.getFact("publicFact").get.revision == 0, "publicFact with revision should exist")
    assert(registry.getDimension("advertiser").get.revision == 0, "publicDimension with revision should exist")
  }

  test("Public Fact and Dimension with multiple revision should take the revision passed in defaultRevisionMap as default revision") {
    val registryBuilder = new RegistryBuilder
    registryBuilder.register(pubFact3)
    registryBuilder.register(pubfact)
    registryBuilder.register(base_dim_with_revision)
    registryBuilder.register(base_dim)
    val registry = registryBuilder.build(defaultPublicFactRevisionMap = Map("publicFact" -> 1), defaultPublicDimRevisionMap = Map("advertiser" -> 1))
    assert(registry.getFact("publicFact").get.revision == 1, "publicFact with revision should exist")
    assert(registry.getDimension("advertiser").get.revision == 1, "publicDimension with revision should exist")
  }

  test("Default Public Revision with a Fact which is not registered should fail") {
    val thrown = intercept[IllegalArgumentException] {
      val registryBuilder = new RegistryBuilder
      registryBuilder.register(pubFact3)
      registryBuilder.register(base_dim_with_revision)
      val registry = registryBuilder.build(defaultPublicFactRevisionMap = Map("publicFact2" -> 0))
    }
    thrown.getMessage should startWith ("requirement failed: Missing default public fact revision : Map(publicFact2 -> 0)")
  }

  test("Default Public Revision with a Dimension which is not registered should fail") {
    val thrown = intercept[IllegalArgumentException] {
      val registryBuilder = new RegistryBuilder
      registryBuilder.register(pubFact3)
      registryBuilder.register(base_dim_with_revision)
      val registry = registryBuilder.build(defaultPublicDimRevisionMap = Map("advertiser_invalid" -> 0))
    }
    thrown.getMessage should startWith ("requirement failed: Missing default public dim revision : Map(advertiser_invalid -> 0)")
  }

  test("getFact and getDimension with invalid revision should return Fact and Dimension associated with defaultRevision") {
    val registryBuilder = new RegistryBuilder
    registryBuilder.register(pubfact)
    registryBuilder.register(pubFact3)
    registryBuilder.register(base_dim)
    registryBuilder.register(base_dim_with_revision)
    val registry = registryBuilder.build()
    assert(registry.getFact("publicFact", Option.apply(2)).get.revision == 0, "publicFact with revision should exist")
    assert(registry.getDimension("advertiser", Option.apply(2)).get.revision == 0, "publicDimension with revision should exist")
  }

  test("Public Fact with same name and revision should fail") {
    val thrown = intercept[IllegalArgumentException] {
      val registryBuilder = new RegistryBuilder
      registryBuilder.register(pubFact3)
      registryBuilder.register(pubFact3WithSameRevision)
      registryBuilder.register(base_dim_with_revision)
      registryBuilder.build()
    }
    thrown.getMessage should startWith ("requirement failed: Cannot register multiple public facts with same name : publicFact and revision 1")
  }

  test("Public Dimension with same name and revision should fail") {
    val thrown = intercept[IllegalArgumentException] {
      val registryBuilder = new RegistryBuilder
      registryBuilder.register(base_dim_with_revision)
      registryBuilder.register(base_dim_with_same_revision)
      registryBuilder.build()
    }
    thrown.getMessage should startWith ("requirement failed: Cannot register multiple public dims with same name : advertiser and revision 1")
  }

  test("domainJsonAsString should include only default revision of the cube") {
    val registryBuilder = new RegistryBuilder
    registryBuilder.register(pubfact)
    registryBuilder.register(pubFact3)
    registryBuilder.register(base_dim)
    registryBuilder.register(base_dim_with_revision)
    val registry = registryBuilder.build()
    registry.domainJsonAsString shouldBe """{"dimensions":[{"name":"advertiser","fields":["Advertiser ID","Advertiser Status","Advertiser Email"],"fieldsWithSchemas":[{"name":"Advertiser ID","allowedSchemas":[]},{"name":"Advertiser Status","allowedSchemas":[]},{"name":"Advertiser Email","allowedSchemas":["internal"]}]}],"schemas":{"advertiser":["publicFact"]},"cubes":[{"name":"publicFact","mainEntityIds":{"advertiser":"Advertiser ID"},"maxDaysLookBack":[{"requestType":"SyncRequest","grain":"DailyGrain","days":30},{"requestType":"AsyncRequest","grain":"DailyGrain","days":30}],"maxDaysWindow":[{"requestType":"SyncRequest","grain":"DailyGrain","days":20},{"requestType":"AsyncRequest","grain":"DailyGrain","days":20},{"requestType":"SyncRequest","grain":"HourlyGrain","days":20},{"requestType":"AsyncRequest","grain":"HourlyGrain","days":20}],"fields":[{"field":"Advertiser ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"advertiser","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Destination URL","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":null,"filterable":false,"filterOperations":null,"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Fact ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Is Adjustment","type":"Dimension","dataType":{"type":"Enum","constraint":"N|Y"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":["Destination URL"],"isImageColumn":false,"allowedSchemas":["internal"]},{"field":"Pricing Type","type":"Dimension","dataType":{"type":"Enum","constraint":"CPA|CPC|CPCV|CPE|CPF|CPM|CPV"},"dimensionName":null,"filterable":true,"filterOperations":["IN"],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Source","type":"Dimension","dataType":{"type":"Number","constraint":"3"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Clicks","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN"],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":["Pricing Type"],"allowedSchemas":null},{"field":"Impressions","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null,"allowedSchemas":null}]}]}"""
  }

  test("flatten domainJsonAsString should return all possible fields in cube") {
    val registryBuilder = new RegistryBuilder
    registryBuilder.register(pubfact)
    registryBuilder.register(pubFact3)
    registryBuilder.register(base_dim)
    registryBuilder.register(base_dim_with_revision)
    val registry = registryBuilder.build()
    val expected = s"""{"name":"publicFact","mainEntityIds":{"advertiser":"Advertiser ID"},"maxDaysLookBack":[{"requestType":"SyncRequest","grain":"DailyGrain","days":30},{"requestType":"AsyncRequest","grain":"DailyGrain","days":30}],"maxDaysWindow":[{"requestType":"SyncRequest","grain":"DailyGrain","days":20},{"requestType":"AsyncRequest","grain":"DailyGrain","days":20},{"requestType":"SyncRequest","grain":"HourlyGrain","days":20},{"requestType":"AsyncRequest","grain":"HourlyGrain","days":20}],"fields":[{"field":"Advertiser ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"advertiser","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":null},{"field":"Destination URL","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":null,"filterable":false,"filterOperations":null,"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":null},{"field":"Fact ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":null},{"field":"Is Adjustment","type":"Dimension","dataType":{"type":"Enum","constraint":"N|Y"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":["internal"]},{"field":"Pricing Type","type":"Dimension","dataType":{"type":"Enum","constraint":"CPA|CPC|CPCV|CPE|CPF|CPM|CPV"},"dimensionName":null,"filterable":true,"filterOperations":["IN"],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":null},{"field":"Source","type":"Dimension","dataType":{"type":"Number","constraint":"3"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":null},{"field":"Clicks","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN"],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","allowedSchemas":null},{"field":"Impressions","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","allowedSchemas":null},{"field":"Advertiser Status","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":"advertiser","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":null},{"field":"Advertiser Email","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":"advertiser","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":["internal"]},{"field":"Ad Asset JSON","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":"advertiser","filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":null}]}"""
    registry.getFlattenCubeJsonAsStringForCube(pubfact.name) shouldBe expected
  }

  test("flatten domainJsonAsString should return correct version of cube") {
    val registryBuilder = new RegistryBuilder
    registryBuilder.register(pubfact)
    registryBuilder.register(pubFact3)
    registryBuilder.register(base_dim)
    registryBuilder.register(base_dim_with_revision)
    val registry = registryBuilder.build()

    val expected = s"""{"name":"publicFact","mainEntityIds":{"advertiser":"Advertiser ID"},"maxDaysLookBack":[{"requestType":"SyncRequest","grain":"DailyGrain","days":30},{"requestType":"AsyncRequest","grain":"DailyGrain","days":30}],"maxDaysWindow":[{"requestType":"SyncRequest","grain":"DailyGrain","days":20},{"requestType":"AsyncRequest","grain":"DailyGrain","days":20},{"requestType":"SyncRequest","grain":"HourlyGrain","days":20},{"requestType":"AsyncRequest","grain":"HourlyGrain","days":20}],"fields":[{"field":"Advertiser ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"advertiser","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":null},{"field":"Destination URL","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":null,"filterable":false,"filterOperations":null,"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":null},{"field":"Fact ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":null},{"field":"Is Adjustment","type":"Dimension","dataType":{"type":"Enum","constraint":"N|Y"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":null},{"field":"Pricing Type","type":"Dimension","dataType":{"type":"Enum","constraint":"CPA|CPC|CPCV|CPE|CPF|CPM|CPV"},"dimensionName":null,"filterable":true,"filterOperations":["IN"],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":null},{"field":"Source","type":"Dimension","dataType":{"type":"Number","constraint":"3"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":null},{"field":"Clicks","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN"],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","allowedSchemas":null},{"field":"Impressions","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","allowedSchemas":null},{"field":"Advertiser Status","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":"advertiser","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":null},{"field":"Ad Asset JSON Copy","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":"advertiser","filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":null}]}"""
    registry.getFlattenCubeJsonAsStringForCube(pubfact.name, 1) shouldBe expected
  }

  test("domainJsonAsString and flattenDomainJsonAsString should not include NoopSchema") {
    val pubFact1 = pubfact
    val registryBuilder = new RegistryBuilder
    registryBuilder.register(pubFact1)
    registryBuilder.register(base_dim)
    val registry = registryBuilder.build()

    registry.domainJsonAsString shouldBe """{"dimensions":[{"name":"advertiser","fields":["Advertiser ID","Advertiser Status","Advertiser Email"],"fieldsWithSchemas":[{"name":"Advertiser ID","allowedSchemas":[]},{"name":"Advertiser Status","allowedSchemas":[]},{"name":"Advertiser Email","allowedSchemas":["internal"]}]}],"schemas":{"advertiser":["publicFact"]},"cubes":[{"name":"publicFact","mainEntityIds":{"advertiser":"Advertiser ID"},"maxDaysLookBack":[{"requestType":"SyncRequest","grain":"DailyGrain","days":30},{"requestType":"AsyncRequest","grain":"DailyGrain","days":30}],"maxDaysWindow":[{"requestType":"SyncRequest","grain":"DailyGrain","days":20},{"requestType":"AsyncRequest","grain":"DailyGrain","days":20},{"requestType":"SyncRequest","grain":"HourlyGrain","days":20},{"requestType":"AsyncRequest","grain":"HourlyGrain","days":20}],"fields":[{"field":"Advertiser ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"advertiser","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Destination URL","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":null,"filterable":false,"filterOperations":null,"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Fact ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Is Adjustment","type":"Dimension","dataType":{"type":"Enum","constraint":"N|Y"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":["Destination URL"],"isImageColumn":false,"allowedSchemas":["internal"]},{"field":"Pricing Type","type":"Dimension","dataType":{"type":"Enum","constraint":"CPA|CPC|CPCV|CPE|CPF|CPM|CPV"},"dimensionName":null,"filterable":true,"filterOperations":["IN"],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Source","type":"Dimension","dataType":{"type":"Number","constraint":"3"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Clicks","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN"],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":["Pricing Type"],"allowedSchemas":null},{"field":"Impressions","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null,"allowedSchemas":null}]}]}"""
    registry.flattenDomainJsonAsString shouldBe """{"dimensions":[{"name":"advertiser","fields":["Advertiser ID","Advertiser Status","Advertiser Email"]}],"schemas":{"advertiser":["publicFact"]},"cubes":[{"name":"publicFact","mainEntityIds":{"advertiser":"Advertiser ID"},"maxDaysLookBack":[{"requestType":"SyncRequest","grain":"DailyGrain","days":30},{"requestType":"AsyncRequest","grain":"DailyGrain","days":30}],"maxDaysWindow":[{"requestType":"SyncRequest","grain":"DailyGrain","days":20},{"requestType":"AsyncRequest","grain":"DailyGrain","days":20},{"requestType":"SyncRequest","grain":"HourlyGrain","days":20},{"requestType":"AsyncRequest","grain":"HourlyGrain","days":20}],"fields":[{"field":"Advertiser ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"advertiser","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":null},{"field":"Destination URL","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":null,"filterable":false,"filterOperations":null,"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":null},{"field":"Fact ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":null},{"field":"Is Adjustment","type":"Dimension","dataType":{"type":"Enum","constraint":"N|Y"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":["internal"]},{"field":"Pricing Type","type":"Dimension","dataType":{"type":"Enum","constraint":"CPA|CPC|CPCV|CPE|CPF|CPM|CPV"},"dimensionName":null,"filterable":true,"filterOperations":["IN"],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":null},{"field":"Source","type":"Dimension","dataType":{"type":"Number","constraint":"3"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":null},{"field":"Clicks","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN"],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","allowedSchemas":null},{"field":"Impressions","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","allowedSchemas":null},{"field":"Advertiser Status","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":"advertiser","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":null},{"field":"Advertiser Email","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":"advertiser","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":["internal"]},{"field":"Ad Asset JSON","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":"advertiser","filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false,"isImageColumn":false,"allowedSchemas":null}]}]}"""
    registry.cubesJson shouldBe """["publicFact"]"""
  }

  test("Should allow aliasing of a publicFact") {
    val pubFact1: PublicFact = pubfact
    val registryBuilder = new RegistryBuilder

    registryBuilder.register(base_dim)
    registryBuilder.register(pubFact1)
    registryBuilder.registerAlias(Set((pubFact1.name, Some(3)), ("alias2", None)), pubFact1)
    val registry = registryBuilder.build()

    assert(
      registry.factMap.keys.toSet.contains(("alias2", 0))
        && registry.factMap.keys.toSet.contains(("publicFact", 0))
        && registry.factMap.keys.toSet.contains(("publicFact", 3)))

  }

  test("Should allow aliasing of a publicFact with column replacements") {
    val pubFact1: PublicFact = pubfact
    val registryBuilder = new RegistryBuilder

    registryBuilder.register(base_dim)
    registryBuilder.register(pubFact1)
    registryBuilder.registerAlias(
      Set((pubFact1.name, Some(3)), ("alias2", None)),
      pubFact1,
      dimColOverrides = Set(PubCol("price_type", "Pricing type", InEquality, isReplacement = true)))
    val registry = registryBuilder.build()

    assert(
      registry.factMap.keys.toSet.contains(("alias2", 0))
        && registry.factMap.keys.toSet.contains(("publicFact", 0))
        && registry.factMap.keys.toSet.contains(("publicFact", 3))
        && registry.factMap.get(("alias2", 0)).get.columnsByAlias.contains("Pricing type"))
  }

  test("Should fail with invalid secondary alias (duplicate)") {
    val pubFact1 = pubfact
    val registryBuilder = new RegistryBuilder

    registryBuilder.register(base_dim)
    registryBuilder.register(pubFact1)
    assertThrows[IllegalArgumentException](registryBuilder.registerAlias(Set(("turtles", Some(1)), (pubFact1.name, Some(pubFact1.revision))), pubFact1))

    val thrown = intercept[IllegalArgumentException] {
      registryBuilder.registerAlias(Set((pubFact1.name, Some(pubFact1.revision)), ("publicFact", Some(pubFact1.revision))), pubFact1)
    }
    assert(thrown.getMessage.contains("Cannot register multiple public facts with same name"))
  }

  test("Should alias a fact with an overriden secondary Dimension, keeping default base.") {
    val pubFact1: PublicFact = pubfact
    val pf2 = pubfact2
    val registryBuilder = new RegistryBuilder

    registryBuilder.register(base_dim)
    registryBuilder.register(base_dim_with_revision)
    registryBuilder.register(base_dim3_with_duplicate_col)
    registryBuilder.register(pubFact1)
    registryBuilder.register(pf2)
    registryBuilder.registerAlias(Set((pf2.name, Some(3)), ("alias2", None)), pf2, Map("advertiser" -> 1),
      dimColOverrides = Set(
        PubCol("aliased_dim", "Is Adjustment", Equality, incompatibleColumns = Set("Destination URL"), restrictedSchemas = Set(InternalSchema))
      ),
      factColOverrides = Set(
        PublicFactCol("aliased_met", "Impressions", InEquality)
      )
    )

    val registry = registryBuilder.build()

    assert(
      registry.factMap.keys.toSet.contains(("alias2", 0))
        && registry.factMap.keys.toSet.contains(("publicFact", 0))
        && registry.factMap.keys.toSet.contains(("publicFact2", 3))
        && registry.factMap.keys.toSet.contains(("publicFact2", 0)))

    assert(registry.getFact("publicFact2", Some(0)).get.dimToRevisionMap.get("advertiser") == Some(0))
    assert(registry.getFact("publicFact2", Some(3)).get.dimToRevisionMap.get("advertiser") == Some(1))
    assert(registry.flattenDomainJsonAsString.contains("Ad Asset JSON Copy")) //should only appear in Advertiser V1.
    val pFact3 = registry.getFact("publicFact2", Some(3))
    val adjName = pFact3.get.columnsByAliasMap("Is Adjustment").name
    val impName = pFact3.get.columnsByAliasMap("Impressions").name
    assert(adjName == "aliased_dim" && impName == "aliased_met")
  }
}
