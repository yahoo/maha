// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.dimension

import com.yahoo.maha.core.BaseExpressionTest.PRESTO_TIMESTAMP_TO_FORMATTED_DATE
import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.DruidPostResultFunction.START_OF_THE_WEEK
import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core._
import com.yahoo.maha.core.ddl.{HiveDDLAnnotation, OracleDDLAnnotation}
import com.yahoo.maha.core.request.{AsyncRequest, SyncRequest}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Created by jians on 10/22/15.
 */
class DimensionTest extends AnyFunSuite with Matchers {
  CoreSchema.register()

  test("newDimension with no primary key should fail") {
    val thrown = intercept[IllegalArgumentException] {
      {
        ColumnContext.withColumnContext { implicit cc =>
          Dimension.newDimension("dim1", HiveEngine, LevelOne, Set(AdvertiserSchema),
            Set(
              DimCol("start_time", StrType(), annotations = Set(EscapingRequired))
              , DimCol("end_time", StrType(), annotations = Set(EscapingRequired))
            )
            , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          )
        }
      }
    }
    thrown.getMessage should startWith  ("requirement failed: Each dimension must have 1 primary key :")
  }

  test("Copy a Hive dimension without resetAliasIfNotPresent") {
    val dim : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc =>
        //import HiveExpression._
        Dimension.newDimension("dim1", HiveEngine, LevelOne, Set(AdvertiserSchema),
          Set(
            DimCol("start_time", StrType(), annotations = Set(PrimaryKey))
            , ConstDimCol("constant", StrType(), "Constant")
            , DimCol("end_time", StrType(), annotations = Set(EscapingRequired))
            , HiveDerDimCol("clicks", DecType(), "start_time")
            , HiveDimCol("new_clicks", DecType())
            , HivePartDimCol("newest_clicks", DecType())

          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        dim.createSubset("dim1_subset", "dim1", Set("end_time"), Set.empty, Set.empty, None, Map.empty, resetAliasIfNotPresent = false)
      }
    }
  }

  test("Copy a Druid dimension without resetAliasIfNotPresent") {
    val dim : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc =>
        import com.yahoo.maha.core.DruidDerivedFunction._

        Dimension.newDimension("dim1", DruidEngine, LevelOne, Set(AdvertiserSchema),
          Set(
            DimCol("stats_date", DateType("YYYY-MM-dd"), annotations = Set(PrimaryKey))
            , DimCol("decodable", IntType(), annotations = Set(EscapingRequired))
            , DimCol("discard", IntType())
            , DruidFuncDimCol("clicks", DecType(), DECODE_DIM("{decodable}", "7", "6"))
            , DruidPostResultFuncDimCol("test_col", IntType(), postResultFunction = START_OF_THE_WEEK("stats_date"))
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        dim.createSubset("dim1_subset", "dim1", Set("discard"), Set.empty, Set.empty, None, Map.empty, resetAliasIfNotPresent = false)
      }
    }
  }

  test("Copy an Oracle dimension without resetAliasIfNotPresent") {
    val dim : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc =>
        import OracleExpression._

        Dimension.newDimension("dim1", OracleEngine, LevelOne, Set(AdvertiserSchema),
          Set(
            DimCol("stats_date", DateType("YYYY-MM-dd"), annotations = Set(PrimaryKey))
            , DimCol("decodable", IntType(), annotations = Set(EscapingRequired))
            , DimCol("discard", IntType())
            , OracleDerDimCol("clicks", StrType(), DECODE_DIM("{decodable}", "7", "6"))
            , OraclePartDimCol("test_col", IntType())
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        dim.createSubset("dim1_subset", "dim1", Set("discard"), Set.empty, Set.empty, None, Map.empty, resetAliasIfNotPresent = false)
      }
    }
  }

  test("Copy an Postgres dimension without resetAliasIfNotPresent") {
    val dim : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc =>
        import PostgresExpression._

        Dimension.newDimension("dim1", PostgresEngine, LevelOne, Set(AdvertiserSchema),
          Set(
            DimCol("stats_date", DateType("YYYY-MM-dd"), annotations = Set(PrimaryKey))
            , DimCol("decodable", IntType(), annotations = Set(EscapingRequired))
            , DimCol("discard", IntType())
            , PostgresDerDimCol("clicks", StrType(), DECODE_DIM("{decodable}", "7", "6"))
            , PostgresPartDimCol("test_col", IntType())
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        dim.createSubset("dim1_subset", "dim1", Set("discard"), Set.empty, Set.empty, None, Map.empty, resetAliasIfNotPresent = false)
      }
    }
  }

  test("Copy a Bigquery dimension without resetAliasIfNotPresent") {
    val dim : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc =>
        import BigqueryExpression._

        Dimension.newDimension("dim1", BigqueryEngine, LevelOne, Set(AdvertiserSchema),
          Set(
            DimCol("stats_date", DateType("YYYY-MM-dd"), annotations = Set(PrimaryKey))
            , DimCol("decodable", IntType(), annotations = Set(EscapingRequired))
            , DimCol("discard", IntType())
            , BigqueryDerDimCol("clicks", StrType(), DECODE_DIM("{decodable}", "7", "6"))
            , BigqueryPartDimCol("test_col", IntType())
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        dim.createSubset("dim1_subset", "dim1", Set("discard"), Set.empty, Set.empty, None, Map.empty, resetAliasIfNotPresent = false)
      }
    }
  }

  test("Copy a Presto dimension without resetAliasIfNotPresent") {
    val dim : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc =>
        //import PrestoExpression._
        Dimension.newDimension("dim1", PrestoEngine, LevelOne, Set(AdvertiserSchema),
          Set(
            DimCol("start_time", StrType(), annotations = Set(PrimaryKey))
            , ConstDimCol("constant", StrType(), "Constant")
            , DimCol("end_time", StrType(), annotations = Set(EscapingRequired))
            , PrestoDerDimCol("clicks", DecType(), "start_time")
            , PrestoDimCol("new_clicks", DecType())
            , PrestoPartDimCol("newest_clicks", DecType())

          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        dim.createSubset("dim1_subset", "dim1", Set("end_time"), Set.empty, Set.empty, None, Map.empty, resetAliasIfNotPresent = false)
      }
    }
  }

  test("Copy a Presto dimension with resetAliasIfNotPresent") {
    val dim : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc =>
        //import PrestoExpression._
        Dimension.newDimension("dim1", PrestoEngine, LevelOne, Set(AdvertiserSchema),
          Set(
            DimCol("start_time", StrType(), annotations = Set(PrimaryKey))
            , ConstDimCol("constant", StrType(), "Constant")
            , DimCol("end_time", StrType(), annotations = Set(EscapingRequired))
            , PrestoDerDimCol("clicks", DecType(), "start_time")
            , PrestoDimCol("new_clicks", DecType())
            , PrestoPartDimCol("newest_clicks", DecType())

          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        dim.createSubset("dim1_subset", "dim1", Set("end_time"), Set.empty, Set.empty, None, Map.empty, resetAliasIfNotPresent = true)
      }
    }
  }

  test("Copy a Hive dimension with resetAliasIfNotPresent") {
    val dim : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc =>
        //import HiveExpression._
        Dimension.newDimension("dim1", HiveEngine, LevelOne, Set(AdvertiserSchema),
          Set(
            DimCol("start_time", StrType(), annotations = Set(PrimaryKey))
            , ConstDimCol("constant", StrType(), "Constant")
            , DimCol("end_time", StrType(), annotations = Set(EscapingRequired))
            , HiveDerDimCol("clicks", DecType(), "start_time")
            , HiveDimCol("new_clicks", DecType())
            , HivePartDimCol("newest_clicks", DecType())

          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        dim.createSubset("dim1_subset", "dim1", Set("end_time"), Set.empty, Set.empty, None, Map.empty, resetAliasIfNotPresent = true)
      }
    }
  }

  test("Copy a Druid dimension with resetAliasIfNotPresent") {
    val dim : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc =>
        import com.yahoo.maha.core.DruidDerivedFunction._

        Dimension.newDimension("dim1", DruidEngine, LevelOne, Set(AdvertiserSchema),
          Set(
            DimCol("stats_date", DateType("YYYY-MM-dd"), annotations = Set(PrimaryKey))
            , DimCol("decodable", IntType(), annotations = Set(EscapingRequired))
            , DimCol("discard", IntType())
            , DruidFuncDimCol("clicks", DecType(), DECODE_DIM("{decodable}", "7", "6"))
            , DruidPostResultFuncDimCol("test_col", IntType(), postResultFunction = START_OF_THE_WEEK("stats_date"))
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        dim.createSubset("dim1_subset", "dim1", Set("discard"), Set.empty, Set.empty, None, Map.empty, resetAliasIfNotPresent = true)
      }
    }
  }

  test("Copy an Oracle dimension with resetAliasIfNotPresent") {
    val dim : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc =>
        import OracleExpression._

        Dimension.newDimension("dim1", OracleEngine, LevelOne, Set(AdvertiserSchema),
          Set(
            DimCol("stats_date", DateType("YYYY-MM-dd"), annotations = Set(PrimaryKey))
            , DimCol("decodable", IntType(), annotations = Set(EscapingRequired))
            , DimCol("discard", IntType())
            , OracleDerDimCol("clicks", StrType(), DECODE_DIM("{decodable}", "7", "6"))
            , OraclePartDimCol("test_col", IntType())
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        dim.createSubset("dim1_subset", "dim1", Set("discard"), Set.empty, Set.empty, None, Map.empty, resetAliasIfNotPresent = true)
      }
    }
  }

  test("Copy an Postgres dimension with resetAliasIfNotPresent") {
    val dim : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc =>
        import PostgresExpression._

        Dimension.newDimension("dim1", PostgresEngine, LevelOne, Set(AdvertiserSchema),
          Set(
            DimCol("stats_date", DateType("YYYY-MM-dd"), annotations = Set(PrimaryKey))
            , DimCol("decodable", IntType(), annotations = Set(EscapingRequired))
            , DimCol("discard", IntType())
            , PostgresDerDimCol("clicks", StrType(), DECODE_DIM("{decodable}", "7", "6"))
            , PostgresPartDimCol("test_col", IntType())
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        dim.createSubset("dim1_subset", "dim1", Set("discard"), Set.empty, Set.empty, None, Map.empty, resetAliasIfNotPresent = true)
      }
    }
  }

  test("Copy a Bigquery dimension with resetAliasIfNotPresent") {
    val dim : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc =>
        import BigqueryExpression._

        Dimension.newDimension("dim1", BigqueryEngine, LevelOne, Set(AdvertiserSchema),
          Set(
            DimCol("stats_date", DateType("YYYY-MM-dd"), annotations = Set(PrimaryKey))
            , DimCol("decodable", IntType(), annotations = Set(EscapingRequired))
            , DimCol("discard", IntType())
            , BigqueryDerDimCol("clicks", StrType(), DECODE_DIM("{decodable}", "7", "6"))
            , BigqueryPartDimCol("test_col", IntType())
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        )
      }
    }

    {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        dim.createSubset("dim1_subset", "dim1", Set("discard"), Set.empty, Set.empty, None, Map.empty, resetAliasIfNotPresent = true)
      }
    }
  }

  test("newDimension should fail with derived expression with unknown referenced fields") {
    val thrown = intercept[IllegalArgumentException] {
      {
        import com.yahoo.maha.core.BaseExpressionTest._
        import com.yahoo.maha.core.HiveExpression._
        ColumnContext.withColumnContext { implicit cc =>
          Dimension.newDimension("dim1", HiveEngine, LevelOne, Set(AdvertiserSchema),
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              ,DimCol("start_time", StrType(), annotations = Set(EscapingRequired))
              , DimCol("end_time", StrType(), annotations = Set(EscapingRequired))
              , HiveDerDimCol("Campaign End Date Full", StrType(),TIMESTAMP_TO_FORMATTED_DATE("{time}", "YYYY-MM-dd HH:mm:ss"))
            )
            , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          )
        }

      }
    }
    thrown.getMessage should startWith ("requirement failed: Failed derived expression validation, unknown referenced column")
  }

  test("newDimension should fail with more than one primary key") {
    val thrown = intercept[IllegalArgumentException] {
      {
        ColumnContext.withColumnContext { implicit cc =>
          Dimension.newDimension("dim1", HiveEngine, LevelOne, Set(AdvertiserSchema),
            Set(
              DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
              , DimCol("dimcol2", IntType(), annotations = Set(PrimaryKey))
            )
            , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          )
        }
      }
    }
    thrown.getMessage should startWith ("requirement failed: Each dimension must have 1 primary key :")
  }

  test("newDimension should fail with columns with different engines") {
    intercept[ClassCastException] {
      {
        import com.yahoo.maha.core.BaseExpressionTest._
        import com.yahoo.maha.core.HiveExpression._
        ColumnContext.withColumnContext { implicit cc =>
          Dimension.newDimension("dim1", HiveEngine, LevelOne, Set(AdvertiserSchema),
            Set(
              DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
              , DimCol("start_time", StrType(), annotations = Set(EscapingRequired))
              , DimCol("end_time", StrType(), annotations = Set(EscapingRequired))
              , OracleDerDimCol("Campaign Start Date Full", StrType(), TIMESTAMP_TO_FORMATTED_DATE("{start_time}", "YYYY-MM-dd HH:mm:ss"))
              , HiveDerDimCol("Campaign End Date Full", StrType(),TIMESTAMP_TO_FORMATTED_DATE("{end_time}", "YYYY-MM-dd HH:mm:ss"))
            )
            , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          )
        }

      }
    }
  }

  test("newDimension should fail with columns with different engine requirement") {
    val thrown = intercept[IllegalArgumentException] {
      {
        import OracleExpression._
        ColumnContext.withColumnContext { implicit cc =>
          Dimension.newDimension("dim1", HiveEngine, LevelOne, Set(AdvertiserSchema),
            Set(
              DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
              , DimCol("dimcol2", IntType())
              , OracleDerDimCol("Campaign Start Date Full", StrType(), TIMESTAMP_TO_FORMATTED_DATE("{start_time}", "YYYY-MM-dd HH:mm:ss"))
            )
            , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          )
        }
      }
    }
    thrown.getMessage should startWith ("requirement failed: Failed engine requirement dim=dim1, engine=Hive, col=OracleDerDimCol")
  }

  test("newDimension should fail with dimension annotated with different engine requirement") {
    val thrown = intercept[IllegalArgumentException] {
      {
        ColumnContext.withColumnContext { implicit cc =>
          Dimension.newDimension("dim1", HiveEngine, LevelOne, Set(AdvertiserSchema),
            Set(
              DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
              , DimCol("dimcol2", IntType())
            )
            , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
            , annotations = Set(OracleHashPartitioning)
          )
        }
      }
    }
    thrown.getMessage should startWith ("requirement failed: Failed engine requirement dim=dim1, engine=Hive, annotation=OracleHashPartitioning")
  }

  test("newDimension should fail with column annotated with different engine requirement") {
    val thrown = intercept[IllegalArgumentException] {
      {
        ColumnContext.withColumnContext { implicit cc =>
          Dimension.newDimension("dim1", OracleEngine, LevelOne, Set(AdvertiserSchema),
            Set(
              DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
              , DimCol("dimcol2", IntType(), annotations = Set(HiveSnapshotTimestamp))
            )
            , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          )
        }
      }
    }
    thrown.getMessage should startWith ("requirement failed: Failed engine requirement dim=dim1, engine=Oracle, col=dimcol2, annotation=HiveSnapshotTimestamp")
  }

  test("newDimension should fail with duplicate columns") {
    val thrown = intercept[IllegalArgumentException] {
      {
        ColumnContext.withColumnContext { implicit cc =>
          Dimension.newDimension("dim1", HiveEngine, LevelOne, Set(AdvertiserSchema),
            Set(
              DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
              , DimCol("dimcol2", IntType())
              , DimCol("dimcol2", IntType())
            )
            , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          )
        }
      }
    }
    thrown.getMessage should startWith ("requirement failed: Column already exists :")
  }

  test("newDimension should succeed with no duplicate columns") {
    val dim : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc =>
        Dimension.newDimension("dim1", HiveEngine, LevelOne, Set(AdvertiserSchema),
          Set(
            DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
            , DimCol("dimcol2", IntType())
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        )
      }

    }
    assert(dim != null)
  }

  test("newDimension with PrestoEngine should succeed with no duplicate columns") {
    val dim : DimensionBuilder = {
      ColumnContext.withColumnContext { implicit cc =>
        Dimension.newDimension("dim1", PrestoEngine, LevelOne, Set(AdvertiserSchema),
          Set(
            DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
            , DimCol("dimcol2", IntType())
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        )
      }
    }
    assert(dim != null)
  }

  test("newDimension should fail if ddl annotation is of a different engine other than the engine of the dim") {
    val thrown = intercept[IllegalArgumentException] {
      {
        ColumnContext.withColumnContext { implicit cc =>
          Dimension.newDimension("dim", HiveEngine, LevelOne, Set(AdvertiserSchema),
            Set(
              DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
            )
            , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
            , ddlAnnotation = Option(OracleDDLAnnotation())
          )
        }
      }
    }
    thrown.getMessage should startWith ("requirement failed: Failed engine requirement dim=dim, engine=Hive, ddlAnnotation=Some(OracleDDLAnnotation(Set(),Set()))")
  }

  test("validates should fail if schema field maps to non-existing column") {
    val thrown= intercept[IllegalArgumentException] {
      {
        ColumnContext.withColumnContext { implicit cc =>
          new DimTable("dim1", 1, OracleEngine, LevelOne, Set(AdvertiserSchema),
            Set(
              DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
              , DimCol("dimcol2", IntType())
            )
            , None
            , Map((AdvertiserSchema, "dim"))
            , Set.empty
            , None
            , true
            , None
            , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
            , None
          )
        }
      }
    }
    thrown.getMessage should startWith ("requirement failed: Schema field mapping invalid for dim=");
  }

  test("validates engine should fail if column engine is different from dim engine") {
    val thrown= intercept[IllegalArgumentException] {
      {
        ColumnContext.withColumnContext { implicit cc =>
          new DimTable("dim1", 1, OracleEngine, LevelOne, Set(AdvertiserSchema),
            Set(
              DimCol("dimcol1", IntType(), annotations = Set(PrimaryKey))
              , HiveDimCol("dimcol2", IntType())
            )
            , None
            , Map((AdvertiserSchema, "dimcol1"))
            , Set.empty
            , None
            , true
            , None
            , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
            , None
          )
        }
      }
    }
    thrown.getMessage should startWith ("requirement failed: Failed engine requirement dim");
  }

  def dimBuilder : DimensionBuilder = {
    ColumnContext.withColumnContext { implicit cc =>
      import HiveExpression._
      import com.yahoo.maha.core.BaseExpressionTest.DECODE_DIM
      Dimension.newDimension("dim", HiveEngine, LevelOne, Set(AdvertiserSchema),
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("source", IntType(3))
          , DimCol("another", IntType(3), alias = Option("source"))
          , DimCol("start_time", StrType(), annotations = Set(EscapingRequired))
          , DimCol("end_time", StrType(), annotations = Set(EscapingRequired))
          , DimCol("some_type", IntType(), annotations = Set(EscapingRequired))
          , HiveDerDimCol("some_type_2", IntType(), DECODE_DIM("{some_type}","1", "Other Type", "{some_type}"))
        )
        , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
      )
    }
  }

  test("withAlternativeEngine should fail with the same engine as base engine") {
    val thrown= intercept[IllegalArgumentException] {
      val dim1 = dimBuilder
      ColumnContext.withColumnContext { implicit  cc =>
        dim1.withAlternateEngine("dim2", "dim", HiveEngine,
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("start_time", StrType(), annotations = Set(EscapingRequired))
            , DimCol("end_time", StrType(), annotations = Set(EscapingRequired))
            , DimCol("some_type", IntType(), annotations = Set(EscapingRequired))
          )
        )
      }
    }
    thrown.getMessage should startWith ("requirement failed: Alternate must have different engine from source dim")
  }

  test("withAlternativeEngine should fail with the same engine as previous alternative engine") {
    val thrown= intercept[IllegalArgumentException] {
      val dim1 = dimBuilder
      val dim2 = ColumnContext.withColumnContext { implicit  cc =>
        dim1.withAlternateEngine("dim2", "dim", OracleEngine,
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("start_time", StrType(), annotations = Set(EscapingRequired))
            , DimCol("end_time", StrType(), annotations = Set(EscapingRequired))
            , DimCol("some_type", IntType(), annotations = Set(EscapingRequired))
          )
        )
      }
      ColumnContext.withColumnContext { implicit  cc =>
        dim2.withAlternateEngine("dim3", "dim", OracleEngine,
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("start_time", StrType(), annotations = Set(EscapingRequired))
            , DimCol("end_time", StrType(), annotations = Set(EscapingRequired))
            , DimCol("some_type", IntType(), annotations = Set(EscapingRequired))
          )
        )
      }
    }
    thrown.getMessage should startWith ("requirement failed: Alternate must have different engine from existing alternate engines");
  }

  test("withAlternateEngine should fail if ddl annotation is of a different engine other than the engine of the dim") {
    val dim1 = dimBuilder
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit  cc : ColumnContext =>
        dim1.withAlternateEngine("dim2", "dim", OracleEngine,
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("start_time", StrType(), annotations = Set(EscapingRequired))
            , DimCol("end_time", StrType(), annotations = Set(EscapingRequired))
            , DimCol("some_type", IntType(), annotations = Set(EscapingRequired))
          )
          , None
          , ddlAnnotation = Option(HiveDDLAnnotation()))
      }
    }
    thrown.getMessage should startWith ("requirement failed: Failed engine requirement dim=dim2, engine=Oracle, ddlAnnotation=Some(HiveDDLAnnotation(Map(),Vector()))")
  }

  test("createSubset should succeed for valid discarding set") {
    val dim1 = dimBuilder
    dim1.createSubset("dim2", "dim", Set("end_time"), Set(AdvertiserSchema))
    assert(dim1.tableDefs.contains("dim2"))
  }

  test("createSubset should not include dependent columns when discarding") {
    val dim1 = dimBuilder
    dim1.createSubset("dim2", "dim", Set("some_type"), Set(AdvertiserSchema))
    assert(dim1.tableDefs.contains("dim2"))
    assert(!dim1.tableDefs("dim2").columnNames("some_type2"))
  }

  test("construct public dim should fail if dim map is empty") {
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc =>
        PublicDim("pd","pd", null,
          Set(
            PubCol("id", "PublicDim ID", InEquality)
          ), Map.empty, Set.empty, Set.empty)
      }
    }
    thrown.getMessage should startWith("requirement failed: dims should not be empty")
  }

  test("public dim must have one primary key") {
    val dim = dimBuilder
    val thrown = intercept[IllegalArgumentException] {
      dim.toPublicDimension("pd","pd",
        Set(
          PubCol("start_time", "Start Time", Equality)
          , PubCol("end_time", "End Time", Equality)
        ), Set.empty
      )
    }
    thrown.getMessage should startWith("requirement failed: Each public dimension must have 1 primary key")
  }

  test("toPublicDimension should fail if pub columns are not in the base dim") {
    val dim = dimBuilder
    val thrown = intercept[IllegalArgumentException] {
      dim.toPublicDimension("pd","pd",
        Set(
          PubCol("id", "PD ID", Equality)
          , PubCol("start_time", "Start Time", Equality)
          , PubCol("end_time", "End Time", Equality)
          , PubCol("test_col", "Test Col", Equality)
        ), Set.empty
      )
    }
    thrown.getMessage should startWith("requirement failed: publicDim=pd, base dim must have all columns!")
  }

  test("toPublicDimension should fail if forced filter column is not in the base dim") {
    val dim = dimBuilder
    val thrown = intercept[IllegalArgumentException] {
      dim.toPublicDimension("pd","pd",
        Set(
          PubCol("id", "PD ID", Equality)
          , PubCol("start_time", "Start Time", Equality)
          , PubCol("end_time", "End Time", Equality)
        ),
        forcedFilters = Set(EqualityFilter("blah", "blah", isForceFilter = true)), highCardinalityFilters = Set.empty
      )
    }
    thrown.getMessage should startWith("requirement failed: Forced filter on non-existing column : EqualityFilter(blah,blah,true,false)")
  }

  test("toPublicDimension should fail if forced filter columns ARE in the base dim, but resolve to the same base column (duplicate where clause)") {
    val dim = dimBuilder
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit cc: ColumnContext =>
        dim.withAlternateEngine("dim2", "dim", OracleEngine,
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("start_time", StrType(), annotations = Set(EscapingRequired))
            , DimCol("source", IntType(3))
            , DimCol("another", IntType(3), alias = Option("source"))
            , DimCol("end_time", StrType(), annotations = Set(EscapingRequired))
            , DimCol("some_type", IntType(), annotations = Set(EscapingRequired))
          )
        )
      }
        .toPublicDimension("dim2", "dim",
          Set(
            PubCol("id", "PD ID", Equality)
            , PubCol("start_time", "Start Time", Equality)
            , PubCol("source", "Source", Equality)
            , PubCol("another", "Another Source", Equality)
            , PubCol("end_time", "End Time", Equality)
          ),
          forcedFilters = Set(EqualityFilter("Another Source", "10", isForceFilter = true), EqualityFilter("Source", "10", isForceFilter = true)), highCardinalityFilters = Set.empty
        )
  }
  thrown.getMessage should startWith("requirement failed: Forced Filters public fact and map of forced base cols differ in size")

  }

  test("toPublicDimension should fail if high cardinality filter column not in definition") {
    val dim = dimBuilder
    val thrown = intercept[IllegalArgumentException] {
      dim.toPublicDimension("pd","pd",
        Set(
          PubCol("id", "PD ID", Equality)
          , PubCol("start_time", "Start Time", Equality)
          , PubCol("end_time", "End Time", Equality)
        ),
        highCardinalityFilters = Set(NotInFilter("blah", List("blah")))
      )
    }
    thrown.getMessage should startWith("requirement failed: Unknown high cardinality filter : blah")
  }

  test("toPublicDimension should fail if a declared forced filter doesn't have isForcedFilter=true") {
    val dim = dimBuilder
    val thrown = intercept[IllegalArgumentException] {
      dim.toPublicDimension(
        "pubDim",
        "pubDim",
        Set(
          PubCol("id", "PD ID", Equality)
        ),
        forcedFilters = Set(EqualityFilter("PD ID", "1"))
      )
    }
    thrown.getMessage should startWith("requirement failed: Forced Filter boolean false, expected true")
  }

  test("newDimension should fail if derived expression is not defined for derived columns") {
    val thrown = intercept[IllegalArgumentException] {
      {
        ColumnContext.withColumnContext { implicit cc =>
          Dimension.newDimension("dim1", HiveEngine, LevelOne, Set(AdvertiserSchema),
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              ,HiveDerDimCol("derived_col", StrType(), null)
            )
            , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          )
        }

      }
    }
    thrown.getMessage should startWith ("requirement failed: Derived expression should be defined for a derived column")
  }

  test("withAlternateEngine should fail if derived expression is not defined for derived columns") {
    val dim1 = dimBuilder
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext { implicit  cc : ColumnContext =>
        dim1.withAlternateEngine("dim2", "dim", OracleEngine,
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("start_time", StrType(), annotations = Set(EscapingRequired))
            , DimCol("end_time", StrType(), annotations = Set(EscapingRequired))
            , DimCol("some_type", IntType(), annotations = Set(EscapingRequired))
            , OracleDerDimCol("ora_derived_col", StrType(), null)
          )
        )
      }
    }
    thrown.getMessage should startWith ("requirement failed: Derived expression should be defined for a derived column")
  }

  test("withAlternateEngine druid should, covering Druid dervied expressions") {
    val dim1 = dimBuilder
    import com.yahoo.maha.core.DruidDerivedFunction.DECODE_DIM
    import com.yahoo.maha.core._
    ColumnContext.withColumnContext { implicit  cc : ColumnContext =>
      dim1.withAlternateEngine("dim2", "dim", DruidEngine,
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("start_time", StrType(), annotations = Set(EscapingRequired))
          , DimCol("end_time", StrType(), annotations = Set(EscapingRequired))
          , DimCol("some_type", IntType(), annotations = Set(EscapingRequired))
          , DruidFuncDimCol("druid_derived_col", StrType(), DECODE_DIM("{end_time}", "'ON'", "'ON'", "'OFF'"))
        )
      )
    }
  }

  test("withAlternateEngine presto should succeed, covering Presto dervied expressions") {
    val dim1 = dimBuilder
    import com.yahoo.maha.core._
    import PrestoExpression._
    ColumnContext.withColumnContext { implicit  cc : ColumnContext =>
      dim1.withAlternateEngine("dim2", "dim", PrestoEngine,
        Set(
          DimCol("id", IntType(), annotations = Set(PrimaryKey))
          , DimCol("created_date", StrType(), annotations = Set(EscapingRequired))
          , PrestoDerDimCol("presto_derived_col", StrType(), PRESTO_TIMESTAMP_TO_FORMATTED_DATE("{created_date}", "YYYY-MM-dd"), annotations = Set())
        )
      )
    }
  }

  test("Should create dims of level nine and eight and Seven") {
    {
      import HiveExpression._
      import com.yahoo.maha.core.BaseExpressionTest._
      ColumnContext.withColumnContext { implicit cc =>
        Dimension.newDimension("dim9", HiveEngine, LevelNine, Set(AdvertiserSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            ,HiveDerDimCol("derived_col", StrType(), DECODE_DIM("{id}", "'ON'", "'ON'", "'OFF'"))
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        )
      }
    }
    {
      import HiveExpression._
      import com.yahoo.maha.core.BaseExpressionTest._
      ColumnContext.withColumnContext { implicit cc =>
        Dimension.newDimension("dim8", HiveEngine, LevelEight, Set(AdvertiserSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            ,HiveDerDimCol("derived_col", StrType(), TIMESTAMP_TO_FORMATTED_DATE("{id}", "YYYY-MM-dd HH:mm:ss"))
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        )
      }

    }
    {
      import HiveExpression._
      import com.yahoo.maha.core.BaseExpressionTest._
      ColumnContext.withColumnContext { implicit cc =>
        Dimension.newDimension("dim7", HiveEngine, LevelSeven, Set(AdvertiserSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            ,HiveDerDimCol("derived_col", StrType(), DECODE_DIM("{id}", "'ON'", "'ON'", "''OFF'"))
            ,HiveDerDimCol("derived_colavs", StrType(), DECODE_DIM("{id}", "{id}", "{id}", "{id}"))
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        )
      }
    }
  }

}
