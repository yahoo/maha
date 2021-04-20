package com.yahoo.maha.service.calcite

import com.yahoo.maha.core.CoreSchema.{AdvertiserSchema, InternalSchema}
import com.yahoo.maha.core.FilterOperation.{Equality, In, InEquality}
import com.yahoo.maha.core.NoopSchema.NoopSchema
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact.{Fact, FactCol, PublicFact, PublicFactCol}
import com.yahoo.maha.core.registry.RegistryBuilder
import com.yahoo.maha.core.request.{AsyncRequest, RequestType, SyncRequest}
import com.yahoo.maha.core.{ColumnContext, CoreSchema, DailyGrain, DecType, EscapingRequired, ForeignKey, Grain, HiveEngine, HourlyGrain, IntType, NotInFilter, PrimaryKey, StrType}
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.{BaseMahaServiceTest, DefaultMahaServiceConfig}
import org.scalatest.funsuite.AnyFunSuite

class DefaultMahaCalciteSqlParserTest extends BaseMahaServiceTest {

  val defaultMahaCalciteSqlParser = DefaultMahaCalciteSqlParser(mahaServiceConfig)

  test("Sql Parser test") {
    val sql = s"""
              select * from student_performance
              where 'Advertiser ID' = 123
              AND 'Source' = 'Native'
              AND 'Price Type' in ('CPC', 'CPA')
              AND 'Impressions' > 0
              """
    //TODO: convert advertiser id to advertiser_id
    //val sql = "select advertiser_id from publicFact where 'Advertiser ID' = 123";
    //TODO:
    //val sql = "select 'Advertiser ID' from publicFact where 'Advertiser ID' = 123";

    val result = defaultMahaCalciteSqlParser.parse(sql, StudentSchema, "er")
    print(result)
  }



}
