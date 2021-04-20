package com.yahoo.maha.service.calcite

import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.{BaseMahaServiceTest}

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
    print (result)
  }



}
