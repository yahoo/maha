package com.yahoo.maha.service.calcite

import com.yahoo.maha.core.{EqualityFilter, GreaterThanFilter, InFilter}
import com.yahoo.maha.core.request.{GroupByQuery, ReportingRequest, SyncRequest}
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.BaseMahaServiceTest

class DefaultMahaCalciteSqlParserTest extends BaseMahaServiceTest {

  val defaultMahaCalciteSqlParser = DefaultMahaCalciteSqlParser(mahaServiceConfig)

  test("test base sql parsing") {
    val sql = s"""
              select * from student_performance
              where 'Student ID' = 123
              """

    val request: ReportingRequest = defaultMahaCalciteSqlParser.parse(sql, StudentSchema, "er")
    //print(result)
    assert(request.requestType === SyncRequest)
    assert(request.selectFields.size == 11)
    assert(request.filterExpressions.size > 0)

    assert(request.filterExpressions(0).isInstanceOf[EqualityFilter])
    assert(request.filterExpressions(0).field == "Student ID")
    assert(request.filterExpressions(0).asValues == "123")
  }

  test("test group by") {
    var sql = s"""
              select 'Student ID', 'Class ID', SUM('Total Marks') from student_performance
              GROUP BY 'Student ID', 'Class ID'
              """

    var request: ReportingRequest = defaultMahaCalciteSqlParser.parse(sql, StudentSchema, "er")
    //print(result)
    assert(request.requestType === SyncRequest)
    assert(request.selectFields.size == 3)
    assert(request.filterExpressions.size == 0)
    assert(request.queryType == GroupByQuery)

    //TODO: need to validate the necessary group by columns present for select * (validator?)
    sql = s"""
          select * from student_performance
          GROUP BY 'Student ID', 'Class ID'
          """
  }

  test("test various filter types") {
    val sql = s"""
              select * from student_performance
              where 'Student ID' = 123
              AND 'Class ID' = 234
              AND 'Total Marks' > 0
              """

    val request: ReportingRequest = defaultMahaCalciteSqlParser.parse(sql, StudentSchema, "er")
    //print(result)
    assert(request.requestType === SyncRequest)
    assert(request.filterExpressions.size > 0)

    assert(request.filterExpressions(0).isInstanceOf[EqualityFilter])
    assert(request.filterExpressions(0).field == "Student ID")
    assert(request.filterExpressions(0).asValues == "123")

    assert(request.filterExpressions(1).isInstanceOf[EqualityFilter])
    assert(request.filterExpressions(1).field == "Class ID")
    assert(request.filterExpressions(1).asValues == "234")

    assert(request.filterExpressions(2).isInstanceOf[GreaterThanFilter])
    assert(request.filterExpressions(2).field == "Total Marks")
    assert(request.filterExpressions(2).asValues == "0")
  }

  test("test time filter") {
    val sql = s"""
              select * from student_performance
              where 'Student ID' = 123
              AND 'Class ID' = 234
              AND 'Total Marks' > 0
              AND 'Day' = '2021-04-20'
              """

    val request: ReportingRequest = defaultMahaCalciteSqlParser.parse(sql, StudentSchema, "er")
    //print(result)
    assert(request.requestType === SyncRequest)
    assert(request.dayFilter.field == "Day")
    assert(request.dayFilter.asValues == "2021-04-20")
  }



}
