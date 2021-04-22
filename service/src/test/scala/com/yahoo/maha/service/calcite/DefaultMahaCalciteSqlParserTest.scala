package com.yahoo.maha.service.calcite

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
    //print(request)
    assert(request.requestType === SyncRequest)
    assert(request.selectFields.size == 11)
    assert(request.filterExpressions.size > 0)

    assert(request.filterExpressions.toString contains "EqualityFilter(Student ID,123,false,false)")
  }

  test("test group by") {
    var sql = s"""
              select 'Student ID', 'Class ID', SUM('Total Marks') from student_performance
              GROUP BY 'Student ID', 'Class ID'
              """

    var request: ReportingRequest = defaultMahaCalciteSqlParser.parse(sql, StudentSchema, "er")
    //print(request)
    assert(request.requestType === SyncRequest)
    assert(request.selectFields.size == 3)
    assert(request.selectFields.map(_.field).contains("Student ID"))
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
//    print(request)
    assert(request.requestType === SyncRequest)
    assert(request.filterExpressions.size > 0)

    assert(request.filterExpressions.toString contains "EqualityFilter(Student ID,123,false,false)")
    assert(request.filterExpressions.toString contains "EqualityFilter(Class ID,234,false,false)")
    assert(request.filterExpressions.toString contains "GreaterThanFilter(Total Marks,0,false,false)")
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
    print(request)
    assert(request.requestType === SyncRequest)
    assert(request.numDays == 1)
    assert(request.dayFilter.toString contains "EqualityFilter(Day,2021-04-20,false,false)")
  }

  test("test between filter") {
    val sql = s"""
              select * from student_performance
              where 'Student ID' = 123
              AND 'Class ID' = 234
              AND 'Day' BETWEEN '2021-04-18' AND '2021-04-21'
              AND 'Total Marks' > 0
              """

    val request: ReportingRequest = defaultMahaCalciteSqlParser.parse(sql, StudentSchema, "er")
    //print(request)
    assert(request.requestType === SyncRequest)
    assert(request.numDays == 3)
    assert(request.dayFilter.toString contains "BetweenFilter(Day,2021-04-18,2021-04-21)")
  }

  test("test or filter") {
    val sql = s"""
              select * from student_performance
              where 'Total Marks' > 0
              AND 'Day' BETWEEN '2021-04-18' AND '2021-04-21'
              AND ('Student ID' = 123 OR 'Class ID' = 234)
              """

    val request: ReportingRequest = defaultMahaCalciteSqlParser.parse(sql, StudentSchema, "er")
//    print(request)
    assert(request.requestType === SyncRequest)
    assert(request.numDays == 3)
    assert(request.dayFilter.toString contains "BetweenFilter(Day,2021-04-18,2021-04-21)")
    assert(request.filterExpressions.toString contains "GreaterThanFilter(Total Marks,0,false,false)")
    assert(request.filterExpressions.toString contains "OrFilter(List(EqualityFilter(Student ID,123,false,false), EqualityFilter(Class ID,234,false,false)))")
  }

}
