package com.yahoo.maha.service.calcite

import com.yahoo.maha.core._
import com.yahoo.maha.core.request.{ASC, DESC, GroupByQuery, ReportingRequest, SyncRequest}
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.BaseMahaServiceTest
import org.scalatest.matchers.should.Matchers

class DefaultMahaCalciteSqlParserTest extends BaseMahaServiceTest with Matchers {

  val defaultMahaCalciteSqlParser = DefaultMahaCalciteSqlParser(mahaServiceConfig)

  test("test base sql parsing") {

    val sql = s"""
              select * from student_performance
              where 'Student ID' = 123
              """

    val mahaSqlNode: MahaSqlNode = defaultMahaCalciteSqlParser.parse(sql, StudentSchema, "er")
    assert(mahaSqlNode.isInstanceOf[SelectSqlNode])
    val request = mahaSqlNode.asInstanceOf[SelectSqlNode].reportingRequest
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

    val mahaSqlNode: MahaSqlNode = defaultMahaCalciteSqlParser.parse(sql, StudentSchema, "er")
    assert(mahaSqlNode.isInstanceOf[SelectSqlNode])
    val request = mahaSqlNode.asInstanceOf[SelectSqlNode].reportingRequest
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

  test("test group by with filters") {
    val sql = s"""
          select * from student_performance
          where 'Student ID' = 123
              AND 'Class ID' = 234
              AND 'Total Marks' > 0
          GROUP BY 'Student ID', 'Class ID'
          ORDER BY 'Class ID' DESC
          """

    val mahaSqlNode: MahaSqlNode = defaultMahaCalciteSqlParser.parse(sql, StudentSchema, "er")
    assert(mahaSqlNode.isInstanceOf[SelectSqlNode])
    val request = mahaSqlNode.asInstanceOf[SelectSqlNode].reportingRequest
    //print(request)
    assert(request.requestType === SyncRequest)
    assert(request.selectFields.size > 3)
    assert(request.selectFields.map(_.field).contains("Student ID"))
    assert(request.filterExpressions.size == 3)
    assert(request.queryType == GroupByQuery)

    request.sortBy.size shouldBe 1
    request.sortBy.head.field shouldBe "Class ID"
    request.sortBy.head.order.toString shouldBe "DESC"

    val ser = ReportingRequest.serialize(request)
    assert(ser != null)

    //TODO: need to validate the necessary group by columns present for select * (validator?)
  }

  test("test Multiple Order by") {
    val sql = s"""
          select * from student_performance
          where 'Student ID' = 123
              AND 'Class ID' = 234
              AND 'Total Marks' > 0
          GROUP BY 'Student ID', 'Class ID'
          ORDER BY 'Class ID', 'Total Marks' DESC
          """

    val mahaSqlNode: MahaSqlNode = defaultMahaCalciteSqlParser.parse(sql, StudentSchema, "er")
    assert(mahaSqlNode.isInstanceOf[SelectSqlNode])
    val request = mahaSqlNode.asInstanceOf[SelectSqlNode].reportingRequest
    //print(request)
    assert(request.requestType === SyncRequest)
    assert(request.selectFields.size > 3)
    assert(request.selectFields.map(_.field).contains("Student ID"))
    assert(request.filterExpressions.size == 3)
    assert(request.queryType == GroupByQuery)

    request.sortBy.size shouldBe 2
    request.sortBy.head.field shouldBe "Class ID"
    request.sortBy.head.order shouldBe ASC
    request.sortBy.last.field shouldBe "Total Marks"
    request.sortBy.last.order shouldBe DESC

    val ser:String = new String(ReportingRequest.serialize(request))
    assert(ser != null)

    val expected  =
      s"""
         |{"queryType":"groupby","cube":"student_performance","reportDisplayName":null,"schema":"student","requestType":"SyncRequest","forceDimensionDriven":false,"selectFields":[{"field":"Total Marks","alias":null,"value":null},{"field":"Marks Obtained","alias":null,"value":null},{"field":"Performance Factor","alias":null,"value":null},{"field":"Day","alias":null,"value":null},{"field":"Class ID","alias":null,"value":null},{"field":"Year","alias":null,"value":null},{"field":"Student ID","alias":null,"value":null},{"field":"Remarks","alias":null,"value":null},{"field":"Month","alias":null,"value":null},{"field":"Top Student ID","alias":null,"value":null},{"field":"Section ID","alias":null,"value":null}],"filterExpressions":[{"field":"Student ID","operator":"=","value":"123"},{"field":"Class ID","operator":"=","value":"234"},{"field":"Total Marks","operator":">","value":"0"},{"field":"Day","operator":"Between","from":"${fromDate}","to":"${toDate}"}],"sortBy":[{"field":"Class ID","order":"ASC"},{"field":"Total Marks","order":"DESC"}],"paginationStartIndex":0,"rowsPerPage":-1,"includeRowCount":false}
         |""".stripMargin
    ser should equal (expected) (after being whiteSpaceNormalised)

    //TODO: need to validate the necessary group by columns present for select * (validator?)
  }



  test("test various filter types") {
    val sql = s"""
              select * from student_performance
              where 'Student ID' = 123
              AND 'Class ID' = 234
              AND 'Total Marks' > 0
              """

    val mahaSqlNode: MahaSqlNode = defaultMahaCalciteSqlParser.parse(sql, StudentSchema, "er")
    assert(mahaSqlNode.isInstanceOf[SelectSqlNode])
    val request = mahaSqlNode.asInstanceOf[SelectSqlNode].reportingRequest
    //print(request)
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

    val mahaSqlNode: MahaSqlNode = defaultMahaCalciteSqlParser.parse(sql, StudentSchema, "er")
    assert(mahaSqlNode.isInstanceOf[SelectSqlNode])
    val request = mahaSqlNode.asInstanceOf[SelectSqlNode].reportingRequest
    //print(request)
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

    val mahaSqlNode: MahaSqlNode = defaultMahaCalciteSqlParser.parse(sql, StudentSchema, "er")
    assert(mahaSqlNode.isInstanceOf[SelectSqlNode])
    val request = mahaSqlNode.asInstanceOf[SelectSqlNode].reportingRequest
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

    val mahaSqlNode: MahaSqlNode = defaultMahaCalciteSqlParser.parse(sql, StudentSchema, "er")
    assert(mahaSqlNode.isInstanceOf[SelectSqlNode])
    val request = mahaSqlNode.asInstanceOf[SelectSqlNode].reportingRequest
    //print(request)
    assert(request.requestType === SyncRequest)
    assert(request.numDays == 3)
    assert(request.dayFilter.toString contains "BetweenFilter(Day,2021-04-18,2021-04-21)")
    assert(request.filterExpressions.toString contains "GreaterThanFilter(Total Marks,0,false,false)")
    assert(request.filterExpressions.toString contains "OrFilter(List(EqualityFilter(Student ID,123,false,false), EqualityFilter(Class ID,234,false,false)))")
  }

  test("tes tDescribe table") {
    val sql = s"""
              DESCRIBE student_performance
              """

    val mahaSqlNode: MahaSqlNode = defaultMahaCalciteSqlParser.parse(sql, StudentSchema, "er")
    assert(mahaSqlNode.isInstanceOf[DescribeSqlNode])
    val describeSqlNode = mahaSqlNode.asInstanceOf[DescribeSqlNode]
    //print(request)
    assert(describeSqlNode.cube == "student_performance")
  }

}
