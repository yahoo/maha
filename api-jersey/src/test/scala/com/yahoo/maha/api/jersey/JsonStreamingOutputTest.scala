package com.yahoo.maha.api.jersey

import java.io.OutputStream

import com.yahoo.maha.api.jersey.example.ExampleMahaService
import com.yahoo.maha.api.jersey.example.ExampleSchema.StudentSchema
import com.yahoo.maha.core.OracleEngine
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.request.ReportingRequest
import org.scalatest.FunSuite

/**
 * Created by pranavbhole on 06/04/18.
 */
class JsonStreamingOutputTest extends FunSuite {

  val jsonRequest = s"""{
                          "cube": "student_performance",
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Class ID"},
                            {"field": "Section ID"},
                            {"field": "Total Marks"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "${ExampleMahaService.yesterday}", "to": "${ExampleMahaService.today}"},
                            {"field": "Student ID", "operator": "=", "value": "213"}
                          ]
                        }"""
  val reportingRequest = ReportingRequest.deserializeSync(jsonRequest.getBytes, StudentSchema).toOption.get

  class StringStream extends OutputStream {
    val stringBuilder = new StringBuilder()
    override def write(b: Int): Unit = {
      stringBuilder.append(b.toChar)
    }
    override def toString() : String = stringBuilder.toString()
  }

  test("Test JsonStreamingOutput") {

    val rowList = new InMemRowList() {
      override def query: Query = NoopQuery
    }

    val row = rowList.newRow
    row.addValue("Student ID", 123)
    row.addValue("Class ID", 234)
    row.addValue("Section ID", 345)
    row.addValue("Total Marks", 99)
    rowList.addRow(row)

    val jsonStreamingOutput = JsonStreamingOutput(reportingRequest, Set("Student ID", "Class ID", "Section ID"), rowList, OracleEngine, "student_grade_sheet")

    val stringStream = new StringStream
    jsonStreamingOutput.write(stringStream)

    println(stringStream.toString())
  }

}
