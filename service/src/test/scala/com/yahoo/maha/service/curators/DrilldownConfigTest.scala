package com.yahoo.maha.service.curators

import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.service.BaseMahaServiceTest
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema

class DrilldownConfigTest extends BaseMahaServiceTest {
  test("Create a valid DrilldownConfig") {
    val json : String =
      s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drillDown" : {
                              "config" : {
                                "enforceFilters": "true",
                                "dimension": "Section ID",
                                "ordering": [{
                                              "field": "Class ID",
                                              "order": "asc"
                                              }],
                                "mr": 1000
                              }
                            }
                          },
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Class ID"},
                            {"field": "Section ID"},
                            {"field": "Total Marks"}
                          ],
                          "sortBy": [
                            {"field": "Total Marks", "order": "Desc"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "2018-01-01", "to": "2018-01-02"},
                            {"field": "Student ID", "operator": "=", "value": "213"}
                          ]
                        }"""
    val reportingRequestResult = ReportingRequest.deserializeSyncWithFactBias(json.getBytes, schema = StudentSchema)
    require(reportingRequestResult.isSuccess)
    val reportingRequest = reportingRequestResult.toOption.get

    val drilldownConfig = DrilldownConfig
    drilldownConfig.validateCuratorConfig(reportingRequest.curatorJsonConfigMap, reportingRequest)

    println(drilldownConfig)
  }
}
