package com.yahoo.maha.service.curators

import com.yahoo.maha.core.request._
import com.yahoo.maha.service.BaseMahaServiceTest
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema

class DrilldownConfigTest extends BaseMahaServiceTest {
  test("Create a valid DrillDownConfig") {
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
    assert(!drilldownConfig.enforceFilters)
    assert(drilldownConfig.maxRows == 1000)
    assert(drilldownConfig.ordering.contains(SortBy("Class ID", ASC)))
    assert(drilldownConfig.dimension == Field("Section ID", None, None))
    assert(drilldownConfig.cube == "student_performance")
  }

  test("Create a valid DrillDownConfig with invalid ordering") {
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
                                              "order": "willreturndesc"
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
    assert(!drilldownConfig.enforceFilters)
    assert(drilldownConfig.maxRows == 1000)
    assert(drilldownConfig.ordering.contains(SortBy("Class ID", DESC)))
    assert(drilldownConfig.dimension == Field("Section ID", None, None))
    assert(drilldownConfig.cube == "student_performance")
  }

  test("DrillDownConfig should throw error on max rows.") {
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
                                "mr": 1001
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
    val thrown = intercept[Exception] {
      drilldownConfig.validateCuratorConfig(reportingRequest.curatorJsonConfigMap, reportingRequest)
    }
    assert(thrown.getMessage.contains("Max Rows limit of 1000 exceeded"))
  }

  test("DrillDownConfig should throw error on no dimension.") {
    val json : String =
      s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drillDown" : {
                              "config" : {
                                "enforceFilters": "true",
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
    val thrown = intercept[Exception] {
      drilldownConfig.validateCuratorConfig(reportingRequest.curatorJsonConfigMap, reportingRequest)
    }
    assert(thrown.getMessage.contains("CuratorConfig for a DrillDown should have a dimension declared"))
  }

  test("DrillDownConfig should throw error on wrong DrillDown declaration.") {
    val json : String =
      s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "downdrill" : {
                              "config" : {
                                "enforceFilters": "true",
                                "dimension": false,
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
    val thrown = intercept[Exception] {
      drilldownConfig.validateCuratorConfig(reportingRequest.curatorJsonConfigMap, reportingRequest)
    }
    assert(thrown.getMessage.contains("DrillDown may not be created without a declaration"))
  }

  test("Create a valid DrillDownConfig with Descending order and no given ordering") {
    val json : String =
      s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drillDown" : {
                              "config" : {
                                "enforceFilters": true,
                                "dimension": "Section ID",
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
    assert(drilldownConfig.enforceFilters)
    assert(drilldownConfig.maxRows == 1000)
    assert(drilldownConfig.ordering.contains(SortBy("Total Marks", DESC)))
    assert(drilldownConfig.dimension == Field("Section ID", None, None))
    assert(drilldownConfig.cube == "student_performance")
  }
}
