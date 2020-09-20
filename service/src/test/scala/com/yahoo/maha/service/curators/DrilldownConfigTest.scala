// Copyright 2018, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.curators

import com.yahoo.maha.core.request._
import com.yahoo.maha.service.BaseMahaServiceTest
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import org.scalatest.BeforeAndAfterAll

class DrilldownConfigTest extends BaseMahaServiceTest with BeforeAndAfterAll {

  override protected def afterAll(): Unit =  {
    super.afterAll()
    server.shutdownNow()
  }

  test("Create a valid DrillDownConfig") {
    val json : String =
      s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drilldown" : {
                              "config" : {
                                "enforceFilters": true,
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

    val drillDownConfig : DrilldownRequest = DrilldownConfig.parse(reportingRequest.curatorJsonConfigMap("drilldown")).toOption.get.requests.head

    
    assert(drillDownConfig.enforceFilters)
    assert(drillDownConfig.maxRows == 1000)
    assert(drillDownConfig.ordering.contains(SortBy("Class ID", ASC)))
    assert(drillDownConfig.dimensions == List(Field("Section ID", None, None)))
    assert(drillDownConfig.cube == "")
  }

  test("Create a valid DrillDownConfig with reversed ordering") {
    val json : String =
      s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drilldown" : {
                              "config" : {
                                "enforceFilters": true,
                                "dimension": "Section ID",
                                "ordering": [{
                                              "field": "Class ID",
                                              "order": "desc"
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

    val drillDownConfig : DrilldownRequest = DrilldownConfig.parse(reportingRequest.curatorJsonConfigMap("drilldown")).toOption.get.requests.head


    assert(drillDownConfig.enforceFilters)
    assert(drillDownConfig.maxRows == 1000)
    assert(drillDownConfig.ordering.contains(SortBy("Class ID", DESC)))
    assert(drillDownConfig.dimensions == List(Field("Section ID", None, None)))
    assert(drillDownConfig.cube == "")
  }

  test("Create a valid DrillDownConfig with multiple requests") {
    val json : String =
      s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drilldown" : {
                              "config" : [{
                                "enforceFilters": true,
                                "dimension": "Section ID",
                                "ordering": [{
                                              "field": "Class ID",
                                              "order": "asc"
                                              }],
                                "mr": 1000
                              }
                              , {
                                "enforceFilters": true,
                                "dimension": "Section ID",
                                "ordering": [{
                                              "field": "Class ID",
                                              "order": "desc"
                                              }],
                                "mr": 1000
                              }]
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
    require(reportingRequestResult.isSuccess, reportingRequestResult)
    val reportingRequest = reportingRequestResult.toOption.get

    val drillDownConfig : DrilldownConfig = DrilldownConfig.parse(reportingRequest.curatorJsonConfigMap("drilldown")).toOption.get

    val req1 = drillDownConfig.requests(0)
    val req2 = drillDownConfig.requests(1)

    assert(req1.enforceFilters)
    assert(req1.maxRows == 1000)
    assert(req1.ordering.contains(SortBy("Class ID", ASC)))
    assert(req1.dimensions == List(Field("Section ID", None, None)))
    assert(req1.cube == "")
    assert(req2.enforceFilters)
    assert(req2.maxRows == 1000)
    assert(req2.ordering.contains(SortBy("Class ID", DESC)))
    assert(req2.dimensions == List(Field("Section ID", None, None)))
    assert(req2.cube == "")
  }

  test("Create a valid DrillDownConfig with invalid ordering") {
    val json : String =
      s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drilldown" : {
                              "config" : {
                                "enforceFilters": true,
                                "dimension": "Section ID",
                                "ordering": [{
                                              "field": "Class ID",
                                              "order": "willfail"
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

    val result = DrilldownConfig.parse(reportingRequest.curatorJsonConfigMap("drilldown"))
    assert(result.toEither.left.get.head.toString.contains("order must be asc|desc not willfail"))
  }

  test("DrildownConfig with one request error should error") {
    val json : String =
      s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drilldown" : {
                              "config" : [{
                                "enforceFilters": true,
                                "dimension": "Section ID",
                                "ordering": [{
                                              "field": "Class ID",
                                              "order": "asc"
                                              }],
                                "mr": 1000
                              }
                              , {
                                "enforceFilters": "true",
                                "ordering": [{
                                              "field": "Class ID",
                                              "order": "desc"
                                              }],
                                "mr": 1000
                              }]
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
    require(reportingRequestResult.isSuccess, reportingRequestResult)
    val reportingRequest = reportingRequestResult.toOption.get

    val result = DrilldownConfig.parse(reportingRequest.curatorJsonConfigMap("drilldown"))
    assert(result.toEither.left.get.head.toString.contains("NoSuchFieldError(dimension,"))
  }

  test("DrillDownConfig should throw error on no dimension.") {
    val json : String =
      s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drilldown" : {
                              "config" : {
                                "enforceFilters": true,
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

    val result = DrilldownConfig.parse(reportingRequest.curatorJsonConfigMap("drilldown"))
    assert(result.toEither.left.get.head.toString.contains("NoSuchFieldError(dimension,"))
  }

  test("Create a valid DrillDownConfig with Descending order and multiple orderings") {
    val json : String =
      s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drilldown" : {
                              "config" : {
                                "dimension": "Section ID",
                                "ordering": [{
                                              "field": "Class ID",
                                              "order": "asc"
                                              },
                                              {
                                               "field": "Section ID",
                                               "order": "Desc"
                                              }]
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

    val drillDownConfig : DrilldownRequest = DrilldownConfig.parse(reportingRequest.curatorJsonConfigMap("drilldown")).toOption.get.requests.head


    assert(drillDownConfig.enforceFilters)
    assert(drillDownConfig.maxRows == 1000)
    assert(drillDownConfig.ordering.contains(SortBy("Section ID", DESC)))
    assert(drillDownConfig.dimensions == List(Field("Section ID", None, None)))
    assert(drillDownConfig.cube == "")
  }

  test("Create a valid DrillDownConfig with Descending order and no given ordering") {
    val json : String =
      s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drilldown" : {
                              "config" : {
                                "enforceFilters": false,
                                "dimensions": ["Day", "Remarks"],
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

    val drillDownConfig : DrilldownRequest = DrilldownConfig.parse(reportingRequest.curatorJsonConfigMap("drilldown")).toOption.get.requests.head


    assert(!drillDownConfig.enforceFilters)
    assert(drillDownConfig.maxRows == 1000)
    assert(drillDownConfig.dimensions == List(Field("Day", None, None), Field("Remarks", None, None)))
    assert(drillDownConfig.cube == "")
  }
}
