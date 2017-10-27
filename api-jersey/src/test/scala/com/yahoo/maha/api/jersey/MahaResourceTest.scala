// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.api.jersey

import javax.ws.rs.core.MediaType
import example.ExampleMahaService
import junit.framework.TestCase.assertNotNull
import org.apache.http.{HttpEntity, HttpHeaders, HttpResponse}
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.util.EntityUtils
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.webapp.WebAppContext
import org.junit.Assert.assertEquals
import org.junit._

class MahaResourceTest {

  @Test
  def successfulDomainEndpoint(){
    assertNotNull("jetty must be initialised", MahaResourceTest.server)
    val httpClient: CloseableHttpClient = HttpClientBuilder.create().build()
    val httpGet : HttpGet = new HttpGet("http://localhost:7875/appName/registry/er/domain")
    val httpResponse: HttpResponse = httpClient.execute(httpGet)
    assertEquals("should return status 200", 200, httpResponse.getStatusLine.getStatusCode)
    val domainJson: String = EntityUtils.toString(httpResponse.getEntity)
    assert(domainJson.contains("""{"dimensions":[{"name":"student","fields":["Student ID","Student Name","Student Status"]}],"schemas":{"student":["student_performance"]},"cubes":[{"name":"student_performance","mainEntityIds":{"student":"Student ID"},"maxDaysLookBack":[{"requestType":"SyncRequest","grain":"DailyGrain","days":30},{"requestType":"AsyncRequest","grain":"DailyGrain","days":30}],"maxDaysWindow":[{"requestType":"SyncRequest","grain":"DailyGrain","days":20},{"requestType":"AsyncRequest","grain":"DailyGrain","days":20},{"requestType":"SyncRequest","grain":"HourlyGrain","days":20},{"requestType":"AsyncRequest","grain":"HourlyGrain","days":20}],"fields":[{"field":"Class ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Day","type":"Dimension","dataType":{"type":"Date","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Remarks","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","=","LIKE"],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Section ID","type":"Dimension","dataType":{"type":"Number","constraint":"3"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Student ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"student","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Year","type":"Dimension","dataType":{"type":"Enum","constraint":"Freshman|Junior|Sophomore|Senior"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Marks Obtained","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null},{"field":"Performance Factor","type":"Fact","dataType":{"type":"Number","constraint":"10"},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null},{"field":"Total Marks","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null}]}]}"""))
  }

  @Test
  def testDomainEndpointWithInvalidRegistry(){
    assertNotNull("jetty must be initialised", MahaResourceTest.server)
    val httpClient: CloseableHttpClient = HttpClientBuilder.create().build()
    val httpGet : HttpGet = new HttpGet("http://localhost:7875/appName/registry/dummy/domain")
    httpGet.addHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON)
    val httpResponse: HttpResponse = httpClient.execute(httpGet)
    assertEquals("should return status 400", 400, httpResponse.getStatusLine.getStatusCode)
    val domainJson: String = EntityUtils.toString(httpResponse.getEntity)
    println(domainJson)
    assert(domainJson.contains("""{"errorMsg":"registry dummy not found"}"""))
  }

  @Test
  def successfulDomainForCubeEndpoint(){
    assertNotNull("jetty must be initialised", MahaResourceTest.server)
    val httpClient: CloseableHttpClient = HttpClientBuilder.create().build()
    val httpGet : HttpGet = new HttpGet("http://localhost:7875/appName/registry/er/domain/cubes/student_performance")
    val httpResponse: HttpResponse = httpClient.execute(httpGet)
    assertEquals("should return status 200", 200, httpResponse.getStatusLine.getStatusCode)
    val domainJson: String = EntityUtils.toString(httpResponse.getEntity)
    assert(domainJson.contains("""{"name":"student_performance","mainEntityIds":{"student":"Student ID"},"maxDaysLookBack":[{"requestType":"SyncRequest","grain":"DailyGrain","days":30},{"requestType":"AsyncRequest","grain":"DailyGrain","days":30}],"maxDaysWindow":[{"requestType":"SyncRequest","grain":"DailyGrain","days":20},{"requestType":"AsyncRequest","grain":"DailyGrain","days":20},{"requestType":"SyncRequest","grain":"HourlyGrain","days":20},{"requestType":"AsyncRequest","grain":"HourlyGrain","days":20}],"fields":[{"field":"Class ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Day","type":"Dimension","dataType":{"type":"Date","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Remarks","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","=","LIKE"],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Section ID","type":"Dimension","dataType":{"type":"Number","constraint":"3"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Student ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"student","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Year","type":"Dimension","dataType":{"type":"Enum","constraint":"Freshman|Junior|Sophomore|Senior"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Marks Obtained","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null},{"field":"Performance Factor","type":"Fact","dataType":{"type":"Number","constraint":"10"},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null},{"field":"Total Marks","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null}]}"""))
  }

  @Test
  def testDomainForCubeEndpointWithInvalidRegistry(){
    assertNotNull("jetty must be initialised", MahaResourceTest.server)
    val httpClient: CloseableHttpClient = HttpClientBuilder.create().build()
    val httpGet : HttpGet = new HttpGet("http://localhost:7875/appName/registry/dummy/domain/cubes/student_performance")
    val httpResponse: HttpResponse = httpClient.execute(httpGet)
    assertEquals("should return status 400", 400, httpResponse.getStatusLine.getStatusCode)
    val domainJson: String = EntityUtils.toString(httpResponse.getEntity)
    assert(domainJson.contains("""{"errorMsg":"registry dummy and cube student_performance not found"}"""))
  }

  @Test
  def testDomainForCubeEndpointWithInvalidCube(){
    assertNotNull("jetty must be initialised", MahaResourceTest.server)
    val httpClient: CloseableHttpClient = HttpClientBuilder.create().build()
    val httpGet : HttpGet = new HttpGet("http://localhost:7875/appName/registry/er/domain/cubes/dummy")
    val httpResponse: HttpResponse = httpClient.execute(httpGet)
//    assertEquals("should return status 400", 400, httpResponse.getStatusLine.getStatusCode)
    val domainJson: String = EntityUtils.toString(httpResponse.getEntity)
//    assert(domainJson.contains("""{"errorMsg":"registry er and cube dummy not found"}"""))
  }


  @Test
  def successfulFlattenDomainEndpoint(){
    assertNotNull("jetty must be initialised", MahaResourceTest.server)
    val httpClient: CloseableHttpClient = HttpClientBuilder.create().build()
    val httpGet : HttpGet = new HttpGet("http://localhost:7875/appName/registry/er/flattenDomain")
    val httpResponse: HttpResponse = httpClient.execute(httpGet)
    assertEquals("should return status 200", 200, httpResponse.getStatusLine.getStatusCode)
    val flattenDomainJson: String = EntityUtils.toString(httpResponse.getEntity)
    assert(flattenDomainJson.contains("""{"dimensions":[{"name":"student","fields":["Student ID","Student Name","Student Status"]}],"schemas":{"student":["student_performance"]},"cubes":[{"name":"student_performance","mainEntityIds":{"student":"Student ID"},"maxDaysLookBack":[{"requestType":"SyncRequest","grain":"DailyGrain","days":30},{"requestType":"AsyncRequest","grain":"DailyGrain","days":30}],"maxDaysWindow":[{"requestType":"SyncRequest","grain":"DailyGrain","days":20},{"requestType":"AsyncRequest","grain":"DailyGrain","days":20},{"requestType":"SyncRequest","grain":"HourlyGrain","days":20},{"requestType":"AsyncRequest","grain":"HourlyGrain","days":20}],"fields":[{"field":"Class ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false},{"field":"Day","type":"Dimension","dataType":{"type":"Date","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false},{"field":"Remarks","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","=","LIKE"],"required":false,"filteringRequired":false},{"field":"Section ID","type":"Dimension","dataType":{"type":"Number","constraint":"3"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false},{"field":"Student ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"student","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false},{"field":"Year","type":"Dimension","dataType":{"type":"Enum","constraint":"Freshman|Junior|Sophomore|Senior"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false},{"field":"Marks Obtained","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup"},{"field":"Performance Factor","type":"Fact","dataType":{"type":"Number","constraint":"10"},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup"},{"field":"Total Marks","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup"},{"field":"Student Name","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":"student","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false},{"field":"Admitted Year","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"student","filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false},{"field":"Student Status","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":"student","filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false}]}]}"""))
  }

  @Test
  def testFlattenDomainEndpointWithInvalidRegistry(){
    assertNotNull("jetty must be initialised", MahaResourceTest.server)
    val httpClient: CloseableHttpClient = HttpClientBuilder.create().build()
    val httpGet : HttpGet = new HttpGet("http://localhost:7875/appName/registry/dummy/flattenDomain")
    val httpResponse: HttpResponse = httpClient.execute(httpGet)
    assertEquals("should return status 400", 400, httpResponse.getStatusLine.getStatusCode)
    val flattenDomainJson: String = EntityUtils.toString(httpResponse.getEntity)
    assert(flattenDomainJson.contains("""{"errorMsg":"registry dummy not found"}"""))
  }

  @Test
  def successfulFlattenDomainForCubeEndpoint(){
    assertNotNull("jetty must be initialised", MahaResourceTest.server)
    val httpClient: CloseableHttpClient = HttpClientBuilder.create().build()
    val httpGet : HttpGet = new HttpGet("http://localhost:7875/appName/registry/er/flattenDomain/cubes/student_performance")
    val httpResponse: HttpResponse = httpClient.execute(httpGet)
    assertEquals("should return status 200", 200, httpResponse.getStatusLine.getStatusCode)
    val flattenDomainJson: String = EntityUtils.toString(httpResponse.getEntity)
    assert(flattenDomainJson.contains("""{"name":"student_performance","mainEntityIds":{"student":"Student ID"},"maxDaysLookBack":[{"requestType":"SyncRequest","grain":"DailyGrain","days":30},{"requestType":"AsyncRequest","grain":"DailyGrain","days":30}],"maxDaysWindow":[{"requestType":"SyncRequest","grain":"DailyGrain","days":20},{"requestType":"AsyncRequest","grain":"DailyGrain","days":20},{"requestType":"SyncRequest","grain":"HourlyGrain","days":20},{"requestType":"AsyncRequest","grain":"HourlyGrain","days":20}],"fields":[{"field":"Class ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false},{"field":"Day","type":"Dimension","dataType":{"type":"Date","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false},{"field":"Remarks","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","=","LIKE"],"required":false,"filteringRequired":false},{"field":"Section ID","type":"Dimension","dataType":{"type":"Number","constraint":"3"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false},{"field":"Student ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"student","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false},{"field":"Year","type":"Dimension","dataType":{"type":"Enum","constraint":"Freshman|Junior|Sophomore|Senior"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false},{"field":"Marks Obtained","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup"},{"field":"Performance Factor","type":"Fact","dataType":{"type":"Number","constraint":"10"},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup"},{"field":"Total Marks","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup"},{"field":"Student Name","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":"student","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false},{"field":"Admitted Year","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"student","filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false},{"field":"Student Status","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":"student","filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false}]}"""))
  }

  @Test
  def successfulFlattenDomainForCubeEndpointWithRevision(){
    assertNotNull("jetty must be initialised", MahaResourceTest.server)
    val httpClient: CloseableHttpClient = HttpClientBuilder.create().build()
    val httpGet : HttpGet = new HttpGet("http://localhost:7875/appName/registry/er/flattenDomain/cubes/student_performance/0")
    val httpResponse: HttpResponse = httpClient.execute(httpGet)
    assertEquals("should return status 200", 200, httpResponse.getStatusLine.getStatusCode)
    val flattenDomainJson: String = EntityUtils.toString(httpResponse.getEntity)
    assert(flattenDomainJson.contains("""{"name":"student_performance","mainEntityIds":{"student":"Student ID"},"maxDaysLookBack":[{"requestType":"SyncRequest","grain":"DailyGrain","days":30},{"requestType":"AsyncRequest","grain":"DailyGrain","days":30}],"maxDaysWindow":[{"requestType":"SyncRequest","grain":"DailyGrain","days":20},{"requestType":"AsyncRequest","grain":"DailyGrain","days":20},{"requestType":"SyncRequest","grain":"HourlyGrain","days":20},{"requestType":"AsyncRequest","grain":"HourlyGrain","days":20}],"fields":[{"field":"Class ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false},{"field":"Day","type":"Dimension","dataType":{"type":"Date","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false},{"field":"Remarks","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","=","LIKE"],"required":false,"filteringRequired":false},{"field":"Section ID","type":"Dimension","dataType":{"type":"Number","constraint":"3"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false},{"field":"Student ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"student","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false},{"field":"Year","type":"Dimension","dataType":{"type":"Enum","constraint":"Freshman|Junior|Sophomore|Senior"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false},{"field":"Marks Obtained","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup"},{"field":"Performance Factor","type":"Fact","dataType":{"type":"Number","constraint":"10"},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup"},{"field":"Total Marks","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup"},{"field":"Student Name","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":"student","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false},{"field":"Admitted Year","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"student","filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false},{"field":"Student Status","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":"student","filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false}]}"""))
  }

  @Test
  def testFlattenDomainForCubeEndpointWithInvalidRegistry(){
    assertNotNull("jetty must be initialised", MahaResourceTest.server)
    val httpClient: CloseableHttpClient = HttpClientBuilder.create().build()
    val httpGet : HttpGet = new HttpGet("http://localhost:7875/appName/registry/dummy/flattenDomain/cubes/student_performance")
    val httpResponse: HttpResponse = httpClient.execute(httpGet)
    assertEquals("should return status 400", 400, httpResponse.getStatusLine.getStatusCode)
    val flattenDomainJson: String = EntityUtils.toString(httpResponse.getEntity)
    println(flattenDomainJson)
    assert(flattenDomainJson.contains("""{"errorMsg":"registry dummy and cube student_performance not found"}"""))
  }

  @Test
  def testFlattenDomainForCubeEndpointWithInvalidCube(){
    assertNotNull("jetty must be initialised", MahaResourceTest.server)
    val httpClient: CloseableHttpClient = HttpClientBuilder.create().build()
    val httpGet : HttpGet = new HttpGet("http://localhost:7875/appName/registry/er/flattenDomain/cubes/blah")
    val httpResponse: HttpResponse = httpClient.execute(httpGet)
    assertEquals("should return status 400", 400, httpResponse.getStatusLine.getStatusCode)
    val flattenDomainJson: String = EntityUtils.toString(httpResponse.getEntity)
    println(flattenDomainJson)
//    assert(flattenDomainJson.contains("""{"errorMsg":"registry er and cube blah not found"}"""))
  }

  @Test
  def testFlattenDomainForCubeEndpointWithInvalidRevision(){
    assertNotNull("jetty must be initialised", MahaResourceTest.server)
    val httpClient: CloseableHttpClient = HttpClientBuilder.create().build()
    val httpGet : HttpGet = new HttpGet("http://localhost:7875/appName/registry/er/flattenDomain/cubes/student_performance/1")
    val httpResponse: HttpResponse = httpClient.execute(httpGet)
    assertEquals("should return status 400", 400, httpResponse.getStatusLine.getStatusCode)
    val flattenDomainJson: String = EntityUtils.toString(httpResponse.getEntity)
    println(flattenDomainJson)
    //    assert(flattenDomainJson.contains("""{"errorMsg":"registry er and cube blah not found"}"""))
  }

  @Test
  def successfulSyncRequest(){
    assertNotNull("jetty must be initialised", MahaResourceTest.server)
    val httpClient: CloseableHttpClient = HttpClientBuilder.create().build()
    val httpPost: HttpPost = new HttpPost("http://localhost:7875/appName/registry/er/schemas/student/query")
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
    val httpEntity: HttpEntity = new StringEntity(jsonRequest)
    httpPost.setEntity(httpEntity)
    httpPost.setHeader(HttpHeaders.ACCEPT,MediaType.APPLICATION_JSON)
    httpPost.setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
    httpPost.setHeader("RequestId", "successfulSyncRequest")
    val httpResponse: HttpResponse = httpClient.execute(httpPost)
    assertEquals(s"should return status 200, ${httpResponse.getStatusLine}", 200, httpResponse.getStatusLine.getStatusCode)
    val responseJson: String = EntityUtils.toString(httpResponse.getEntity)
    println(responseJson)
    assert(responseJson.contains("""{"header":{"cube":"student_performance","fields":[{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Class ID","fieldType":"DIM"},{"fieldName":"Section ID","fieldType":"FACT"},{"fieldName":"Total Marks","fieldType":"FACT"}],"maxRows":200},"rows":[[213,200,100,125]]}"""))
  }

  @Test
  def testInvalidSchemaSyncRequest(){
    assertNotNull("jetty must be initialised", MahaResourceTest.server)
    val httpClient: CloseableHttpClient = HttpClientBuilder.create().build()
    val httpPost: HttpPost = new HttpPost("http://localhost:7875/appName/registry/er/schemas/blah/query")
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
    val httpEntity: HttpEntity = new StringEntity(jsonRequest)
    httpPost.setEntity(httpEntity)
    httpPost.setHeader(HttpHeaders.ACCEPT,MediaType.APPLICATION_JSON)
    httpPost.setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
    val httpResponse: HttpResponse = httpClient.execute(httpPost)
    assertEquals("should return status 400", 400, httpResponse.getStatusLine.getStatusCode)
    val responseJson: String = EntityUtils.toString(httpResponse.getEntity)
    println(responseJson)
    assert(responseJson.contains("""{"errorMsg":"schema blah not found"}"""))
  }

  @Test
  def testInvalidCubeRequestSyncRequest(){
    assertNotNull("jetty must be initialised", MahaResourceTest.server)
    val httpClient: CloseableHttpClient = HttpClientBuilder.create().build()
    val httpPost: HttpPost = new HttpPost("http://localhost:7875/appName/registry/er/schemas/student/query")
    val jsonRequest = s"""{
                          "cube": "blah_performance",
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
    val httpEntity: HttpEntity = new StringEntity(jsonRequest)
    httpPost.setEntity(httpEntity)
    httpPost.setHeader(HttpHeaders.ACCEPT,MediaType.APPLICATION_JSON)
    httpPost.setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
    val httpResponse: HttpResponse = httpClient.execute(httpPost)
    assertEquals("should return status 400", 400, httpResponse.getStatusLine.getStatusCode)
    val responseJson: String = EntityUtils.toString(httpResponse.getEntity)
    println(responseJson)
    assert(responseJson.contains("""{"errorMsg":"cube does not exist : blah_performance"}"""))
  }

  @Test
  def testInvalidRequestSyncRequest(){
    assertNotNull("jetty must be initialised", MahaResourceTest.server)
    val httpClient: CloseableHttpClient = HttpClientBuilder.create().build()
    val httpPost: HttpPost = new HttpPost("http://localhost:7875/appName/registry/er/schemas/student/query")
    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Class ID"},
                            {"field": "Section ID"},
                            {"field": "Total Marks"}
                          ],
                          "filterExpressions": [
                            {"field": "Student ID", "operator": "=", "value": "213"}
                          ]
                        }"""
    val httpEntity: HttpEntity = new StringEntity(jsonRequest)
    httpPost.setEntity(httpEntity)
    httpPost.setHeader(HttpHeaders.ACCEPT,MediaType.APPLICATION_JSON)
    httpPost.setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
    val httpResponse: HttpResponse = httpClient.execute(httpPost)
    assertEquals("should return status 400", 400, httpResponse.getStatusLine.getStatusCode)
    val responseJson: String = EntityUtils.toString(httpResponse.getEntity)
    println(responseJson)
    assert(responseJson.contains("""{"errorMsg":"requirement failed: Failure(NonEmpty[UncategorizedError(Day,requirement failed: Day filter not found in list of filters!,List())])"}"""))
  }

  @Test
  def testMaxWindowExceededError(){
    assertNotNull("jetty must be initialised", MahaResourceTest.server)
    val httpClient: CloseableHttpClient = HttpClientBuilder.create().build()
    val httpPost: HttpPost = new HttpPost("http://localhost:7875/appName/registry/er/schemas/student/query")
    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Class ID"},
                            {"field": "Section ID"},
                            {"field": "Total Marks"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "2016-08-14", "to": "${ExampleMahaService.today}"},
                            {"field": "Student ID", "operator": "=", "value": "213"}
                          ]
                        }"""
    val httpEntity: HttpEntity = new StringEntity(jsonRequest)
    httpPost.setEntity(httpEntity)
    httpPost.setHeader(HttpHeaders.ACCEPT,MediaType.APPLICATION_JSON)
    httpPost.setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
    val httpResponse: HttpResponse = httpClient.execute(httpPost)
    assertEquals("should return status 400", 400, httpResponse.getStatusLine.getStatusCode)
    val responseJson: String = EntityUtils.toString(httpResponse.getEntity)
    println(responseJson)
    assert(responseJson.contains("""{"errorMsg":"requirement failed: ERROR_CODE:10001 Max days window exceeded"""))
  }
}

object MahaResourceTest {

  var server: Server = null

  @BeforeClass
  def startJetty() {
    server = new Server(7875)
    server.setStopAtShutdown(true)
    val webAppContext : WebAppContext = new WebAppContext()
    webAppContext.setContextPath("/appName")
    webAppContext.setResourceBase("src/test/webapp")
    webAppContext.setClassLoader(getClass().getClassLoader())
    server.setHandler(webAppContext)
    server.start()
  }

  @AfterClass
  def stopJetty(){
    server.stop()
  }

}
