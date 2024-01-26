package com.yahoo.maha.api.example

import com.yahoo.maha.service.calcite.avatica.MahaAvaticaService
import jakarta.ws.rs.core.MediaType
import com.yahoo.maha.service.utils.MahaConstants
import junit.framework.TestCase.assertNotNull
import org.apache.calcite.avatica.proto.Common.WireMessage
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.{ByteArrayEntity, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpEntity, HttpHeaders, HttpResponse}
import org.apache.log4j.MDC
import org.eclipse.jetty.ee10.webapp.WebAppContext
import org.eclipse.jetty.server.Server
import org.junit.Assert.assertEquals
import org.junit._
class ExampleResourceTest {
  @Test
  def successfulCubesEndpoint() {
    assertNotNull("jetty must be initialised", ExampleResourceTest.server)
    val httpClient: CloseableHttpClient = HttpClientBuilder.create().build()
    val httpGet: HttpGet = new HttpGet("http://localhost:7875/appName/registry/student/cubes")
    val httpResponse: HttpResponse = httpClient.execute(httpGet)
    assertEquals("should return status 200", 200, httpResponse.getStatusLine.getStatusCode)
    val cubesJson: String = EntityUtils.toString(httpResponse.getEntity)
    assert(cubesJson.equals("""["student_performance"]"""))

  }

  @Test
  def successfulDomainEndpoint() {
    assertNotNull("jetty must be initialised", ExampleResourceTest.server)
    val httpClient: CloseableHttpClient = HttpClientBuilder.create().build()
    val httpGet: HttpGet = new HttpGet("http://localhost:7875/appName/registry/student/domain")
    val httpResponse: HttpResponse = httpClient.execute(httpGet)
    assertEquals("should return status 200", 200, httpResponse.getStatusLine.getStatusCode)
    val domainJson: String = EntityUtils.toString(httpResponse.getEntity)
    val expected: String = """{"dimensions":[{"name":"student","fields":["Student ID","Student Name","Student Status"],"fieldsWithSchemas":[{"name":"Student ID","allowedSchemas":[]},{"name":"Student Name","allowedSchemas":[]},{"name":"Student Status","allowedSchemas":[]}]}],"schemas":{"student":["student_performance"]},"cubes":[{"name":"student_performance","mainEntityIds":{"student":"Student ID"},"maxDaysLookBack":[{"requestType":"SyncRequest","grain":"DailyGrain","days":9999},{"requestType":"AsyncRequest","grain":"DailyGrain","days":9999}],"maxDaysWindow":[{"requestType":"SyncRequest","grain":"DailyGrain","days":20},{"requestType":"AsyncRequest","grain":"DailyGrain","days":20},{"requestType":"SyncRequest","grain":"HourlyGrain","days":20},{"requestType":"AsyncRequest","grain":"HourlyGrain","days":20}],"fields":[{"field":"Class ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","NOT IN","=","<>"],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Day","type":"Dimension","dataType":{"type":"Date","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","<>","=","<","BETWEEN",">","NOT IN"],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Remarks","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","=","LIKE"],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Section ID","type":"Dimension","dataType":{"type":"Number","constraint":"3"},"dimensionName":null,"filterable":true,"filterOperations":["IN","NOT IN","=","<>"],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Student ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"student","filterable":true,"filterOperations":["IN","NOT IN","=","<>"],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Year","type":"Dimension","dataType":{"type":"Enum","constraint":"Freshman|Junior|Senior|Sophomore"},"dimensionName":null,"filterable":true,"filterOperations":["IN","<>","=","<","BETWEEN",">","NOT IN"],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Marks Obtained","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","<>","=","<","BETWEEN",">","NOT IN"],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null,"allowedSchemas":null},{"field":"Performance Factor","type":"Fact","dataType":{"type":"Number","constraint":"10"},"dimensionName":null,"filterable":true,"filterOperations":["IN","<>","=","<","BETWEEN",">","NOT IN"],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null,"allowedSchemas":null},{"field":"Total Marks","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","<>","=","<","BETWEEN",">","NOT IN"],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null,"allowedSchemas":null}]}]}"""
    println(domainJson)
    println(expected)
    assert(domainJson.contains(expected))
  }
}

object ExampleResourceTest {
  var server: Server = null

  @BeforeClass
  def startJetty() {
    // user params for kafka logging and bucketing
    MDC.put(MahaConstants.REQUEST_ID, "123Request")
    MDC.put(MahaConstants.USER_ID, "abc")

    server = new Server(7875)
    server.setStopAtShutdown(true)
    val webAppContext: WebAppContext = new WebAppContext()
    webAppContext.setContextPath("/appName")
    webAppContext.setBaseResourceAsString("src/main/webapp")
    webAppContext.setClassLoader(getClass().getClassLoader())
    server.setHandler(webAppContext)
    server.start()
  }

  @AfterClass
  def stopJetty() {
    server.stop()
  }

}