package com.yahoo.maha.service.calcite.avatica

import com.yahoo.maha.service.BaseMahaServiceTest
import com.yahoo.maha.service.error.{MahaServiceBadRequestException, MahaServiceExecutionException}
import com.yahoo.maha.core._
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem
import org.apache.calcite.sql.parser.SqlParseException
import org.scalatest.matchers.should.Matchers

class AvaticaMahaJsonHandlerTest extends BaseMahaServiceTest with Matchers {

  val requestList = Map(
    s""""""-> ""
  )
  val avaticaMahaJsonHandler = new AvaticaMahaJsonHandler(null, NoopMetricsSystem.getInstance())

  test("test AvaticaMahaJsonHandler") {
    {
      val res = avaticaMahaJsonHandler.convertToErrorResponse(new MahaServiceExecutionException("Test execu exception")).getResponse
      val expected = """{"response":"error","exceptions":[],"errorMessage":"Failed to execute the Request,Test execu exception  ","errorCode":500,"sqlState":"DATA_EXCEPTION_NO_SUBCLASS","severity":"FATAL","rpcMetadata":null}"""
      res should equal (expected) (after being whiteSpaceNormalised)
    }
    {
      val res = avaticaMahaJsonHandler.convertToErrorResponse(new MahaServiceBadRequestException("Test execu exception", Some(new IllegalArgumentException("Inner exception")))).getResponse
      val expected = """{"response":"error","exceptions":[],"errorMessage":"Bad SQL Request,Test execu exception,Inner exception ","errorCode":400,"sqlState":"INVALID_SQL_STATEMENT_IDENTIFIER_NO_SUBCLASS","severity":"ERROR","rpcMetadata":null}"""
      res should equal (expected) (after being whiteSpaceNormalised)
    }
    {
      val res = avaticaMahaJsonHandler.convertToErrorResponse(new IllegalArgumentException("Test execu exception", (new IllegalArgumentException("Inner exception")))).getResponse
      val expected = """{"response":"error","exceptions":[],"errorMessage":"Bad SQL Request,Test execu exception,Inner exception ","errorCode":400,"sqlState":"INVALID_SQL_STATEMENT_IDENTIFIER_NO_SUBCLASS","severity":"ERROR","rpcMetadata":null}"""
      res should equal (expected) (after being whiteSpaceNormalised)
    }
    {
      val res = avaticaMahaJsonHandler.convertToErrorResponse(new java.util.NoSuchElementException("Test execu exception")).getResponse
      val expected = """{"response":"error","exceptions":[],"errorMessage":"Bad SQL Request,Test execu exception, ","errorCode":400,"sqlState":"INVALID_SQL_STATEMENT_IDENTIFIER_NO_SUBCLASS","severity":"ERROR","rpcMetadata":null}"""
      res should equal (expected) (after being whiteSpaceNormalised)
    }
    {
      val res = avaticaMahaJsonHandler.convertToErrorResponse(new SqlParseException("Test execu exception", null, null, null, null)).getResponse
      val expected = """{"response":"error","exceptions":[],"errorMessage":"Bad SQL Request,Test execu exception, ","errorCode":400,"sqlState":"INVALID_SQL_STATEMENT_IDENTIFIER_NO_SUBCLASS","severity":"ERROR","rpcMetadata":null}"""
      res should equal (expected) (after being whiteSpaceNormalised)
    }
    {
      val res = avaticaMahaJsonHandler.convertToErrorResponse(new NullPointerException("Test execu exception")).getResponse
      val expected = """{"response":"error","exceptions":[],"errorMessage":"Exception at executing query Test execu exception, ","errorCode":500,"sqlState":"DATA_EXCEPTION_NO_SUBCLASS","severity":"UNKNOWN","rpcMetadata":null}"""
      res should equal (expected) (after being whiteSpaceNormalised)
    }
  }
}
