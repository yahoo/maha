package com.yahoo.maha.service.calcite.avatica

import com.yahoo.maha.core._
import com.yahoo.maha.service.BaseMahaServiceTest
import com.yahoo.maha.service.error.{MahaServiceBadRequestException, MahaServiceExecutionException}
import org.apache.calcite.avatica.{AvaticaSeverity, SqlState}
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem
import org.apache.calcite.avatica.remote.{ProtobufTranslationImpl, Service}
import org.apache.calcite.sql.parser.SqlParseException
import org.scalatest.matchers.should.Matchers

class AvaticaMahaProtobufHandlerTest extends BaseMahaServiceTest with Matchers {

  val protobufTranslationImpl = new ProtobufTranslationImpl
  val avaticaMahaProtobufHandler = AvaticaMahaProtobufHandler(null, protobufTranslationImpl, NoopMetricsSystem.getInstance())

  test("test AvaticaMahaJsonHandler") {
    {
      val handlerGetResponse = avaticaMahaProtobufHandler.convertToErrorResponse(new MahaServiceExecutionException("Test execu exception")).getResponse
      val res = protobufTranslationImpl.parseResponse(handlerGetResponse)
      res.isInstanceOf[Service.ErrorResponse] should equal (true)

      val errorResponse = res.asInstanceOf[Service.ErrorResponse]
      errorResponse.errorMessage should equal ("Failed to execute the Request,Test execu exception") (after being whiteSpaceNormalised)
      errorResponse.errorCode should equal (500)
      errorResponse.severity should equal (AvaticaSeverity.FATAL)
      errorResponse.sqlState should equal ("DATA_EXCEPTION_NO_SUBCLASS")
    }
    {
      val handlerGetResponse = avaticaMahaProtobufHandler.convertToErrorResponse(new MahaServiceBadRequestException("Test execu exception", Some(new IllegalArgumentException("Inner exception")))).getResponse
      val res = protobufTranslationImpl.parseResponse(handlerGetResponse)
      res.isInstanceOf[Service.ErrorResponse] should equal (true)

      val errorResponse = res.asInstanceOf[Service.ErrorResponse]
      errorResponse.errorMessage should equal ("Bad SQL Request,Test execu exception,Inner exception") (after being whiteSpaceNormalised)
      errorResponse.errorCode should equal (400)
      errorResponse.severity should equal (AvaticaSeverity.ERROR)
      errorResponse.sqlState should equal ("INVALID_SQL_STATEMENT_IDENTIFIER_NO_SUBCLASS")
    }
    {
      val handlerGetResponse = avaticaMahaProtobufHandler.convertToErrorResponse(new IllegalArgumentException("Test execu exception", (new IllegalArgumentException("Inner exception")))).getResponse
      val res = protobufTranslationImpl.parseResponse(handlerGetResponse)
      res.isInstanceOf[Service.ErrorResponse] should equal (true)

      val errorResponse = res.asInstanceOf[Service.ErrorResponse]
      errorResponse.errorMessage should equal ("Bad SQL Request,Test execu exception,Inner exception") (after being whiteSpaceNormalised)
      errorResponse.errorCode should equal (400)
      errorResponse.severity should equal (AvaticaSeverity.ERROR)
      errorResponse.sqlState should equal ("INVALID_SQL_STATEMENT_IDENTIFIER_NO_SUBCLASS")
    }
    {
      val handlerGetResponse = avaticaMahaProtobufHandler.convertToErrorResponse(new java.util.NoSuchElementException("Test execu exception")).getResponse
      val res = protobufTranslationImpl.parseResponse(handlerGetResponse)
      res.isInstanceOf[Service.ErrorResponse] should equal (true)

      val errorResponse = res.asInstanceOf[Service.ErrorResponse]
      errorResponse.errorMessage should equal ("Bad SQL Request,Test execu exception,") (after being whiteSpaceNormalised)
      errorResponse.errorCode should equal (400)
      errorResponse.severity should equal (AvaticaSeverity.ERROR)
      errorResponse.sqlState should equal ("INVALID_SQL_STATEMENT_IDENTIFIER_NO_SUBCLASS")
    }
    {
      val handlerGetResponse = avaticaMahaProtobufHandler.convertToErrorResponse(new SqlParseException("Test execu exception", null, null, null, null)).getResponse
      val res = protobufTranslationImpl.parseResponse(handlerGetResponse)
      res.isInstanceOf[Service.ErrorResponse] should equal (true)

      val errorResponse = res.asInstanceOf[Service.ErrorResponse]
      errorResponse.errorMessage should equal ("Bad SQL Request,Test execu exception,") (after being whiteSpaceNormalised)
      errorResponse.errorCode should equal (400)
      errorResponse.severity should equal (AvaticaSeverity.ERROR)
      errorResponse.sqlState should equal ("INVALID_SQL_STATEMENT_IDENTIFIER_NO_SUBCLASS")
    }
    {
      val handlerGetResponse = avaticaMahaProtobufHandler.convertToErrorResponse(new NullPointerException("Test execu exception")).getResponse
      val res = protobufTranslationImpl.parseResponse(handlerGetResponse)
      res.isInstanceOf[Service.ErrorResponse] should equal (true)

      val errorResponse = res.asInstanceOf[Service.ErrorResponse]
      println(errorResponse.toString)
      errorResponse.errorMessage should equal ("Exception at executing query Test execu exception, ") (after being whiteSpaceNormalised)
      errorResponse.errorCode should equal (500)
      //potential bug in Avatica: AvaticaSeverity.UNKNOWN lost after deserializing(paring) serialized ErrorResponse objects
      //ticket: https://issues.apache.org/jira/browse/CALCITE-4604
      //errorResponse.severity should equal (AvaticaSeverity.UNKNOWN)
      errorResponse.sqlState should equal ("DATA_EXCEPTION_NO_SUBCLASS")
    }
  }
}
