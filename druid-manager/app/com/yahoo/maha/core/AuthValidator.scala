package com.yahoo.maha.core.auth

import play.api.Configuration
import play.api.mvc.{RequestHeader, Result, Results}

case class ValidationResult(success: Boolean, user: Option[String])

trait AuthValidator {
  def init(configuration: Configuration)
  def validate(requestHeader: RequestHeader) : ValidationResult
  def handleAuthCallback(requestHeader: RequestHeader) : Result
  def handleAuthFailure(requestHeader: RequestHeader) : Result
}

class DefaultAuthValidator extends AuthValidator {
  override def init(configuration: Configuration): Unit = {
  }

  override def validate(requestHeader: RequestHeader): ValidationResult = {
    ValidationResult(success = true, user = None)
  }

  override def handleAuthCallback(requestHeader: RequestHeader): Result = {
    Results.Ok
  }

  override def handleAuthFailure(requestHeader: RequestHeader): Result = {
    Results.Ok
  }
}

trait DruidAuthHeaderProvider {
  def init(configuration: Configuration)
  def getAuthHeaders : Map[String, String]
}

class DefaultDruidAuthHeaderProvider extends DruidAuthHeaderProvider {
  override def init(configuration: Configuration): Unit = {
  }

  override def getAuthHeaders: Map[String, String] = {
    Map.empty
  }
}
