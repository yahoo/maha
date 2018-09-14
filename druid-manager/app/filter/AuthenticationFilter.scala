// Copyright 2018, Oath Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package filter
import akka.stream.Materializer
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import com.yahoo.maha.core.auth.{AuthValidator, ValidationResult}
import play.api.Logger
import play.api.mvc._

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class AuthenticationFilter(authValidator: AuthValidator)(implicit val mat: Materializer) extends Filter {

  private val routesWhichRequireAuth : Set[String] = Set("/segments", "/overlord/workers", "/lookups", "/kill/segments")

  def apply(nextFilter: RequestHeader => Future[Result])
           (requestHeader: RequestHeader): Future[Result] = {

    if(routesWhichRequireAuth.contains(requestHeader.path)) {
      Try {
        val result: ValidationResult = authValidator.validate(requestHeader)
        result
      } match {
        case Success(result) =>
          val requestHeaderWithId = requestHeader.copy(tags = requestHeader.tags + ("X-Request-Id" -> generateRequestId(requestHeader))
            + ("userId" -> result.user.getOrElse("Authorized User")))
          nextFilter(requestHeaderWithId)
        case Failure(e) =>
          Logger.error(s"Exception while authenticating user", e)
          val result: Result = authValidator.handleAuthFailure(requestHeader)
          Future.successful(result)
      }
    } else {
      Logger.debug(s"no auth required for path : ${requestHeader.path}")
      nextFilter(requestHeader)
    }
  }

  private def generateRequestId(requestHeader: RequestHeader): String = {
    return s" ${Hashing.goodFastHash(128).newHasher.putString(requestHeader.path + requestHeader.queryString, Charsets.UTF_8).hash.asLong}-${System.nanoTime}"
  }

}