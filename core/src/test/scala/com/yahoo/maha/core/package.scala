// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha

import scala.util.Try
/**
 * Created by hiral on 11/23/15.
 */
package object core {
  implicit class PrintErrorMessage(tryAny: Try[Any]) {
    def errorMessage(s: String) : String = {
      tryAny match {
        case scala.util.Failure(t) =>
          import scala.compat.Platform.EOL

          s"$s - ${t.getMessage} \n ${t.getStackTrace().mkString("", EOL, EOL)}"
        case _ => s
      }
    }
    
    def checkFailureMessage(s: String) : Boolean = {
      tryAny match {
        case scala.util.Failure(t) =>
          t.getMessage.contains(s)
        case _ => false
      }
    }

  }

  import org.scalactic._
  val whiteSpaceNormalised: Uniformity[String] =
    new AbstractStringUniformity {
      /**Returns the string with all consecutive white spaces and other non printable chars reduced to a single space.*/
      def normalized(s: String): String = s.replaceAll("[\\s\\n\\t\\v\\a\\e]+", " ").trim
      override def toString: String = "whiteSpaceNormalised"
    }
}
