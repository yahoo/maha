// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import org.json4s._
import org.json4s.scalaz.JsonScalaz
import org.json4s.scalaz.JsonScalaz._

import _root_.scalaz.{Scalaz, syntax}
import syntax.validation._

/**
 * Created by hiral on 2/25/16.
 */
object JsonUtils {
  def string(json: JValue): Result[String] = json match {
    case JString(s) => s.successNel
    case JLong(l) => l.toString.successNel
    case JInt(num) => num.toString().successNel
    case JDouble(num) => num.toString.successNel
    case JDecimal(num) => num.toString().successNel
    case JBool(b) => b.toString.successNel
    case x => UnexpectedJSONError(x, classOf[JString]).asInstanceOf[JsonScalaz.Error].failureNel[String]
  }

  def stringField(name: String)(json: JValue): Result[String] = json match {
    case JObject(fs) =>
      fs.find(_._1 == name)
        .map { case (_, jvalue) => string(jvalue) } .getOrElse(NoSuchFieldError(name, json).asInstanceOf[JsonScalaz.Error].failureNel)
    case x => UnexpectedJSONError(x, classOf[JObject]).asInstanceOf[JsonScalaz.Error].failureNel
  }
  
  import Scalaz._
  def stringListField(name: String)(json: JValue): Result[List[String]] = json match {
    case JObject(fs) =>
      fs.find(_._1 == name)
        .map {
        case (_, JArray(s)) =>
          s.collect {
            case jvalue if jvalue != JNull => string(jvalue)
          }.sequence[Result, String]
        case (_, x) => UnexpectedJSONError(x, classOf[JArray]).asInstanceOf[JsonScalaz.Error].failureNel
      }.getOrElse(NoSuchFieldError(name, json).asInstanceOf[JsonScalaz.Error].failureNel)
    case x => UnexpectedJSONError(x, classOf[JObject]).asInstanceOf[JsonScalaz.Error].failureNel
  }

  def booleanFalse(json: JValue): Result[Boolean] = false.successNel
}
