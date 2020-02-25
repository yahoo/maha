// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import org.json4s._
import org.json4s.scalaz.JsonScalaz
import org.json4s.scalaz.JsonScalaz._
import _root_.scalaz.{Scalaz, syntax}
import org.json4s.JsonAST.{JArray, JValue}
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

  /**
   * Implicits used for JSON converters, IE Set, Map, Annotations, etc.
   */
  /*implicit def setJSONW: JSONW[Set[String]] = new JSONW[Set[String]] {
    def write(values: Set[String]) = JArray(values.map(x => toJSON(x)).toList)
  }*/

  implicit def mapJSONW: JSONW[Map[String, Set[String]]] = new JSONW[Map[String, Set[String]]] {
    def write(values: Map[String, Set[String]]) = makeObj(values.map(kv => kv._1 -> toJSON(kv._2.toList)).toList)
  }

  implicit def bdJSONW: JSONW[BigDecimal] = new JSONW[BigDecimal] {
    def write(value: BigDecimal): JValue = makeObj(List(("value" -> toJSON(value.doubleValue()))))
  }

  implicit def toStringJSONW[A]: JSONW[A] = new JSONW[A] {
    def write(value: A): JValue = value match {
      case a: ColumnAnnotation =>
        a.asJSON
      case null =>
        JNull
      case _ =>
        toJSON(value.toString)
    }
  }

  implicit def setJSONW[A]: JSONW[Set[A]] = new JSONW[Set[A]] {
    def write(values: Set[A]) = JArray(values.map(x => toJSON(x)).toList)
  }

  implicit def listJSONW[A: JSONW]: JSONW[List[A]] = new JSONW[List[A]] {
    def write(values: List[A]) = JArray(values.map(x => implicitly[JSONW[A]].write(x)))
  }

  def asJSON[A](a: A): JValue = {
    toJSON(a)
  }

}
