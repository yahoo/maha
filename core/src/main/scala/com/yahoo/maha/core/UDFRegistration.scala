// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import scala.collection.mutable

/**
 * Created by shengyao on 1/6/16.
 */
trait UDFRegistration {
  val statement: String
}

trait UDFRegistrationFactory {
  val empty : Set[UDFRegistration] = Set()
  val defaultUDFStatements = new mutable.HashSet[UDFRegistration]()
  def register(udfRegistration: UDFRegistration): Unit = {
    defaultUDFStatements.add(udfRegistration)
  }
  def apply(): Set[UDFRegistration] = {
    defaultUDFStatements.toSet
  }
}

object DefaultUDFRegistrationFactory extends UDFRegistrationFactory

trait UDF extends UDFRegistration
trait UDAF extends UDFRegistration