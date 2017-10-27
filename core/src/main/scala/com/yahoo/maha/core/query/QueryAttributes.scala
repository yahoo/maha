// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core.ValuesFilter
/**
 * Created by jians on 10/20/15.
 */

trait QueryAttribute

case class LongAttribute(value: Long) extends QueryAttribute {
  override def toString : String = {
    value.toString
  }
}

case class StringAttribute(value: String) extends QueryAttribute {
  override def toString : String = {
    value
  }
}
case class InjectedDimFilterAttribute(value: ValuesFilter) extends QueryAttribute

case class QueryAttributes(attributes: Map[String, QueryAttribute]) {
  def getAttribute(attribute: String): QueryAttribute = {
    require(attributes.contains(attribute), s"Attribute is not defined: $attribute")
    attributes(attribute)
  }

  def toBuilder: QueryAttributeBuilder = {
    new QueryAttributeBuilder(attributes)
  }

  def getAttributeOption(attribute: String): Option[QueryAttribute] = {
    attributes.get(attribute)
  }
}

object QueryAttributes {
  val empty = new QueryAttributes(Map.empty)
  val QueryStats: String = "query-stats"
  val injectedDimINFilter: String = "injected-dim-in-filter"
  val injectedDimNOTINFilter: String = "injected-dim-not-in-filter"
}

class QueryAttributeBuilder(private var attributes: Map[String, QueryAttribute] = Map.empty) {

  def addAttribute(attribute: String, value: QueryAttribute): QueryAttributeBuilder = {
    attributes += (attribute -> value)
    this
  }

  def build : QueryAttributes = {
    new QueryAttributes(attributes)
  }
}

case class QueryStatsAttribute(stats: EngineQueryStats) extends QueryAttribute
