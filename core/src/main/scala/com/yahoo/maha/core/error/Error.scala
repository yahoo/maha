// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.error

import com.yahoo.maha.core.Grain
import com.yahoo.maha.core.request.RequestType

/**
  * Created by hwang2 on 4/13/17.
  */
sealed trait Error {
  val code: Int
  val message: String
  override def toString: String = s"ERROR_CODE:$code $message"
}

case class MaxWindowExceededError(max: Int, cur: Int, fact: String) extends Error {
  override val code = 10001
  override val message = s"Max days window exceeded expected=$max, actual=$cur for cube/fact=$fact"
}

case class MaxLookBackExceededError(max: Int, cur: Int, fact: String) extends Error {
  override val code = 10002
  override val message = s"Max look back window exceeded expected=$max, actual=$cur for cube/fact=$fact"
}

case class GranularityNotSupportedError(cube: String, requestType: RequestType, grain: Grain) extends Error {
  override val code = 10003
  override val message = s"Cube $cube does not support request type : $requestType -> $grain"
}

case class NoRelationWithPrimaryKeyError(cube: String, key: String, field: Option[String] = None) extends Error {
  override val code = 10004
  override val message = {
    val fieldString = field.fold("") {f => s" for field $f"}
    s"Cube $cube has no relation to dimension with primary key $key$fieldString"
  }
}

case class UnknownFieldNameError(field: String) extends Error {
  override val code = 10005
  override val message = s"Failed to find primary key alias for $field"
}

case class FutureDateNotSupportedError(date: String) extends Error {
  override val code = 10006
  override val message = s"Querying for future date $date is not supported"
}

case class RestrictedSchemaError(colsWithRestrictedSchema: IndexedSeq[String], schema: String, cubeName: String) extends Error {
  override val code = 10007
  override val message = s"${colsWithRestrictedSchema.mkString("(", ", ", ")")} can't be used with ${schema} schema in ${cubeName} cube"
}

case class InCompatibleColumnError(alias: String, incompatibleColumns: Set[String]) extends Error {
  override val code = 10008
  override val message = s"Incompatible columns found in request, ${alias} is not compatible with ${incompatibleColumns}"
}


