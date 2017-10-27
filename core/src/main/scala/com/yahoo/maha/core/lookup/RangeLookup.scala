// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.lookup

import scala.annotation.tailrec

/**
 * Created by hiral on 3/31/16.
 */
trait Range[T] {
  def from: T
  def to: T
  def inRange(value: T) : Boolean
  def isBefore(value: T) : Boolean
}

trait RangeInclusive[T] extends Range[T] {
  require(inRange(from))
  require(inRange(to))
}

case class LongRange(from: Long, to: Long) extends RangeInclusive[Long] {
  require(from < to)
  def inRange(value: Long) : Boolean = value >= from && value <= to
  def isBefore(value: Long) : Boolean = value < from
}

object RangeLookup {
  def binarySearch[T, U <: RangeInclusive[T], V](key: T, list: IndexedSeq[(U, V)]): Option[V] = {
    @tailrec
    def go(lo: Int, hi: Int): Option[V] = {
      if (lo > hi)
        None
      else {
        val mid: Int = lo + (hi - lo) / 2
        val elem = list(mid)
        if(elem._1.inRange(key)) {
          Option(elem._2)
        } else {
          if(elem._1.isBefore(key)) {
            go(lo, mid - 1)
          } else {
            go(mid + 1, hi)
          }
        }
      }
    }
    go(0, list.size - 1)
  }
}
trait RangeLookup[T, U <: RangeInclusive[T], V] {
  val list : IndexedSeq[(U, V)]

  def find(key: T) : Option[V] = RangeLookup.binarySearch(key, list)
}

class LongRangeLookup[V] private[lookup](val list: IndexedSeq[(LongRange, V)]) extends RangeLookup[Long, LongRange, V]

object LongRangeLookup {
  def apply[V](list: IndexedSeq[(LongRange, V)]) : LongRangeLookup[V] = {
    val builder = new LongRangeLookupBuilder[V]
    list.foreach { case (range, value) => builder.add(range, value) }
    builder.build
  }
  def full[V](value : V) : LongRangeLookup[V] = {
    val builder = new LongRangeLookupBuilder[V]
    builder.add(LongRange(0, Long.MaxValue), value)
    builder.build
  }
}

class LongRangeLookupBuilder[V] {
  val list = new collection.mutable.ArrayBuffer[(LongRange, V)]()
  
  def add(range: LongRange, value: V) : LongRangeLookupBuilder[V] = {
    val fromExists = RangeLookup.binarySearch(range.from, list)
    require(fromExists.isEmpty, s"range overlap, existing range : ${fromExists.get} , new range : $range")
    val toExists = RangeLookup.binarySearch(range.to, list)
    require(toExists.isEmpty, s"range overlap, existing range : ${toExists.get} , new range : $range")
    list += range -> value
    this
  }
  
  def build : LongRangeLookup[V] = {
    new LongRangeLookup(list.toIndexedSeq)
  }
}

