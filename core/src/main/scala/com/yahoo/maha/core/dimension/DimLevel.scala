// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.dimension

/**
 * Created by hiral on 10/5/15.
 */

sealed trait DimLevel {
  //value between 0 and 9
  val level: Int
  
  def >(that: DimLevel) : Boolean = this.level > that.level
  def <(that: DimLevel) : Boolean = this.level < that.level
  def >=(that: DimLevel) : Boolean = this.level >= that.level
  def <=(that: DimLevel) : Boolean = this.level <= that.level
  def -(that: Int) : DimLevel = (this.level - that) match {
    case 1 => LevelOne
    case 2 => LevelTwo
    case 3 => LevelThree
    case 4 => LevelFour
    case 5 => LevelFive
    case 6 => LevelSix
    case 7 => LevelSeven
    case 8 => LevelEight
    case 9 => LevelNine
    case _ => this
  }
  def +(that: Int) : DimLevel = (this.level + that) match {
    case 1 => LevelOne
    case 2 => LevelTwo
    case 3 => LevelThree
    case 4 => LevelFour
    case 5 => LevelFive
    case 6 => LevelSix
    case 7 => LevelSeven
    case 8 => LevelEight
    case 9 => LevelNine
    case _ => this
  }
}

case object LevelOne extends DimLevel {
  val level: Int = 1
}
case object LevelTwo extends DimLevel {
  val level: Int = 2
}
case object LevelThree extends DimLevel {
  val level: Int = 3
}
case object LevelFour extends DimLevel {
  val level: Int = 4
}
case object LevelFive extends DimLevel {
  val level: Int = 5
}
case object LevelSix extends DimLevel {
  val level: Int = 6
}
case object LevelSeven extends DimLevel {
  val level: Int = 7
}
case object LevelEight extends DimLevel {
  val level: Int = 8
}
case object LevelNine extends DimLevel {
  val level: Int = 9
}
