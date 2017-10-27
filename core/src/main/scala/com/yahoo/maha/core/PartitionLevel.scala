// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

/**
 * Created by pranavbhole on 18/04/16.
 */
sealed trait PartitionLevel {
  def level: Int
}

case object NoPartitionLevel extends PartitionLevel {
  val level: Int = 0
}
case object FirstPartitionLevel extends PartitionLevel {
  val level: Int = 1
}
case object SecondPartitionLevel extends PartitionLevel {
  val level: Int = 2
}
case object ThirdPartitionLevel extends PartitionLevel {
  val level: Int = 3
}
