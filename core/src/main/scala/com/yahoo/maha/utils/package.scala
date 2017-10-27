// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
/**
 * Created by jians on 10/15/15.
 */
package object utils {
  def power[T](original: Set[T], len: Int):Set[Set[T]] = {
    original.subsets().filter(s => s.nonEmpty && s.size <= len).toSet
  }
}
