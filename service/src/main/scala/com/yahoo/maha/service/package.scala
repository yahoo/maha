// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha

import com.yahoo.maha.parrequest.future.ParFunction

/**
  * Created by hiral on 6/19/17.
  */
package object service {
  implicit def toParFunction[T, U](fn1: Function1[T,U]) : ParFunction[T,U] = {
    ParFunction.fromScala(fn1)
  }
}
