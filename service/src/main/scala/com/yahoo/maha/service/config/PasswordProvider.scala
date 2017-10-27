// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.config

/**
  * Created by hiral on 5/30/17.
  */
trait PasswordProvider {
  def getPassword(key: String): String
}

object PassThroughPasswordProvider extends PasswordProvider {
  def getPassword(key: String): String = key
}
