// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.

package com.yahoo.maha.service

import java.lang.reflect.Modifier

import grizzled.slf4j.Logging
import net.bytebuddy.ByteBuddy
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy
import net.bytebuddy.implementation.MethodDelegation
import net.bytebuddy.matcher.ElementMatchers

object DynamicWrapper extends Logging {

  val CURRENT_OBJECT = "currentObject"

  def getDynamicClassFor(obj: Object) = {
    info("Getting Dynamic Class for : " + obj.getClass)
    new ByteBuddy()
      .subclass(obj.getClass, ConstructorStrategy.Default.DEFAULT_CONSTRUCTOR)
      .defineField(CURRENT_OBJECT, obj.getClass, Modifier.PUBLIC)
      .method(ElementMatchers.not(ElementMatchers.isDeclaredBy(classOf[Object])))
      .intercept(MethodDelegation.toField(CURRENT_OBJECT))
      .make()
      .load(getClass().getClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
      .getLoaded
  }
}