// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service

import java.io.Closeable

import com.google.common.io.Closer
import com.yahoo.maha.service.error.{FailedToConstructFactory, JsonParseError, MahaServiceError}

import scala.reflect.runtime.universe._
import scala.reflect.runtime._
import scala.reflect.runtime.{universe => ru}
import _root_.scalaz._
import syntax.validation._

/**
  * Created by hiral on 5/30/17.
  */
package object factory {

  def getFactory[T](clazz: String)(implicit tag: TypeTag[T]) : MahaServiceConfig.MahaConfigResult[T] = {
    getFactory(clazz, None)
  }
  def getFactory[T](clazz: String, closer: Closer)(implicit tag: TypeTag[T]) : MahaServiceConfig.MahaConfigResult[T] = {
    getFactory(clazz, Option(closer))
  }
  def getFactory[T](clazz: String, closerOption: Option[Closer])(implicit tag: TypeTag[T]) : MahaServiceConfig.MahaConfigResult[T] = {
    try {
      val m = ru.runtimeMirror(Thread.currentThread().getContextClassLoader)
      val typ: universe.ClassSymbol = m.staticClass(clazz)
      val cm = m.reflectClass(typ)
      val ctor = cm.symbol.primaryConstructor.asMethod
      val t: T = cm.reflectConstructor(ctor).apply().asInstanceOf[T]
      if(t.isInstanceOf[Closeable]) {
        closerOption.foreach(_.register(t.asInstanceOf[Closeable]))
      }
      t.successNel[MahaServiceError]
    } catch {
      case t: Exception =>
        FailedToConstructFactory(s"Failed to construct factory : $clazz", Option(t)).asInstanceOf[MahaServiceError].failureNel[T]
    }

  }

  import org.json4s.scalaz.JsonScalaz._
  implicit def resultToMahaSerivceError[T](r: Result[T]) : MahaServiceConfig.MahaConfigResult[T] = {
    r.leftMap( nel => nel.map(err => JsonParseError(err.toString)))
  }
}
