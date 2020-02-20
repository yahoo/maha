// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import com.yahoo.maha.core.dimension.DimensionColumn
import org.json4s.JsonAST.{JArray, JObject, JValue}
import org.json4s.scalaz.JsonScalaz._

/**
 * Created by hiral on 10/13/15.
 */

trait PublicColumn {
  def name: String
  def alias: String
  def filters: Set[FilterOperation]
  def dependsOnColumns: Set[String]
  def incompatibleColumns: Set[String]
  def required: Boolean
  def hiddenFromJson: Boolean
  def filteringRequired: Boolean
  def restrictedSchemas: Set[Schema]
  def isImageColumn: Boolean

  private val jUtils = JsonUtils

  def asJSON: JObject = {
    makeObj(
      List(("name" -> toJSON(name))
        ,("alias" -> toJSON(alias))
        ,("schemas" -> jUtils.asJSON(restrictedSchemas.toList))
        ,("dependsOnColumns" -> jUtils.asJSON(dependsOnColumns))
        ,("incompatibleColumns" -> jUtils.asJSON(incompatibleColumns))
        ,("filters" -> jUtils.asJSON(filters.map(f => f.toString)))
        ,("required" -> toJSON(required))
        ,("hiddenFromJson" -> toJSON(hiddenFromJson))
        ,("filteringRequired" -> toJSON(filteringRequired))
        ,("isImageColumn" -> toJSON(isImageColumn))
      )
    )
  }
}

trait Column {
  def isDerivedColumn: Boolean
  def columnContext: ColumnContext
  def name: String
  def alias: Option[String]
  def dataType: DataType
  def annotations: Set[ColumnAnnotation]
  def filterOperationOverrides: Set[FilterOperation]

  val hasEngineRequirement : Boolean = {
    this.isInstanceOf[EngineRequirement]
  }

  val hasAnnotationsWithEngineRequirement : Boolean = {
    annotationsWithEngineRequirement.nonEmpty
  }
  
  val isKey : Boolean = annotations.exists(a => a.isInstanceOf[ForeignKey] || a == PrimaryKey)

  def caseInSensitive : Boolean = annotations.contains(CaseInsensitive)
  def escapingRequired : Boolean = annotations.contains(EscapingRequired)

  def annotationsWithEngineRequirement: Set[ColumnAnnotation] = annotations.filter(_.isInstanceOf[EngineRequirement])

  private val jUtils = JsonUtils

  def asJSON: JObject = {
    makeObj(
      List(
        ("name" -> toJSON(name))
        ,("alias" -> toJSON(alias))
        ,("dataType" -> dataType.asJSON)
        ,("annotations" -> jUtils.asJSON(annotations))
        ,("filterOperationOverrides", jUtils.asJSON(filterOperationOverrides))
      )
    )
  }
}

trait ConstColumn extends Column {
  def constantValue: String

  override def asJSON: JObject = {
    makeObj(
      List(
        ("ConstColumn" -> super.asJSON)
        ,("constantValue" -> toJSON(constantValue))
      )
    )
  }
}

trait DerivedColumn extends Column {
  override val isDerivedColumn: Boolean = true
  def derivedExpression : DerivedExpression[_]

  override def asJSON: JObject = {
    makeObj(
      List(
        ("DerivedColumn" -> super.asJSON)
        ,("derivedExpression" -> derivedExpression.asJSON)
      )
    )
  }
}

trait DerivedFunctionColumn extends Column {
  override val isDerivedColumn: Boolean = false
  def derivedFunction : DerivedFunction

  override def asJSON: JObject = {
    makeObj(
      List(
        ("DerivedFunctionColumn" -> super.asJSON)
        ,("derivedFunction" -> derivedFunction.asJSON)
      )
    )
  }
}

trait PostResultColumn extends Column {
  override val isDerivedColumn: Boolean = false
  def postResultFunction: PostResultFunction
  def validate()

  override def asJSON: JObject = {
    makeObj(
      List(
        ("PostResultColumn" -> super.asJSON)
        ,("postResultFunction" -> postResultFunction.asJSON)
      )
    )
  }
}

trait PartitionColumn extends Column {
  def partitionLevel : PartitionLevel

  override def asJSON: JObject = {
    makeObj(
      List(
        ("PartitionColumn" -> super.asJSON)
        ,("partitionLevel" -> partitionLevel.asJSON)
      )
    )
  }
}

object PartitionColumn {
  implicit val ordering : Ordering[PartitionColumn] = Ordering.by(_.partitionLevel.level)
}

class ColumnContext {
  private[this] var cols : Map[String, Column] = Map.empty
  def register(d: Column) = {
    require(!cols.contains(d.name), s"Column already exists : ${d.name}")
    cols = cols + (d.name -> d)
  }

  def getColumnByName(name: String): Option[Column] = {
    cols.get(name)
  }

  def render(name: String
             , renderedColumnAliasMap: scala.collection.Map[String, String]
             , columnPrefix: Option[String] = None
             , parentColumn: Option[String] = None
             , expandDerivedExpression: Boolean = true) : String = {
    require(cols.contains(name), s"Column doesn't exist: $name")
    cols(name) match {
      case c if c.isInstanceOf[DerivedColumn] && !parentColumn.contains(name) && expandDerivedExpression =>
        val dc = c.asInstanceOf[DerivedColumn]
        dc.derivedExpression.render(name
          , renderedColumnAliasMap
          , None
          , columnPrefix
          , expandDerivedExpression
          , insideDerived = true) match {
          case s: String => s
          case _ => dc.derivedExpression.expression.asString
        }
      case c if c.isInstanceOf[DerivedColumn] && !parentColumn.contains(name) && !expandDerivedExpression
        && renderedColumnAliasMap.contains(name) =>
        renderedColumnAliasMap(name)
      case c =>
        if(columnPrefix.isDefined) {
          s"""${columnPrefix.get}"${c.alias.getOrElse(name)}""""
        } else {
          c.alias.getOrElse(c.name)
        }
    }
  }

  def isDimensionDriven(name: String) : Boolean = {
    require(cols.contains(name), s"Unknown column referenced through column context : $name")
    val c = cols(name)
    if (c.isDerivedColumn) {
      c.asInstanceOf[DerivedColumn].derivedExpression.isDimensionDriven
    } else {
      c.isInstanceOf[DimensionColumn]
    }
  }

  override def toString: String = {
    this.hashCode().toString
  }
}

object ColumnContext {
  def withColumnContext[T](fn: ColumnContext => T):  T = {
    implicit val dc : ColumnContext = new ColumnContext
    fn(dc)
  }

  def validateColumnContext[T <: Column](columns: Set[T], errorPrefix: String) : Unit = {
    val cc = columns.head.columnContext
    columns.foreach {
      case c if c.isInstanceOf[DerivedExpression[_]] =>
        require(c.columnContext eq cc, s"$errorPrefix : ColumnContext must be same for all columns : ${c.name}")
        require(c.asInstanceOf[DerivedExpression[_]].columnContext eq cc, s"$errorPrefix : ColumnContext must be same for derived expression and column : ${c.name}")
      case c =>
        require(c.columnContext eq cc, s"$errorPrefix : ColumnContext must be same for all columns : ${c.name}")
    }
  }
}
