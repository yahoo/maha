// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.fact

import com.yahoo.maha.core._
import com.yahoo.maha.core.ddl.DDLAnnotation
import com.yahoo.maha.core.dimension.{BaseFunctionDimCol, ConstDimCol, DimensionColumn}
import com.yahoo.maha.core.request.RequestType

import scala.collection.{SortedSet, mutable}

/**
 * Created by pranavbhole on 01/11/16.
 */
trait View {
  def viewName : String
  def facts : Seq[Fact]
  def viewOperation : ViewOperation
  def dimColMap : Map[String, DimensionColumn]
  def factColMap : Map[String, FactColumn]
  def columnsByNameMap : Map[String, Column]
}

case class UnionView(viewName : String, facts: Seq[Fact]) extends View {

  View.validateBaseViewFacts(facts)

  val dimCols = facts.head.dimCols
  val factCols = facts.head.factCols

  val viewOperation = UnionViewOperation

  val fields : Set[String] = dimCols.map(_.name) ++ factCols.map(_.name)
  val dimColMap : Map[String, DimensionColumn] = dimCols.map(c => (c.name, c)).toMap
  val factColMap : Map[String, FactColumn] = factCols.map(c => (c.name, c)).toMap
  val columnsByNameMap : Map[String, Column] = dimColMap ++ factColMap
}

object View {
  def validateBaseViewFacts(facts : Seq[Fact]): Unit = {
    require(facts.size > 0, "Require 2 or more facts in order to create view")

    facts.sliding(2).foreach {
      seq =>
        val fact1 = seq.tail.head
        val fact2 = seq.head
        require(fact1.dimCols.size == fact2.dimCols.size, s"two tables ${fact1.name}, ${fact2.name} should have same number of dim columns")
        require(fact1.factCols.size == fact2.factCols.size, s"two tables ${fact1.name}, ${fact2.name} should have same number of fact columns")
        fact1.dimCols.foreach {
          col=>
            require(fact2.dimCols.map(_.name).contains(col.name), s"Col mismatch ${col.name} in view ${fact1.name}, ${fact2.name}")
        }
        fact1.factCols.foreach {
          col=>
            require(fact2.factCols.map(_.name).contains(col.name), s"Col mismatch ${col.name} in view ${fact1.name}, ${fact2.name}")
        }
    }
  }

  def validateView(view: View, dimCols: Set[DimensionColumn], factCols: Set[FactColumn]): Unit = {
    dimCols.foreach {
      dimCol =>
        require(view.dimColMap.contains(dimCol.name), s"Failed to find $dimCol in the View")
        val viewDimCol = view.dimColMap.get(dimCol.name)
        require(viewDimCol.get.dataType.jsonDataType == dimCol.dataType.jsonDataType, s"Col data type does not match with union View Col $dimCol")
    }

    factCols.foreach {
      factCol =>
        require(view.factColMap.contains(factCol.name), s"Failed to find $factCol in the View")
        val viewFactCol = view.factColMap.get(factCol.name)
        require(viewFactCol.get.dataType.jsonDataType == factCol.dataType.jsonDataType, s"Col data type does not match with union View Col $factCol")
    }
  }
}

trait ViewOperation
class UnionViewOperation extends ViewOperation
object UnionViewOperation extends ViewOperation


case class ViewBaseTable private[fact](name: String
                                   , level: Int
                                   , grain: Grain
                                   , engine: Engine
                                   , schemas: Set[Schema]
                                   , dimCols: Set[DimensionColumn]
                                   , factCols: Set[FactColumn]
                                   , from: Option[Fact]
                                   , annotations: Set[FactAnnotation]
                                   , ddlAnnotation: Option[DDLAnnotation]
                                   , costMultiplierMap: Map[RequestType, CostMultiplier]
                                   , forceFilters: Set[ForceFilter]
                                   , defaultCardinality: Int
                                   , defaultRowCount: Int
                                   , viewBaseTable : Option[String]
                                   , maxDaysWindow: Option[Map[RequestType, Int]]
                                   , maxDaysLookBack: Option[Map[RequestType, Int]]
                                   , availableOnwardsDate: Option[String]
                                    ) extends FactView {
  validate()

  def copyWith(newName:String, discarding: Set[String], newConstNameToValueMap:Map[String, String]): ViewBaseTable = {

    discarding.foreach {
      colName =>
        require(!factCols.map(_.name).contains(colName), s"Can not discard fact col")
        require(dimCols.map(_.name).contains(colName), s"Discarding col $colName is not existed in the base ViewTable $name")
    }

    newConstNameToValueMap.foreach {
      entry=>
        require(!factCols.map(_.name).contains(entry._1), s"Can not override the constant fact column ${entry._1} to override with new constant value")
        require(dimCols.map(_.name).contains(entry._1), s"Unable to find the constant dim column ${entry._1} to override with new constant value")
        require(dimCols.filter(col=> col.name.equals(entry._1)).head.isInstanceOf[ConstDimCol], s"present col should be instance of constant dim column ${entry._1} to override with new constant value")
    }

    ColumnContext.withColumnContext { columnContext =>
      val mutableDiscardingSet: mutable.Set[String] = new mutable.HashSet[String]()
      discarding.foreach(mutableDiscardingSet.add)
      newConstNameToValueMap.foreach(e=> mutableDiscardingSet.add(e._1))
      var hasAdditionalDiscards = false
      val rolledUpDims = {
        do {
          hasAdditionalDiscards = false
          this
            .dimCols
            .view
            .filterNot(dim => mutableDiscardingSet.contains(dim.name))
            .filter(dimCol => (dimCol.isDerivedColumn
              && dimCol.asInstanceOf[DerivedColumn].derivedExpression.sourceColumns.intersect(discarding).nonEmpty))
            .foreach { c =>
              mutableDiscardingSet += c.name
              hasAdditionalDiscards = true
            }
        } while(hasAdditionalDiscards)
        this
          .dimCols
          .filter(dim => !discarding.contains(dim.name) &&
            !newConstNameToValueMap.contains(dim.name))
          .filter(dimCol => !dimCol.isDerivedColumn
            || (dimCol.isDerivedColumn
            && dimCol.asInstanceOf[DerivedColumn].derivedExpression.sourceColumns.intersect(mutableDiscardingSet).isEmpty))
          .map(_.copyWith(columnContext, Map.empty, false))
      }

      val overriddenConstDimCols = dimCols.filter(col=> newConstNameToValueMap.contains(col.name)).map {
        col =>
          val newValue = newConstNameToValueMap.get(col.name).get
          col.asInstanceOf[ConstDimCol].copyWith(columnContext, newValue)
      }


      rolledUpDims.foreach {
        c =>
          require(c.columnContext eq columnContext, "Failed to copy dim columns with new column context!")
      }

      require(rolledUpDims.exists(_.isForeignKey), s"Fact has no foreign keys after discarding $discarding")

      val factCols = {
        do {
          hasAdditionalDiscards = false
          this
            .factCols
            .view
            .filterNot(dim => mutableDiscardingSet.contains(dim.name))
            .filter(factCol => (factCol.isDerivedColumn
              && factCol.asInstanceOf[DerivedColumn].derivedExpression.sourceColumns.intersect(discarding).nonEmpty))
            .foreach { c =>
              mutableDiscardingSet += c.name
              hasAdditionalDiscards = true
            }
        } while(hasAdditionalDiscards)
        this
          .factCols
          .filter(factCol => !factCol.isDerivedColumn
            || (factCol.isDerivedColumn
            && factCol.asInstanceOf[DerivedColumn].derivedExpression.sourceColumns.intersect(mutableDiscardingSet).isEmpty) )
          .map(_.copyWith(columnContext, Map.empty, false))
      }

      factCols.foreach {
        c =>
          require(c.columnContext eq columnContext, "Failed to copy fact columns with new column context!")
      }

      new ViewBaseTable(newName,
        this.level,
        this.grain,
        this.engine,
        this.schemas,
        rolledUpDims ++ overriddenConstDimCols,
        factCols,
        this.from,
        this.annotations,
        this.ddlAnnotation,
        this.costMultiplierMap,
        this.forceFilters,
        this.defaultCardinality,
        this.defaultRowCount,
        this.viewBaseTable,
        this.maxDaysWindow,
        this.maxDaysLookBack,
        this.availableOnwardsDate)
    }
  }

  val fields : Set[String] = dimCols.map(_.name) ++ factCols.map(_.name)
  val dimColMap : Map[String, DimensionColumn] = dimCols.map(c => (c.name, c)).toMap
  val factColMap : Map[String, FactColumn] = factCols.map(c => (c.name, c)).toMap
  val columnsByNameMap : Map[String, Column] = dimColMap ++ factColMap

  val constantColNameToValueMap: Map[String, String] = {
    val dimColMap = dimCols.filter(_.isInstanceOf[ConstDimCol]).map(d=> d.name -> d.asInstanceOf[ConstDimCol].constantValue).toMap
    val factColMap = factCols.filter(_.isInstanceOf[ConstFactCol]).map(d=> d.name -> d.asInstanceOf[ConstFactCol].constantValue).toMap
    dimColMap++factColMap
  }

  val publicDimToForeignKeyMap : Map[String, String] =
    dimCols
      .filter(_.isForeignKey)
      .map(col => col.getForeignKeySource.get -> col.name)
      .toMap

  val publicDimToForeignKeyColMap : Map[String, Column] =
    dimCols
      .filter(_.isForeignKey)
      .map(col => col.getForeignKeySource.get -> col)
      .toMap

  val partitionCols : SortedSet[PartitionColumn] = {
    dimCols.filter(_.isInstanceOf[PartitionColumn]).map(c=> c.asInstanceOf[PartitionColumn]).to[SortedSet]
  }

  private[this] def validate() : Unit = {
    schemaRequired()
    engineValidations(dimCols)
    engineValidations(factCols)
    validateDerivedExpression(dimCols)
    validateDerivedExpression(factCols ++ dimCols)
    validateForeignKeyCols(factCols)
    validateFactCols(factCols)
    validateDerivedFunction(dimCols)
  }

  private[this] def schemaRequired(): Unit = {
    require(schemas.nonEmpty, s"Failed schema requirement fact=$name, engine=$engine, schemas=$schemas")
  }

  //validation for engine
  private[this] def engineValidations[T <: Column](columns: Set[T]): Unit ={
    columns.foreach { c =>
      c.annotations.filter(_.isInstanceOf[EngineRequirement]).map { er =>
        require(er.asInstanceOf[EngineRequirement].acceptEngine(engine),
          s"Failed engine requirement fact=$name, engine=$engine, col=${c.name}, annotation=$er")
      }
      if(c.isInstanceOf[EngineRequirement]) {
        require(c.asInstanceOf[EngineRequirement].acceptEngine(engine),
          s"Failed engine requirement fact=$name, engine=$engine, col=$c")
      }
    }
    factCols.foreach { fc =>
      if(fc.hasRollupWithEngineRequirement) {
        require(fc.rollupExpression.asInstanceOf[EngineRequirement].acceptEngine(engine),
          s"Failed engine requirement fact=$name, engine=$engine, col=${fc.name}, rollup=${fc.rollupExpression}")
      }
    }
    annotations.filter(_.isInstanceOf[EngineRequirement]).map { er =>
      require(er.asInstanceOf[EngineRequirement].acceptEngine(engine),
        s"Failed engine requirement fact=$name, engine=$engine, annotation=$er")
    }
  }

  //validation for derived expression columns
  private[this] def validateDerivedExpression[T <: Column](columns: Set[T]): Unit = {
    val columnNames : Set[String] = columns.map(_.name)
    columns.view.filter(_.isInstanceOf[DerivedColumn]).map(_.asInstanceOf[DerivedColumn]).foreach { c =>
      require(c.derivedExpression != null,
        s"Derived expression should be defined for a derived column $c")
      c.derivedExpression.sourceColumns.foreach { cn =>
        require(columnNames.contains(cn),
          s"Failed derived expression validation, unknown referenced column in fact=$name, $cn in $c")
        require(cn != c.name,
          s"Derived column is referring to itself, this will cause infinite loop : fact=$name, column=$cn, sourceColumns=${c.derivedExpression.sourceColumns}")
      }
      //validate we can render it
      c.derivedExpression.render(c.name)
    }
  }

  private[this] def validateForeignKeyCols[T <: Column](columns: Set[T]) : Unit = {
    columns.foreach {
      col =>
        if(col.annotations.exists(_.isInstanceOf[ForeignKey])) {
          require(col.isInstanceOf[DimensionColumn], s"Non dimension column cannot have foreign key annotation : $col")
        }
    }
  }

  private[this] def validateFactCols[T <: Column](columns: Set[T]): Unit = {
    columns.view.filter(_.isInstanceOf[FactCol]).map(_.asInstanceOf[FactCol]).foreach { c =>
      //validate we can render it
      c.validate()
    }
  }

  private[this] def validateDerivedFunction[T <: Column](columns: Set[T]): Unit = {
    columns.view.filter(_.isInstanceOf[BaseFunctionDimCol]).map(_.asInstanceOf[BaseFunctionDimCol]).foreach { c =>
      //validate we can render it
      c.validate()
    }
  }

  def postValidate(publicFact: PublicFact) : Unit = {
  }
}
