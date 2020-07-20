// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.ddl

import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension.{OracleDerDimCol, DimCol}

import scala.collection.mutable


/**
 * Created by shengyao on 1/28/16.
 */
class OracleDDLGenerator {

  def toDDL(table: BaseTable): String = {

    def renderColumnDefn(name: String, col: Column): String = {
      val defaultClause = getDefaultClause(col.dataType)
      val columnConstraints = defaultClause match {
        case "" => getColumnConstraint(false, col)
        case _ => getColumnConstraint(true, col)
      }
      s"""$name\t${renderType(col.dataType)}\t$defaultClause\t$columnConstraints"""
    }

    def renderType(dataType: DataType) : String = {
      dataType match {
        case IntType(length, _, _, _, _) =>
          s"""NUMBER($length)"""
        case DecType(length, scale, _, _, _, _) =>
          s"""NUMBER($length, $scale)"""
        case StrType(length, _, _) =>
          s"""VARCHAR2($length CHAR)"""
        case _ =>
          s"""${dataType.jsonDataType.toUpperCase}"""
      }
    }

    def getDefaultClause(dataType: DataType) : String = {
      dataType match {
        case d if d.isInstanceOf[IntType] && d.asInstanceOf[IntType].default.isDefined =>
          s"""DEFAULT ${d.asInstanceOf[IntType].default.get}"""
        case d if d.isInstanceOf[DecType] && d.asInstanceOf[DecType].default.isDefined =>
          s"""DEFAULT ${d.asInstanceOf[DecType].default.get}"""
        case d if d.isInstanceOf[StrType] && d.asInstanceOf[StrType].default.isDefined =>
          s"""DEFAULT ${d.asInstanceOf[StrType].default.get}"""
        case _ => s""""""
      }
    }

    def getColumnConstraint(hasDefaultValue : Boolean, column: Column) : String = {
      column match {
        case c if /*c.isInstanceOf[DimCol] | c.isInstanceOf[OracleDerDimCol] |*/ hasDefaultValue => "NOT NULL"
        case _ => "NULL"
      }
    }

    def generatePartitionClause(columnNames: Set[String]): String = {
      columnNames match {
        case c if c.contains("stats_date") =>
          s"""PARTITION BY LIST(stats_date)
             |( PARTITION p_default VALUES(TO_DATE('01-JAN-1970 00:00:00', 'DD-MON-YYYY HH24:MI:SS'))
             |)
             |;
         """.stripMargin
        case _ => ";"
      }
    }

    def generatePrimaryKeyConstraint(tableName: String, tablePrimaryKeys: Set[String]): String = {
      tablePrimaryKeys match {
        case t if t.nonEmpty =>
          s"""ALTER TABLE $tableName
             |ADD CONSTRAINT ${tableName}_pk
             |PRIMARY KEY
             |(${tablePrimaryKeys.mkString(",")})
             |;
       """.stripMargin
        case _ => ""
      }
    }

    val tableName = table.name

    val columns = table.columnsByNameMap.filter( m => !m._2.isInstanceOf[DerivedColumn] ) map {
      case (name, col) =>
        renderColumnDefn(name, col)
    }

    val partitionColumnNames = new mutable.HashSet[String]
    val tablePrimaryKeys = new mutable.HashSet[String]

    table.ddlAnnotation match {
      case Some(ddlAnnotation) =>
        ddlAnnotation.asInstanceOf[OracleDDLAnnotation].partCols.foreach(col => partitionColumnNames += col)
        ddlAnnotation.asInstanceOf[OracleDDLAnnotation].pks.foreach(pk => tablePrimaryKeys += pk)
      case None => //do nothing
    }

    partitionColumnNames.foreach {
      pc => require(table.columnsByNameMap.keySet.contains(pc), s"Partition column $pc does not exist in table $tableName")
    }

    tablePrimaryKeys.foreach {
      pk => require(table.columnsByNameMap.keySet.contains(pk), s"Primary key column $pk does not exist in table $tableName")
    }

    s"""CREATE TABLE $tableName
       |(${columns.mkString(", \n")})
       |${generatePartitionClause(partitionColumnNames.toSet)}
       |${generatePrimaryKeyConstraint(tableName, tablePrimaryKeys.toSet)}
     """.stripMargin

  }
}
