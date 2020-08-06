// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.ddl

import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension.{DimCol, PostgresDerDimCol}

import scala.collection.mutable


class PostgresDDLGenerator {

  def toDDL(table: BaseTable): String = {
    val renderedColSet = new mutable.HashSet[String]

    def renderColumnDefn(name: String, col: Column): String = {
      val defaultClause = getDefaultClause(col.dataType)
      val columnConstraints = defaultClause match {
        case "" => getColumnConstraint(false, col)
        case _ => getColumnConstraint(true, col)
      }
      val colName = col.alias.getOrElse(name).replaceAllLiterally(" ","_").toLowerCase()
      if(!renderedColSet(colName)) {
        renderedColSet += colName
        s"""$colName\t${renderType(col.dataType)}\t$defaultClause\t$columnConstraints"""
      } else ""
    }

    def renderType(dataType: DataType) : String = {
      dataType match {
        case IntType(length, _, _, _, _) =>
          if(length > 0)
            s"""NUMERIC($length)"""
          else
            "NUMERIC"
        case DecType(length, scale, _, _, _, _) =>
          if(length > 0 && scale > 0)
            s"""NUMERIC($length, $scale)"""
          else
            "NUMERIC"
        case StrType(length, _, _) =>
          if(length > 0)
            s"""VARCHAR($length)"""
          else
            "VARCHAR"
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
        case c if c.isInstanceOf[DimCol] | c.isInstanceOf[PostgresDerDimCol] | hasDefaultValue => "NOT NULL"
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

    val columns = table.columnsByNameMap.filter( m => !m._2.isInstanceOf[DerivedColumn] ).map {
      case (name, col) =>
        renderColumnDefn(name, col)
    }.filterNot(_.isEmpty)

    var partitionColumnNames = new mutable.HashSet[String]
    var tablePrimaryKeys = new mutable.HashSet[String]

    table.ddlAnnotation match {
      case Some(ddlAnnotation) =>
        ddlAnnotation.asInstanceOf[PostgresDDLAnnotation].partCols.foreach(col => partitionColumnNames += col)
        ddlAnnotation.asInstanceOf[PostgresDDLAnnotation].pks.foreach(pk => tablePrimaryKeys += pk)
      case None => //do nothing
    }

    partitionColumnNames = partitionColumnNames.map {
      pc =>
        require(table.columnsByNameMap.keySet.contains(pc), s"Partition column $pc does not exist in table $tableName")
        val c = table.columnsByNameMap(pc)
        val n = c.alias.getOrElse(c.name)
        n
    }

    tablePrimaryKeys = tablePrimaryKeys.map {
      pk =>
        require(table.columnsByNameMap.keySet.contains(pk), s"Primary key column $pk does not exist in table $tableName")
        val c = table.columnsByNameMap(pk)
        val n = c.alias.getOrElse(c.name)
        n
    }

    s"""CREATE TABLE $tableName
       |(${columns.mkString(", \n")})
       |${generatePartitionClause(partitionColumnNames.toSet)}
       |${generatePrimaryKeyConstraint(tableName, tablePrimaryKeys.toSet)}
     """.stripMargin

  }
}
