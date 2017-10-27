// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.ddl

import com.yahoo.maha.core._

/**
 * Created by shengyao on 3/9/16.
 */
class HiveDDLGenerator {
  def toDDL(table: BaseTable): String = {
    val tableName = table.name
    val columnDefnsMap : Map[String, String] = table.columnsByNameMap.filter(m => !m._2.isInstanceOf[PartitionColumn] && !m._2.isDerivedColumn) collect {
      case (name, col) if col.alias.isEmpty || !table.columnsByNameMap.contains(col.alias.get) =>
        name -> renderColumnDefn(name, col)
    }

    val partitionCols = table.columnsByNameMap.filter( m => m._2.isInstanceOf[PartitionColumn]) map {
      case (name, col) =>
        renderColumnDefn(name, col)
    }

    val partitionClause = partitionCols match {
      case cols if cols.isEmpty => ""
      case cols => s"""PARTITIONED BY(
                      |${cols.mkString(", \n")}
                      |)
       """.stripMargin
    }

    val ddlAnnotationOption = table.ddlAnnotation.map(_.asInstanceOf[HiveDDLAnnotation])
    val storage = ddlAnnotationOption.flatMap(_.annotations.get("hive.storage")).getOrElse("TEXT")
    val fieldDelimiter = ddlAnnotationOption.flatMap(_.annotations.get("hive.fieldDelimiter")).getOrElse("\\001")
    val recordTerminator = ddlAnnotationOption.flatMap(_.annotations.get("hive.recordTerminator")).getOrElse("\\n")
    val path = ddlAnnotationOption.flatMap(_.annotations.get("hive.LOCATION")).getOrElse("")

    val colOrdering : IndexedSeq[String] = ddlAnnotationOption match {
      case a if a.isDefined => a.get.columnOrdering
      case _ => IndexedSeq.empty
    }

    val columnDefns : IndexedSeq[String] = colOrdering match {
      case co if co.nonEmpty =>
        val orderCols = IndexedSeq.newBuilder[String]
        co.foreach {
          col =>
            require(columnDefnsMap.contains(col), s"Column $col in column ordering doesn't exist in table $tableName")
            if (!orderCols.result().contains(columnDefnsMap(col)))
              orderCols += columnDefnsMap(col)
        }
        val coSet = co.toSet
        columnDefnsMap.keys.filter(c => !coSet.contains(c)) foreach {
          col =>
            if (!orderCols.result().contains(columnDefnsMap(col)))
              orderCols += columnDefnsMap(col)
        }
        orderCols.result()
      case _ =>
        columnDefnsMap.values.toIndexedSeq
    }

    s"""CREATE TABLE $tableName
       |(${columnDefns.mkString(", \n")})
       |$partitionClause
       |${generateFormatClause(storage, fieldDelimiter, recordTerminator)}
       |${generateLocationClause(path, tableName)}
       |;
     """.stripMargin
  }

  private[this] def renderColumnDefn(name: String, col: Column): String = {
    val dataType = col.dataType match {
      case IntType(length, _, _, _, _) if length <= 3 => "tinyint"
      case DateType(_) => "string"
      case StrType(_, _, _) => "string"
      case DecType(_, _, _, _, _, _) => "double"
      case _ => "bigint"
    }
    s"""${col.alias.getOrElse(name)} $dataType"""
  }

  private[this] def generateFormatClause(storage: String,
                                         fieldDelimiter: String, recordTerminator: String) : String = {
      storage match {
        case "TEXT" =>
          s"""ROW FORMAT DELIMITED FIELDS TERMINATED BY
             |'$fieldDelimiter'
             |LINES TERMINATED BY '$recordTerminator'""".stripMargin
        case _ => s"""STORED AS $storage"""
      }
  }

  private[this] def generateLocationClause(path: String, tableName: String) : String = {
    path match {
      case "" => ""
      case _ => s"""LOCATION ${path.replace("${name}", tableName)}"""
    }

  }
}
