package com.yahoo.maha.core.helper.jdbc

import com.yahoo.maha.core.{DataType, DateType, DecType, IntType, StrType, TimestampType}

object ColumnGenerator {
  private[jdbc] val printer =  pprint.PPrinter.BlackWhite.copy(
    additionalHandlers = {
      case value: com.yahoo.maha.core.FilterOperation =>
        val cn = value.getClass().getSimpleName().filterNot(_ == '$')
        pprint.Tree.Literal(s"${cn}")
    }
  )

  def getColumnType(typeName: String, cn: String): DataType = {
    typeName.toLowerCase match {
      case "uuid" | "text" | "varchar" | "varchar2" => StrType()
      case "timestamptz" => TimestampType()
      case a if a.startsWith("int") => IntType()
      case a if a.startsWith("float") || a.startsWith("real") || a.startsWith("numeric") => DecType()
      case a if a.startsWith("bool") => StrType()
      case a if a.startsWith("date") => DateType()
      case _ =>
        throw new IllegalArgumentException(s"Unhandled type=$typeName for column=$cn")
    }
  }

  def getAlias(prefix: String, colName: String, idColumn: String = "id"): String = {
    val diff = 'a' - 'A'
    val alias: Array[Char] = colName.toLowerCase match {
      case b if b == idColumn => s"${prefix}_$idColumn".toLowerCase().toCharArray
      case a => a.toCharArray
    }
    var count = 0
    var capNext = true
    while (count < alias.length) {
      if (alias(count) != '_' || alias(count) != ' ') {
        if (capNext) {
          capNext = false
          if (alias(count) >= 'a' && alias(count) <= 'z') {
            alias(count) = (alias(count) - diff).toChar
          }
        }
      }
      if (alias(count) == '_') {
        alias(count) = ' '
      }
      capNext = alias(count) == ' '
      count += 1
    }
    new String(alias)
  }

  def capWord(s: String): String = {
    s.substring(0, 1).toUpperCase + s.drop(1)
  }
}
