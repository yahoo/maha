package com.yahoo.maha.executor.postgres

import com.yahoo.maha.core.PostgresLiteralMapper

class PostgresLiteralMapperUsingDriver extends PostgresLiteralMapper {
  override protected def getEscapedSqlString(s: String): String = {
    val sbuf = org.postgresql.core.Utils.escapeLiteral(null, s, true)
    sbuf.toString
  }
}
