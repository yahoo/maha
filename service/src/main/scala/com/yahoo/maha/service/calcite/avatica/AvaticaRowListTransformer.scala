package com.yahoo.maha.service.calcite.avatica

import java.sql.{Date, Timestamp, Types}
import java.util

import com.yahoo.maha.core.{DateType, DecType, IntType, StrType, TimestampType}
import com.yahoo.maha.core.query.{InMemRowList, QueryRowList, RowList}
import org.apache.calcite.avatica.{AvaticaParameter, ColumnMetaData, Meta, MetaImpl}
import org.apache.calcite.avatica.Meta.{Frame, Signature, Style}
import org.apache.calcite.avatica.remote.Service
import org.apache.calcite.avatica.remote.Service.{ResultSetResponse, RpcMetadataResponse}

trait AvaticaRowListTransformer {
  def map(rowList:QueryRowList, avaticaContext: AvaticaContext): ResultSetResponse
}

class DefaultAvaticaRowListTransformer extends AvaticaRowListTransformer {

  override def map(rowList: QueryRowList, avaticaContext: AvaticaContext): ResultSetResponse = {
    val columns = new util.ArrayList[ColumnMetaData]
    val params = new util.ArrayList[AvaticaParameter]
    val aliasColumnMap = rowList.query.aliasColumnMap
    rowList.columnNames.zipWithIndex.foreach {
      case (columnName, index) =>
        val columnOption = aliasColumnMap.get(columnName)
        if (columnOption.isDefined) {
          columnOption.get.dataType match {
            case IntType(_,_,_,_,_) =>
              columns.add(MetaImpl.columnMetaData(columnName, index, classOf[java.lang.Integer], true))
            case DecType(_,_,_,_,_,_) =>
              columns.add(MetaImpl.columnMetaData(columnName, index, classOf[java.lang.Double], true))
            case e=>
              columns.add(MetaImpl.columnMetaData(columnName, index, classOf[String], true))
          }
        }
    }

    val cursorFactory = Meta.CursorFactory.create(Style.LIST, classOf[String], util.Arrays.asList())

    val rows = new util.ArrayList[Object]

    rowList.foreach {
      row =>
        val arrayList = new util.ArrayList[Object]()
        rowList.columnNames.foreach {
          columnName =>
            val columnOption = aliasColumnMap.get(columnName)
            if (columnOption.isDefined) {
              val value = row.getValue(columnName).asInstanceOf[AnyRef]
              columnOption.get.dataType match {
                case IntType(_, _, _, _, _) => arrayList.add(value)
                case DecType(_, _, _, _, _, _) => arrayList.add(value)
                case StrType(_, _, _) => arrayList.add(value)
                case DateType(format) => arrayList.add(value.toString)
                case TimestampType(format) => arrayList.add(value.toString)
                case _ =>
              }
            }
        }
        rows.add(arrayList.toArray)
    }

    // Create the signature and frame using the metadata and values
    val signature = Signature.create(columns, avaticaContext.sql, params, cursorFactory, Meta.StatementType.SELECT)
    val frame = Frame.create(0, true, rows)
        // And then create a ResultSetResponse
    new Service.ResultSetResponse(avaticaContext.connectionID, avaticaContext.statementID, false, signature, frame, -1, avaticaContext.rpcMetadataResponse)
  }
}
