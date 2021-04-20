/*
package com.yahoo.maha.service.calcite.avatica

import java.util

import org.apache.calcite.avatica.ColumnMetaData.AvaticaType
import org.apache.calcite.avatica.{AvaticaParameter, ColumnMetaData, ConnectionPropertiesImpl, MetaImpl}
import org.apache.calcite.avatica.Meta.{ConnectionProperties, CursorFactory, Frame, Signature, StatementHandle, Style}
import org.apache.calcite.avatica.remote.Service.{CloseConnectionResponse, CloseStatementResponse, ConnectionSyncResponse, CreateStatementResponse, ExecuteBatchResponse, ExecuteResponse, OpenConnectionRequest, OpenConnectionResponse, PrepareResponse, ResultSetResponse, RpcMetadataResponse, SyncResultsResponse}

import org.apache.calcite.avatica.ColumnMetaData
import java.sql.Types

import org.apache.calcite.avatica.Meta
import org.apache.calcite.avatica.remote.Service





/*
Mock service implementation of JDBC Request Handler
 */
class MahaAvaticaService1 extends Service {
    val rpcMetadataResponse = new RpcMetadataResponse("localhost")
    override def apply(catalogsRequest: Service.CatalogsRequest): Service.ResultSetResponse = {
        null
    }

    override def apply(schemasRequest: Service.SchemasRequest): Service.ResultSetResponse = null

    override def apply(tablesRequest: Service.TablesRequest): Service.ResultSetResponse = null

    override def apply(tableTypesRequest: Service.TableTypesRequest): Service.ResultSetResponse = null

    override def apply(typeInfoRequest: Service.TypeInfoRequest): Service.ResultSetResponse = null

    override def apply(columnsRequest: Service.ColumnsRequest): Service.ResultSetResponse = {
        //new ResultSetResponse(connectionID, 1, false, null, null, null, rpc )
        // TODO : Domain
        null
    }

    override def apply(prepareRequest: Service.PrepareRequest): Service.PrepareResponse = {
        val signature = AvaticaHelper.getResponses(prepareRequest.connectionId, prepareRequest.sql, 1).stream().filter(s=> s.isInstanceOf[ResultSetResponse]).findFirst().get().asInstanceOf[ResultSetResponse].signature;
        new PrepareResponse(new StatementHandle(prepareRequest.connectionId,1, signature),rpcMetadataResponse);
    }

    override def apply(executeRequest: Service.ExecuteRequest): Service.ExecuteResponse = {
        val response = AvaticaHelper.getResponses(executeRequest.statementHandle.connectionId, executeRequest.statementHandle.connectionId, 1).stream().filter(s=> s.isInstanceOf[ResultSetResponse]).findFirst().get().asInstanceOf[ResultSetResponse];
        val list = new util.ArrayList[ResultSetResponse]();
        list.add(response)
        new ExecuteResponse(list, false, rpcMetadataResponse)
    }

    override def apply(prepareAndExecuteRequest: Service.PrepareAndExecuteRequest): Service.ExecuteResponse = {
        val responseList = new util.ArrayList[Service.ResultSetResponse]()

        val mahaResponse =  MahaAvaticaServiceHelper.avaticaResultToResponse(MahaAvaticaServiceHelper.getMahaResult(),
            prepareAndExecuteRequest.connectionId,
            prepareAndExecuteRequest.sql,
            prepareAndExecuteRequest.statementId)
         responseList.add(mahaResponse)
        //responseList.add(AvaticaHelper.getResponses(prepareAndExecuteRequest.connectionId, prepareAndExecuteRequest.sql, prepareAndExecuteRequest.statementId).stream().filter(s=> s.isInstanceOf[ResultSetResponse]).findFirst().get().asInstanceOf[ResultSetResponse]);
        new ExecuteResponse(responseList, false, rpcMetadataResponse)
    }

    override def apply(syncResultsRequest: Service.SyncResultsRequest): Service.SyncResultsResponse =  {
        new SyncResultsResponse(true, false, rpcMetadataResponse)
    }

    override def apply(fetchRequest: Service.FetchRequest): Service.FetchResponse = null

    override def apply(createStatementRequest: Service.CreateStatementRequest): Service.CreateStatementResponse = {
        print("Got createStatementRequest request "+createStatementRequest)
        new CreateStatementResponse(createStatementRequest.connectionId, 1, rpcMetadataResponse)
    }

    override def apply(closeStatementRequest: Service.CloseStatementRequest): Service.CloseStatementResponse = {
        new CloseStatementResponse(rpcMetadataResponse)
    }

    override def apply(openConnectionRequest: Service.OpenConnectionRequest): Service.OpenConnectionResponse = {
        new OpenConnectionResponse(rpcMetadataResponse)
    }

    override def apply(closeConnectionRequest: Service.CloseConnectionRequest): Service.CloseConnectionResponse =  {
        new CloseConnectionResponse(rpcMetadataResponse)
    }

    override def apply(connectionSyncRequest: Service.ConnectionSyncRequest): Service.ConnectionSyncResponse = {
        val connectionProps:ConnectionProperties = new ConnectionPropertiesImpl();
        new ConnectionSyncResponse(connectionProps, rpcMetadataResponse)
    }

    override def apply(databasePropertyRequest: Service.DatabasePropertyRequest): Service.DatabasePropertyResponse = null

    override def apply(commitRequest: Service.CommitRequest): Service.CommitResponse = null

    override def apply(rollbackRequest: Service.RollbackRequest): Service.RollbackResponse = null

    override def apply(prepareAndExecuteBatchRequest: Service.PrepareAndExecuteBatchRequest): Service.ExecuteBatchResponse = {
        val updateCounts =  new Array[Long](1);
        new ExecuteBatchResponse(prepareAndExecuteBatchRequest.connectionId, 1,updateCounts , false, rpcMetadataResponse)
    }

    override def apply(executeBatchRequest: Service.ExecuteBatchRequest): Service.ExecuteBatchResponse = null

    override def setRpcMetadata(rpcMetadataResponse: Service.RpcMetadataResponse): Unit = ???
}

object MahaAvaticaServiceHelper {
    val json =
        s"""
           |{
           |    "cube": "performance_stats",
           |    "fields": [
           |        {
           |            "field": "Advertiser ID",
           |            "alias": null,
           |            "value": null
           |        },
           |        {
           |            "field": "Campaign ID",
           |            "alias": null,
           |            "value": null
           |        },
           |        {
           |            "field": "Impressions",
           |            "alias": null,
           |            "value": null
           |        },
           |        {
           |            "field": "Clicks",
           |            "alias": null,
           |            "value": null
           |        },
           |        {
           |            "field": "CTR",
           |            "alias": null,
           |            "value": null
           |        },
           |        {
           |            "field": "Spend",
           |            "alias": null,
           |            "value": null
           |        },
           |        {
           |            "field": "Conversions",
           |            "alias": null,
           |            "value": null
           |        }
           |    ],
           |    "filters": [
           |        {
           |            "field": "Advertiser ID",
           |            "operator": "=",
           |            "value": "1603510"
           |        },
           |        {
           |            "field": "Day",
           |            "operator": "Between",
           |            "from": "2021-03-21",
           |            "to": "2021-03-27"
           |        }
           |    ]
           |}
           |""".stripMargin.stripLineEnd.replaceAll("\n", "")

    def getMahaResult(): AvaticaResult = {
        implicit val formats = org.json4s.DefaultFormats
        import org.json4s._
        import org.json4s.jackson.JsonMethods._

        val response = Http("https://reporttest.api.streamads.yahoo.com:4443/na_reporting_cb_ws/public/api/v4/er/schemas/advertiser/sync")
          .header("Content-Type", "application/json")
          .header("X-Request-Id", "avatica-pranav-poc")
          .postData(json).asString
        println(response.body)
        (parse(response.body)).extract[AvaticaResult]
    }

    case class AvaticaResultField(fieldName:String)
    case class AvaticaResultHeader(fields: Array[AvaticaResultField])
    case class AvaticaResult(header: AvaticaResultHeader, rows: Array[Array[Any]], curators: Any)

    def avaticaResultToResponse(avaticaResult: AvaticaResult, connectionID: String, sql: String, statementID: Int): ResultSetResponse = {
        val columns = new util.ArrayList[ColumnMetaData]
        val params = new util.ArrayList[AvaticaParameter]

        avaticaResult.header.fields.toList.foreach{
            f=>
                columns.add(MetaImpl.columnMetaData(f.fieldName, 0, classOf[String], true))
                params.add(new AvaticaParameter(false, 10, 0, Types.VARCHAR, "VARCHAR", classOf[String].getName, f.fieldName))
        }

        val cursorFactory = Meta.CursorFactory.create(Style.LIST, classOf[String], util.Arrays.asList("str"))
        // The row values
        val rows = new util.ArrayList[Object]
        import scala.collection.JavaConverters._
        avaticaResult.rows.toList.foreach(row=> {
            rows.add(row.toList.map(String.valueOf(_)).toArray)
        })

        // Create the signature and frame using the metadata and values
        val signature = Signature.create(columns, sql, params, cursorFactory, Meta.StatementType.SELECT)
        val frame = Frame.create(0, true, rows)

        // And then create a ResultSetResponse
         new Service.ResultSetResponse(connectionID, statementID, false, signature, frame, -1, new RpcMetadataResponse("localhost"))
    }
}
*/
