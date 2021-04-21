package com.yahoo.maha.service.calcite.avatica;

import java.util

import org.apache.calcite.avatica.ConnectionPropertiesImpl
import org.apache.calcite.avatica.Meta.{ConnectionProperties, StatementHandle}
import org.apache.calcite.avatica.remote.Service._
import java.util.concurrent.atomic.AtomicInteger

import org.apache.calcite.avatica.remote.Service
import com.yahoo.maha.core.Schema
import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.query.QueryRowList
import com.yahoo.maha.core.request.BaseRequest
import com.yahoo.maha.service.{MahaRequestContext, MahaService}
import com.yahoo.maha.service.calcite.MahaCalciteSqlParser
import grizzled.slf4j.Logging
import org.apache.commons.codec.digest.DigestUtils

/*
 Maha Avatica service implementation of JDBC Request Handler with calcite parsers and executors as input
 */
abstract class MahaAvaticaService extends Service {

}
class DefaultMahaAvaticaService(executeFunction: (MahaRequestContext, MahaService) => QueryRowList,
                                calciteSqlParser: MahaCalciteSqlParser,
                                mahaService: MahaService,
                                rowListTransformer: AvaticaRowListTransformer,
                                schemaMapper: (String) => Schema,
                                defaultRegistry: String,
                                defaultSchema: Schema,
                                baseReportingRequest:BaseRequest,
                                connectionUserInfoProvider: ConnectionUserInfoProvider
                        ) extends MahaAvaticaService with Logging {
    import MahaAvaticaServiceHelper._

    val rpcMetadataResponse = new RpcMetadataResponse(MahaAvaticaServiceHelper.hostname)
    val statementIdCounter = new AtomicInteger(1)

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
        new PrepareResponse(new StatementHandle(prepareRequest.connectionId, statementIdCounter.getAndIncrement() , null),rpcMetadataResponse);
    }

    override def apply(executeRequest: Service.ExecuteRequest): Service.ExecuteResponse = {
        execute(AvaticaContext(executeRequest.statementHandle.connectionId, executeRequest.statementHandle.signature.sql, executeRequest.statementHandle.id, rpcMetadataResponse))
    }

    override def apply(prepareAndExecuteRequest: Service.PrepareAndExecuteRequest): Service.ExecuteResponse = {
        execute(AvaticaContext(prepareAndExecuteRequest.connectionId, prepareAndExecuteRequest.sql, prepareAndExecuteRequest.statementId, rpcMetadataResponse))
    }

    override def apply(syncResultsRequest: Service.SyncResultsRequest): Service.SyncResultsResponse =  {
        new SyncResultsResponse(true, false, rpcMetadataResponse)
    }

    override def apply(fetchRequest: Service.FetchRequest): Service.FetchResponse = null

    override def apply(createStatementRequest: Service.CreateStatementRequest): Service.CreateStatementResponse = {
        logger.info("Got createStatementRequest request "+createStatementRequest)
        new CreateStatementResponse(createStatementRequest.connectionId, statementIdCounter.getAndIncrement(), rpcMetadataResponse)
    }

    override def apply(closeStatementRequest: Service.CloseStatementRequest): Service.CloseStatementResponse = {
        new CloseStatementResponse(rpcMetadataResponse)
    }

    override def apply(openConnectionRequest: Service.OpenConnectionRequest): Service.OpenConnectionResponse = {
        val infoMap = openConnectionRequest.info
        val userID = infoMap.getOrDefault("userId", CalciteAvaticaUser)
        val requestId = infoMap.getOrDefault("requestId", MahaAvaticaServiceHelper.getRequestID(CalciteAvaticaUser, openConnectionRequest.connectionId))
        val schemaStr = infoMap.getOrDefault("schema", defaultSchema.entryName)
        val schema = schemaMapper(schemaStr)
        val connectionUserInfo = ConnectionUserInfo(userID, requestId)
        connectionUserInfo.setSchema(schema)
        connectionUserInfoProvider.store(openConnectionRequest.connectionId, connectionUserInfo)
        info(s"Open Connection for ${connectionUserInfo} with Schema: ${schema}")
        new OpenConnectionResponse(rpcMetadataResponse)
    }

    override def apply(closeConnectionRequest: Service.CloseConnectionRequest): Service.CloseConnectionResponse =  {
       connectionUserInfoProvider.invalidate(closeConnectionRequest.connectionId)
        info(s"Closing Connection, id: ${closeConnectionRequest.connectionId} ")
        new CloseConnectionResponse(rpcMetadataResponse)
    }

    override def apply(connectionSyncRequest: Service.ConnectionSyncRequest): Service.ConnectionSyncResponse = {
        val connectionProps:ConnectionProperties = new ConnectionPropertiesImpl()
        new ConnectionSyncResponse(connectionProps, rpcMetadataResponse)
    }

    override def apply(databasePropertyRequest: Service.DatabasePropertyRequest): Service.DatabasePropertyResponse = null

    override def apply(commitRequest: Service.CommitRequest): Service.CommitResponse = null

    override def apply(rollbackRequest: Service.RollbackRequest): Service.RollbackResponse = null

    override def apply(prepareAndExecuteBatchRequest: Service.PrepareAndExecuteBatchRequest): Service.ExecuteBatchResponse = {
        val updateCounts =  new Array[Long](1)
        new ExecuteBatchResponse(prepareAndExecuteBatchRequest.connectionId, prepareAndExecuteBatchRequest.statementId, updateCounts , false, rpcMetadataResponse)
    }

    override def apply(executeBatchRequest: Service.ExecuteBatchRequest): Service.ExecuteBatchResponse = null

    override def setRpcMetadata(rpcMetadataResponse: Service.RpcMetadataResponse): Unit = ???

    def execute(avaticaContext: AvaticaContext) : ExecuteResponse = {
        val responseList = new util.ArrayList[Service.ResultSetResponse]()
        val connectionID = avaticaContext.connectionID
        val sql = avaticaContext.sql
        val reportingRequest = calciteSqlParser.parse(sql, defaultSchema, defaultRegistry)
        val userInfo = connectionUserInfoProvider.getUserInfo(connectionID)
        val requestJson = baseReportingRequest.serialize(reportingRequest)
        info(s"Got the maha SQL : ${sql}, userInfo : ${userInfo}")
        info(s"Translated sql ${sql} to  ${new String(requestJson)}")

        val mahaRequestContext =  MahaRequestContext(defaultRegistry,
            BucketParams(UserInfo(userInfo.userId, isInternal = true)),
            reportingRequest,
            requestJson,
            Map.empty,
            userInfo.userId,
            userInfo.requestId
        )
        val mahaResults = executeFunction(mahaRequestContext, mahaService)
        responseList.add(rowListTransformer.map(mahaResults, AvaticaContext(connectionID, sql, avaticaContext.statementID, rpcMetadataResponse)))
        new ExecuteResponse(responseList, false, rpcMetadataResponse)
    }
}

case class ConnectionUserInfo(userId: String, requestId: String) {
    var schemaOption:Option[Schema] = None
    def setSchema(schema: Schema): Unit = {
        schemaOption = Some(schema)
    }

    override def toString: String = {
        s"ConnectionUserInfo{UserID=${userId}, requestId=${requestId}, schema = ${schemaOption}}"
    }
}

object MahaAvaticaServiceHelper extends Logging {

    val CalciteAvaticaUser = "calcite-avatica"
    def getRequestID(userId:String, connectionId:String):String= {
        DigestUtils.md5(s"${userId}${System.currentTimeMillis()}${connectionId}").toString
    }

    val hostname: String = {
        try {
            java.net.InetAddress.getLocalHost.getHostName
        } catch {
            case e=>
            error("Failed to get hostname", e);
            "localhost"
        }
    }

}
