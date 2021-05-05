package com.yahoo.maha.service.calcite.avatica;

import java.util
import org.apache.calcite.avatica.{AvaticaParameter, ColumnMetaData, ConnectionPropertiesImpl, Meta, MetaImpl}
import org.apache.calcite.avatica.Meta.{ConnectionProperties, CursorFactory, Frame, Signature, StatementHandle, StatementType, Style}
import org.apache.calcite.avatica.remote.Service._

import java.util.concurrent.atomic.AtomicInteger
import org.apache.calcite.avatica.remote.Service
import com.yahoo.maha.core.{PublicColumn, Schema}
import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.dimension.{PubCol, PublicDimColumn, PublicDimension}
import com.yahoo.maha.core.fact.PublicFact
import com.yahoo.maha.core.query.QueryRowList
import com.yahoo.maha.core.registry.Registry
import com.yahoo.maha.core.request.BaseRequest
import com.yahoo.maha.service.{MahaRequestContext, MahaService, RegistryConfig}
import com.yahoo.maha.service.calcite.{DescribeSqlNode, MahaCalciteSqlParser, SelectSqlNode}
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

    val registryConfigMap = mahaService.getMahaServiceConfig.registry
    require(registryConfigMap.contains(defaultRegistry), s"Failed to find the ${defaultRegistry} in registry config")
    val registry: Registry = registryConfigMap.get(defaultRegistry).get.registry

    val allFactMap = mahaService.getMahaServiceConfig.registry.map {
        case (regName, registryConfig: RegistryConfig) => {
            registryConfig.registry.factMap
        }
    }.flatten.toMap

    val cubeToRegistryMap = allFactMap.map(f => f._1._1 -> getRegistryForFact(f._1._1))

    val cubeToColsMap: Map[String, (Frame, Signature)] = cubeToRegistryMap.map(f=> f._1 -> getColsForCube(f._2, f._1))

    val tablesRequestSignature: Signature = {
        val columns = new util.ArrayList[ColumnMetaData]
        val params = new util.ArrayList[AvaticaParameter]
        val cursorFactory = CursorFactory.create(Style.LIST, classOf[String], util.Arrays.asList())
        tableMetaArray.zipWithIndex.foreach {
            case (columnName, index) => {
                columns.add(MetaImpl.columnMetaData(columnName, index, classOf[String], true))
            }
        }
        Signature.create(columns, "", params, cursorFactory, StatementType.SELECT)
    }
    require(tablesRequestSignature != null, s"tablesRequestSignature is null")

    val tableRequestFrame: Frame = {
        val rows = new util.ArrayList[Object]()
        allFactMap.foreach{
            case ((name, version), value) =>
                val row = Array(name, "fact", value.toString) //name, type, remarks(description)
                rows.add(row)
        }
        Frame.create(0, true, rows)
    }
    require(tableRequestFrame != null, s"tableRequestFrame is null")


    override def apply(catalogsRequest: Service.CatalogsRequest): Service.ResultSetResponse = {
        null
    }

    override def apply(schemasRequest: Service.SchemasRequest): Service.ResultSetResponse = null

    override def apply(tablesRequest: Service.TablesRequest): Service.ResultSetResponse = {
        new ResultSetResponse(tablesRequest.connectionId, -1, false, tablesRequestSignature, tableRequestFrame, -1, rpcMetadataResponse)
    }

    override def apply(tableTypesRequest: Service.TableTypesRequest): Service.ResultSetResponse = null

    override def apply(typeInfoRequest: Service.TypeInfoRequest): Service.ResultSetResponse = null

    override def apply(columnsRequest: Service.ColumnsRequest): Service.ResultSetResponse = {
        val cachedEntryOption: Option[(Frame, Signature)] = cubeToColsMap.get(columnsRequest.tableNamePattern)
        require(cachedEntryOption.isDefined, s"Failed to find the cube ${columnsRequest.tableNamePattern} in the cached cubeToColsMap map")
        val (frame, signature) = cachedEntryOption.get
        new ResultSetResponse(columnsRequest.connectionId, -1, false, signature, frame, -1, rpcMetadataResponse)
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

    def toComment(pubCol: PublicColumn):String = {
        s""" ${pubCol.alias}, allowed filters: ${pubCol.filters}, restricted schemas: ${pubCol.restrictedSchemas}, Is required: ${pubCol.required} """
    }

    def getDataType(dimCol: PublicColumn, publicFact: PublicFact): String = {
        val name = dimCol.name
        val list =  publicFact.factList.map(fact=> fact.columnsByNameMap.get(name)).filter(_.isDefined).map(_.get).toList
        if (list.nonEmpty) {
            list.head.dataType.getClass.getSimpleName
        } else ""
    }

    def getDataTypeFromDim(dimCol: PublicColumn, publicDim: PublicDimension): String = {
        val name = dimCol.name
        val list =  publicDim.dimList.map(d=> d.columnsByNameMap.get(name)).filter(_.isDefined).map(_.get).toList
        if(list.nonEmpty) {
            list.head.dataType.getClass.getSimpleName
        } else ""
    }

    def getSqlDataType(dimCol: PublicColumn, publicFact: PublicFact): Int = {
        val name = dimCol.name
        val list =  publicFact.factList.map(fact=> fact.columnsByNameMap.get(name)).filter(_.isDefined).map(_.get).toList
        if (list.nonEmpty) {
            list.head.dataType.sqlDataType
        } else java.sql.Types.OTHER
    }

    def getSqlDataTypeFromDim(dimCol: PublicColumn, publicDim: PublicDimension): Int = {
        val name = dimCol.name
        val list =  publicDim.dimList.map(d=> d.columnsByNameMap.get(name)).filter(_.isDefined).map(_.get).toList
        if(list.nonEmpty) {
            list.head.dataType.sqlDataType
        } else java.sql.Types.OTHER
    }

    def getColsForCube(registry: Registry, cube:String): (Frame, Signature) = {
        val columns = new util.ArrayList[ColumnMetaData]
        val params = new util.ArrayList[AvaticaParameter]
        val cursorFactory = CursorFactory.create(Style.LIST, classOf[String], util.Arrays.asList())
        columnMetaArray.zipWithIndex.foreach {
            case (columnName, index) => {
                columns.add(MetaImpl.columnMetaData(columnName, index, classOf[String], true))
            }
        }
        val signature: Signature = Signature.create(columns, "", params, cursorFactory, StatementType.SELECT)
        val pubFactOption = registry.getFact(cube)
        require(pubFactOption.isDefined, s"Failed to find the cube ${cube} in the registry fact map")
        val publicFact = pubFactOption.get
        val rows = new util.ArrayList[Object]()
        publicFact.dimCols.foreach {
            dimCol=>
                val row = Array(dimCol.alias, getSqlDataType(dimCol, publicFact), getDataType(dimCol, publicFact) , toComment(dimCol))
                rows.add(row)
        }
        publicFact.factCols.foreach {
            factCol=>
                val row = Array(factCol.alias, getSqlDataType(factCol, publicFact), getDataType(factCol, publicFact) , toComment(factCol))
                rows.add(row)
        }
        publicFact.foreignKeySources.foreach {
            dimensionCube =>
                val dimVersionOption = publicFact.dimToRevisionMap.get(dimensionCube)
                val dimCubeOption =  registry.getDimension(dimensionCube, dimVersionOption)
                dimCubeOption.map {
                    dim=>
                        dim.columnsByAliasMap.foreach {
                            case (alias, dimCol)=>
                                val row = Array(dimCol.alias, getSqlDataTypeFromDim(dimCol, dim), getDataTypeFromDim(dimCol, dim) , toComment(dimCol))
                                rows.add(row)
                        }
                }
        }
        val frame: Frame = Frame.create(0, true, rows)
        (frame, signature)
    }

    def getRegistryForFact(cube: String): Registry = {
        val registryOption = mahaService.getMahaServiceConfig.registry
          .map(rc => rc._2.registry -> rc._2.registry.getFact(cube))
          .filter(entry => entry._2.isDefined)
          .map(entry => entry._1)
          .headOption
        require(registryOption.isDefined, s"Failed to find the registry for ${cube}")
        registryOption.get
    }

    def execute(avaticaContext: AvaticaContext) : ExecuteResponse = {
        val responseList = new util.ArrayList[Service.ResultSetResponse]()
        val connectionID = avaticaContext.connectionID
        val sql = avaticaContext.sql
        val mahaSqlNode = calciteSqlParser.parse(sql, defaultSchema, defaultRegistry)
        mahaSqlNode match {
            case describeSqlNode: DescribeSqlNode => {
                val pubFactOption = registry.getFact(describeSqlNode.cube)
                require(pubFactOption.isDefined, s"Failed to find the cube ${describeSqlNode.cube} in the registry fact map")
                val publicFact = pubFactOption.get
                val columnsByAliasMap = pubFactOption.get.columnsByAliasMap

                val columns = new util.ArrayList[ColumnMetaData]
                val params = new util.ArrayList[AvaticaParameter]
                val cursorFactory = CursorFactory.create(Style.LIST, classOf[String], util.Arrays.asList())
                Array("Column Name", "Column Type", "Data Type", "Comment").zipWithIndex.foreach {
                    case (columnName, index) => {
                        columns.add(MetaImpl.columnMetaData(columnName, index, classOf[String], true))
                    }
                }
                val signature: Signature = Signature.create(columns, "", params, cursorFactory, StatementType.SELECT)
                val rows = new util.ArrayList[Object]()
                publicFact.dimCols.foreach {
                    dimCol=>
                        val row = Array(dimCol.alias, DIMENSION_COLUMN, getDataType(dimCol, publicFact) , toComment(dimCol))
                        rows.add(row)
                }
                publicFact.factCols.foreach {
                    factCol=>
                        val row = Array(factCol.alias, METRIC_COLUMN, getDataType(factCol, publicFact) , toComment(factCol))
                        rows.add(row)
                }
                publicFact.foreignKeySources.foreach {
                    dimensionCube =>
                    val dimVersionOption = publicFact.dimToRevisionMap.get(dimensionCube)
                    val dimCubeOption =  registry.getDimension(dimensionCube, dimVersionOption)
                        dimCubeOption.map {
                            dim=>
                            dim.columnsByAliasMap.foreach {
                                case (alias, dimCol)=>
                                    val row = Array(dimCol.alias, DIMENSION_JOIN_COLUMN, getDataTypeFromDim(dimCol, dim) , toComment(dimCol))
                                    rows.add(row)
                            }
                        }
                }
                val frame: Frame = Frame.create(0, true, rows)
                responseList.add(new ResultSetResponse(connectionID, -1, false, signature, frame, -1, rpcMetadataResponse))
            }
            case selectSqlNode: SelectSqlNode => {
                val reportingRequest = selectSqlNode.reportingRequest
                val userInfo = connectionUserInfoProvider.getUserInfo(connectionID)
                val requestJson = baseReportingRequest.serialize(reportingRequest)
                info(s"Got the maha SQL : ${sql}, userInfo : ${userInfo}")
                info(s"Translated sql ${sql} to  ${new String(requestJson)}")

                val mahaRequestContext = MahaRequestContext(defaultRegistry,
                    BucketParams(UserInfo(userInfo.userId, isInternal = true)),
                    reportingRequest,
                    requestJson,
                    Map.empty,
                    userInfo.userId,
                    userInfo.requestId
                )
                val mahaResults = executeFunction(mahaRequestContext, mahaService)
                responseList.add(rowListTransformer.map(mahaResults, AvaticaContext(connectionID, sql, avaticaContext.statementID, rpcMetadataResponse)))
            }
        }
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
    val METRIC_COLUMN = "Metric/Fact Column"
    val DIMENSION_COLUMN = "Dimension Column"
    val DIMENSION_JOIN_COLUMN = "Dimension Join Column"
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

    val tableMetaArray: Array[String] = Array("TABLE_NAME", "TABLE_TYPE", "REMARKS")

    val columnMetaArray: Array[String] = Array("COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "REMARKS")

}
