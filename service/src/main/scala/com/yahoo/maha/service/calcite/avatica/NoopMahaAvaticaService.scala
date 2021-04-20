package com.yahoo.maha.service.calcite.avatica
import org.apache.calcite.avatica.remote.Service

class NoopMahaAvaticaService extends MahaAvaticaService {

  override def apply(catalogsRequest: Service.CatalogsRequest): Service.ResultSetResponse = {
    null
  }

  override def apply(schemasRequest: Service.SchemasRequest): Service.ResultSetResponse = {
    null
  }


  override def apply(tablesRequest: Service.TablesRequest): Service.ResultSetResponse = {
    null
  }

  override def apply(tableTypesRequest: Service.TableTypesRequest): Service.ResultSetResponse = {
    null
  }

  override def apply(typeInfoRequest: Service.TypeInfoRequest): Service.ResultSetResponse = {
    null
  }

  override def apply(columnsRequest: Service.ColumnsRequest): Service.ResultSetResponse = {
    null
  }

  override def apply(prepareRequest: Service.PrepareRequest): Service.PrepareResponse = {
    null
  }

  override def apply(executeRequest: Service.ExecuteRequest): Service.ExecuteResponse = {
    null
  }

  override def apply(prepareAndExecuteRequest: Service.PrepareAndExecuteRequest): Service.ExecuteResponse = {
    null
  }

  override def apply(syncResultsRequest: Service.SyncResultsRequest): Service.SyncResultsResponse = {
    null
  }

  override def apply(fetchRequest: Service.FetchRequest): Service.FetchResponse = {
    null
  }

  override def apply(createStatementRequest: Service.CreateStatementRequest): Service.CreateStatementResponse = {
    null
  }

  override def apply(closeStatementRequest: Service.CloseStatementRequest): Service.CloseStatementResponse = {
    null
  }

  override def apply(openConnectionRequest: Service.OpenConnectionRequest): Service.OpenConnectionResponse = {
    null
  }

  override def apply(closeConnectionRequest: Service.CloseConnectionRequest): Service.CloseConnectionResponse = {
    null
  }

  override def apply(connectionSyncRequest: Service.ConnectionSyncRequest): Service.ConnectionSyncResponse = {
    null
  }

  override def apply(databasePropertyRequest: Service.DatabasePropertyRequest): Service.DatabasePropertyResponse = {
    null
  }

  override def apply(commitRequest: Service.CommitRequest): Service.CommitResponse = {
    null
  }

  override def apply(rollbackRequest: Service.RollbackRequest): Service.RollbackResponse = {
    null
  }

  override def apply(prepareAndExecuteBatchRequest: Service.PrepareAndExecuteBatchRequest): Service.ExecuteBatchResponse = {
    null
  }

  override def apply(executeBatchRequest: Service.ExecuteBatchRequest): Service.ExecuteBatchResponse = {
    null
  }

  override def setRpcMetadata(rpcMetadataResponse: Service.RpcMetadataResponse): Unit = {
    null
  }
}
