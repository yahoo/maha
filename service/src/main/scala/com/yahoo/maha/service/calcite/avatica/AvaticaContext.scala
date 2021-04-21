package com.yahoo.maha.service.calcite.avatica

import org.apache.calcite.avatica.remote.Service.RpcMetadataResponse

case class AvaticaContext(connectionID: String, sql: String, statementID: Int, rpcMetadataResponse: RpcMetadataResponse)
