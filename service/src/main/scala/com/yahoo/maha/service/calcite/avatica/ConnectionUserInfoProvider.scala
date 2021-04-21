package com.yahoo.maha.service.calcite.avatica

import java.util.concurrent.ConcurrentHashMap

/*
 When JDBC establishes the connection, provider stores userInfo in metadata and validates queries based on this connection id.
 In the case of multi host deployments, this needs be implementation of common store across all host
 */
trait ConnectionUserInfoProvider {
  def store(connectionId:String, userInfo: ConnectionUserInfo)
  def getUserInfo(connectionId:String): ConnectionUserInfo
  def invalidate(connectionId:String)
}

class DefaultConnectionUserInfoProvider extends ConnectionUserInfoProvider {
  val connectionIDToUserInfoMap = new ConcurrentHashMap[String, ConnectionUserInfo]()

  override def getUserInfo(connectionId: String): ConnectionUserInfo = {
    connectionIDToUserInfoMap.getOrDefault(connectionId,
      ConnectionUserInfo(MahaAvaticaServiceHelper.CalciteAvaticaUser, MahaAvaticaServiceHelper.getRequestID(MahaAvaticaServiceHelper.CalciteAvaticaUser, connectionId))
    )
  }

  override def store(connectionId: String, userInfo: ConnectionUserInfo): Unit = {
    connectionIDToUserInfoMap.put(connectionId, userInfo)
  }

  override def invalidate(connectionId: String): Unit = {
    connectionIDToUserInfoMap.remove(connectionId)
  }
}


