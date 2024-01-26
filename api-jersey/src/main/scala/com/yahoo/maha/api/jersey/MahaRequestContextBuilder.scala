package com.yahoo.maha.api.jersey

import com.yahoo.maha.core.bucketing.BucketParams
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.service.MahaRequestContext
import jakarta.ws.rs.container.ContainerRequestContext

trait MahaRequestContextBuilder {
  def build(registryName: String
            , bucketParams: BucketParams
            , reportingRequest: ReportingRequest
            , rawJson: Array[Byte]
            , requestId: String
            , userId: String
            , requestStartTime: Long
            , containerRequestContext: ContainerRequestContext
           ): MahaRequestContext
}

class DefaultMahaRequestContextBuilder extends MahaRequestContextBuilder {
  def build(registryName: String
            , bucketParams: BucketParams
            , reportingRequest: ReportingRequest
            , rawJson: Array[Byte]
            , requestId: String
            , userId: String
            , requestStartTime: Long
            , containerRequestContext: ContainerRequestContext
           ): MahaRequestContext = {
    MahaRequestContext(registryName
      , bucketParams
      , reportingRequest
      , rawJson
      , Map.empty
      , requestId
      , userId
      , requestStartTime
    )
  }
}
