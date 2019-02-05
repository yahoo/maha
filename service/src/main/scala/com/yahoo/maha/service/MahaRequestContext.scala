package com.yahoo.maha.service

import com.yahoo.maha.core.bucketing.BucketParams
import com.yahoo.maha.core.request.ReportingRequest
import org.apache.commons.codec.digest.DigestUtils

import scala.collection.concurrent.TrieMap

/**
  * Created by hiral on 4/5/18.
  */
case class MahaRequestContext(registryName: String
                              , bucketParams: BucketParams
                              , reportingRequest: ReportingRequest
                              , rawJson: Array[Byte]
                              , context: Map[String, Any]
                              , requestId: String
                              , userId: String
                              , requestStartTime: Long = System.currentTimeMillis()
                             ) {
  lazy val mutableState = new TrieMap[String, Any]()
  val requestHashOption = {
    if(rawJson!=null) {
      Some(DigestUtils.md5Hex(rawJson))
    } else {
      None
    }
  }

}
