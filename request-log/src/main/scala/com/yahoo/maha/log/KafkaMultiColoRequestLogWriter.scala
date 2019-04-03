package com.yahoo.maha.log

import com.yahoo.maha.proto.MahaRequestLog

/*
    Created by pranavbhole on 3/15/19
*/
case class MultiColoMahaRequestLogWriter(list: List[MahaRequestLogWriter]) extends MahaRequestLogWriter {

  require(list!=null && !list.isEmpty, "Found null or empty list in the MultiColo Request Log writer")

  override def write(reqLogBuilder: MahaRequestLog.MahaRequestProto): Unit = {
    list.foreach(rlw=> rlw.write(reqLogBuilder))
  }

  override def validate(reqLogBuilder: MahaRequestLog.MahaRequestProto): Unit = {
    //validation is to be done in the respective MahaRequestLogWriter
  }
}
