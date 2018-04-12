// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.api.jersey

import java.io.OutputStream
import javax.ws.rs.core.StreamingOutput

import com.yahoo.maha.core._
import com.yahoo.maha.service.curators.CuratorResult
import com.yahoo.maha.service.datasource.IngestionTimeUpdater
import com.yahoo.maha.service.output.JsonOutputFormat

class JsonStreamingOutput(override val resultList: IndexedSeq[CuratorResult],
                               override val ingestionTimeUpdaterMap : Map[Engine, IngestionTimeUpdater] = Map.empty
                              ) extends JsonOutputFormat(resultList, ingestionTimeUpdaterMap) with StreamingOutput {


  override def write(outputStream: OutputStream): Unit = {
    writeStream(outputStream)
  }
}

