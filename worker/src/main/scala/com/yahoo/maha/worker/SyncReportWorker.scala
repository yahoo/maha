package com.yahoo.maha.worker

import com.yahoo.maha.worker.AckStatus.AckStatus
import com.yahoo.maha.worker.proto.MahaWorkerReportingProto.MahaCustomReportRequest
import com.yahoo.maha.worker.state.WorkerStateReporter

/*
    Created by pranavbhole on 9/26/18
*/
case class SyncReportWorker(workerConfig: WorkerConfig
                            , workerStateReporter: WorkerStateReporter) extends Worker(workerConfig, workerStateReporter) {
  override def work(getData: Array[Byte]): AckStatus =  ???

}
