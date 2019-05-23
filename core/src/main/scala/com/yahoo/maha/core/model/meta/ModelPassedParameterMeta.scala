package com.yahoo.maha.core.model.meta

import com.yahoo.maha.core.{Grain, Schema}
import com.yahoo.maha.core.request.{Parameter, RequestType}

/**
  * Passed in Additional Parameters for RequestModel, including:
  * - Schema (forced or derived)
  * - RequestType (forced or derived)
  * - additionalParameters (all forced)
  * - queryGrain (derived)
  */
case class ModelPassedParameterMeta (
                                    schema: Schema
                                    , requestType: RequestType
                                    , additionalParameters: Map[Parameter, Any]
                                    , queryGrainOption: Option[Grain]
                                    ) {

}
