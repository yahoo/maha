package com.yahoo.maha.core.model.meta

/**
  * Contains the metadata of its current RequestModel, including:
  * - RequestParams
  * - Candidate Data
  * - Request Column Info
  * - ReportingRequest data
  * - SortBy Info
  * - Cardinality estimates
  * - Passed in Filters
  * - Flags, either passed in or derived based on other members
  */
case class RequestModelMeta(
                           modelCandidateMeta: ModelCandidateMeta
                           , modelCardinalityMeta: ModelCardinalityMeta
                           , modelColumnsMeta: ModelColumnsMeta
                           , modelFilterMeta: ModelFilterMeta
                           , modelFlagMeta: ModelFlagMeta
                           , modelPassedParameterMeta: ModelPassedParameterMeta
                           , modelReportingRequestMeta: ModelReportingRequestMeta
                           , modelSortByMeta: ModelSortByMeta
                           ) {
  override def toString: String =
    s"""  ${this.getClass.getSimpleName}:
      ${modelCandidateMeta.toString}
      ${modelCardinalityMeta.toString}
      ${modelColumnsMeta.toString}
      ${modelFilterMeta.toString}
      ${modelFlagMeta.toString}
      ${modelPassedParameterMeta.toString}
      ${modelReportingRequestMeta.toString}
      ${modelSortByMeta.toString}"""

}
