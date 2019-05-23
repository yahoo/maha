package com.yahoo.maha.core.model

import com.yahoo.maha.core.model.meta.{ModelCandidateMeta, ModelCardinalityMeta, ModelColumnsMeta, ModelFilterMeta, ModelFlagMeta, ModelPassedParameterMeta, ModelReportingRequestMeta, ModelSortByMeta, RequestModelMeta}
import org.scalatest.{FunSuite, Matchers}
import com.yahoo.maha.core.whiteSpaceNormalised

class RequestModelV1Test extends FunSuite with Matchers {

  /**
    * Demonstrate what toString is expected to Print for RequestModelV1
    */
  test("Verify RequestModelV1 toString") {
    val modelCandidateMeta: ModelCandidateMeta = ModelCandidateMeta(None, Map.empty, null, Map.empty, null)
    val modelCardinalityMeta: ModelCardinalityMeta = ModelCardinalityMeta(Map.empty, None)
    val modelColumnsMeta: ModelColumnsMeta = ModelColumnsMeta(IndexedSeq[ColumnInfo](), IndexedSeq[SortByColumnInfo](), Set.empty)
    val modelFilterMeta: ModelFilterMeta = ModelFilterMeta(null, null, null, None, None, None, None, null, null)
    val modelFlagMeta: ModelFlagMeta = ModelFlagMeta(false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false)
    val modelPassedParameterMeta: ModelPassedParameterMeta = ModelPassedParameterMeta(null, null, null, None)
    val modelReportingRequestMeta: ModelReportingRequestMeta = ModelReportingRequestMeta("", null, 0, 0, 0, 0)
    val modelSortByMeta: ModelSortByMeta  = ModelSortByMeta(Map.empty, Map.empty)
    val requestModelMeta: RequestModelMeta = RequestModelMeta(
      modelCandidateMeta, modelCardinalityMeta, modelColumnsMeta, modelFilterMeta, modelFlagMeta, modelPassedParameterMeta, modelReportingRequestMeta, modelSortByMeta
    )
    val v1Model : RequestModelV1 = RequestModelV1(requestModelMeta)

    val modelAsString = v1Model.toString
    val expectedReturnString: String =
      s"""
         |RequestModelV1:
         |    RequestModelMeta:
         |      ModelCandidateMeta:
         |       bestCandidatesOption: None
         |       factSchemaRequiredAliasesMap: Map()
         |       dimensionsCandidates: null
         |       requestedFkAliasToPublicDimensionMap: Map()
         |       dimensionRelations: null
         |      ModelCardinalityMeta:
         |       factCost: Map()
         |       dimCardinalityEstimateOption: None
         |      ModelColumnsMeta:
         |       requestCols: Vector()
         |       requestSortByCols: Vector()
         |       dimColumnAliases: Set()
         |      ModelFilterMeta:
         |       factFilters: null
         |       utcTimeDayFilter: null
         |       localTimeDayFilter: null
         |       utcTimeHourFilterOption: None
         |       localTimeHourFilterOption: None
         |       utcTimeMinuteFilterOption: None
         |       localTimeMinuteFilterOption: None
         |       outerFilters: null
         |       orFilterMeta: null
         |      ModelFlagMeta:
         |       hasFactFilters: false
         |       hasMetricFilters: false
         |       hasNonFKFactFilters: false
         |       hasDimFilters: false
         |       hasNonFKDimFilters: false
         |       hasFactSortBy: false
         |       hasDimSortBy: false
         |       isFactDriven: false
         |       forceDimDriven: false
         |       forceFactDriven: false
         |       hasNonDrivingDimSortOrFilter: false
         |       hasDrivingDimNonFKNonPKSortBy: false
         |       hasNonDrivingDimNonFKNonPKFilter: false
         |       anyDimHasNonFKNonForceFilter: false
         |       includeRowCount: false
         |       isDebugEnabled: false
         |       isRequestingDistinct: false
         |       hasLowCardinalityDimFilters: false
         |      ModelPassedParameterMeta:
         |       schema: null
         |       requestType: null
         |       additionalParameters: null
         |       queryGrainOption: None
         |      ModelReportingRequestMeta:
         |       cube:
         |       reportingRequest: null
         |       requestedDaysWindow: 0
         |       requestedDaysLookBack: 0
         |       startIndex: 0
         |       maxRows: 0
         |      ModelSortByMeta:
         |       factSortByMap: Map()
         |       dimSortByMap: Map()
       """.stripMargin
    modelAsString should equal (expectedReturnString) (after being whiteSpaceNormalised)
  }
}
