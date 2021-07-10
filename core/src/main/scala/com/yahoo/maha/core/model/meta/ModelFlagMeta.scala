package com.yahoo.maha.core.model.meta

/**
  *
  * - hasFactFilters
  * - hasMetricFilters
  * - hasNonFKFactFilters
  * - hasDimFilters
  * - hasNonFKDimFilters
  * - hasFactSortBy
  * - hasDimSortBy
  * - isFactDriven
  * - forceDimFriven
  * - forceFactDriven
  * - hasNonDrivingDimSortOrFilter
  * - hasDrivingDimNonFKNonPKSortBy
  * - hasNonDrivingDimNonFKNonPKFilter
  * - anyDimHasNonFKNonForceFilter
  * - includeRowCount
  * - isDebugEnabled
  * - isRequestingDistinct
  * - hasLowCardinalityDimFilters
  */
case class ModelFlagMeta (
                           hasFactFilters: Boolean
                           , hasMetricFilters: Boolean
                           , hasNonFKFactFilters: Boolean
                           , hasDimFilters: Boolean
                           , hasNonFKDimFilters: Boolean
                           , hasFactSortBy: Boolean
                           , hasDimSortBy: Boolean
                           , isFactDriven: Boolean
                           , forceDimDriven: Boolean
                           , forceFactDriven: Boolean
                           , hasNonDrivingDimSortOrFilter: Boolean
                           , hasDrivingDimNonFKNonPKSortBy: Boolean
                           , hasNonDrivingDimNonFKNonPKFilter: Boolean
                           , anyDimHasNonFKNonForceFilter: Boolean
                           , includeRowCount: Boolean
                           , isDebugEnabled: Boolean
                           , isRequestingDistinct:Boolean
                           , hasLowCardinalityDimFilters: Boolean
                         ) {
  override def toString: String =
    s"""${this.getClass.getSimpleName}:
       hasFactFilters: $hasFactFilters
       hasMetricFilters: $hasMetricFilters
       hasNonFKFactFilters: $hasNonFKFactFilters
       hasDimFilters: $hasDimFilters
       hasNonFKDimFilters: $hasNonFKDimFilters
       hasFactSortBy: $hasFactSortBy
       hasDimSortBy: $hasDimSortBy
       isFactDriven: $isFactDriven
       forceDimDriven: $forceDimDriven
       forceFactDriven: $forceFactDriven
       hasNonDrivingDimSortOrFilter: $hasNonDrivingDimSortOrFilter
       hasDrivingDimNonFKNonPKSortBy: $hasDrivingDimNonFKNonPKSortBy
       hasNonDrivingDimNonFKNonPKFilter: $hasNonDrivingDimNonFKNonPKFilter
       anyDimHasNonFKNonForceFilter: $anyDimHasNonFKNonForceFilter
       includeRowCount: $includeRowCount
       isDebugEnabled: $isDebugEnabled
       isRequestingDistinct: $isRequestingDistinct
       hasLowCardinalityDimFilters: $hasLowCardinalityDimFilters""".stripMargin
}
