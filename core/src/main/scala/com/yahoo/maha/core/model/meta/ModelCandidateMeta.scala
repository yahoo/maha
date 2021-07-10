package com.yahoo.maha.core.model.meta

import com.yahoo.maha.core.DimensionRelations
import com.yahoo.maha.core.dimension.PublicDimension
import com.yahoo.maha.core.fact.BestCandidates
import com.yahoo.maha.core.model.DimensionCandidate

import scala.collection.SortedSet

/**
  * Candidate info for RequestModel, including:
  * - bestCandidates
  * - factSchemaRequiredAliasMap - best candidates schema to required request/filter aliases
  * - dimensionsCandidates - All possible used Dimension tables
  * - requestedFkAliasToPublicDimensionMap - Aliases keying into foreign Dimensions
  * - dimensionRelations
  */
case class ModelCandidateMeta (
                              bestCandidatesOption: Option[BestCandidates]
                              , factSchemaRequiredAliasesMap: Map[String, Set[String]]
                              , dimensionsCandidates: SortedSet[DimensionCandidate]
                              , requestedFkAliasToPublicDimensionMap: Map[String, PublicDimension]
                              , dimensionRelations: DimensionRelations
                              ) {
  override def toString: String =
    s"""${this.getClass.getSimpleName}:
       bestCandidatesOption: $bestCandidatesOption
       factSchemaRequiredAliasesMap: $factSchemaRequiredAliasesMap
       dimensionsCandidates: $dimensionsCandidates
       requestedFkAliasToPublicDimensionMap: $requestedFkAliasToPublicDimensionMap
       dimensionRelations: $dimensionRelations""".stripMargin
}
