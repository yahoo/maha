// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core.dimension.{Dimension, PublicDimension}
import com.yahoo.maha.core.fact.FactBestCandidate
import com.yahoo.maha.core._
import grizzled.slf4j.Logging

import scala.collection.SortedSet

/**
 * Created by jians on 10/22/15.
 */

sealed trait QueryContext {
  def requestModel: RequestModel
  def indexAliasOption: Option[String]
  def primaryTableName: String
  def joinTypeHelper : JoinTypeHelper
}

sealed trait DimensionQueryContext extends QueryContext {
  def dims: SortedSet[DimensionBundle]
  def primaryTableName: String = dims.last.dim.name
}

trait FactualQueryContext extends QueryContext {
  def factBestCandidate: FactBestCandidate
  def primaryTableName: String = factBestCandidate.fact.name
}

trait QueryType
case object DimOnlyQuery extends QueryType
case object FactOnlyQuery extends QueryType
case object DimFactQuery extends QueryType
case object DimFactOuterGroupByQuery extends QueryType

case class DimQueryContext private[query](dims: SortedSet[DimensionBundle],
                           requestModel: RequestModel,
                           indexAliasOption: Option[String],
                           queryAttributes: QueryAttributes= QueryAttributes.empty) extends DimensionQueryContext {
  override def joinTypeHelper: JoinTypeHelper = NoopJoinTypeHelper
}

case class FactQueryContext private[query](factBestCandidate: FactBestCandidate,
                            requestModel: RequestModel,
                            indexAliasOption: Option[String],
                            queryAttributes: QueryAttributes) extends FactualQueryContext {
  override def joinTypeHelper: JoinTypeHelper = NoopJoinTypeHelper
}

case class CombinedQueryContext private[query](dims: SortedSet[DimensionBundle],
                                factBestCandidate: FactBestCandidate,
                                requestModel: RequestModel,
                                queryAttributes: QueryAttributes) extends DimensionQueryContext with FactualQueryContext {
  val indexAliasOption = None
  override def primaryTableName: String = {
    if(requestModel.isDimDriven) {
      dims.last.dim.name
    } else {
      factBestCandidate.fact.name
    }
  }

  override def joinTypeHelper: JoinTypeHelper = DimFactJoinTypeHelper(factBestCandidate, requestModel)
}

case class DimFactOuterGroupByQueryQueryContext(dims: SortedSet[DimensionBundle],
                                                factBestCandidate: FactBestCandidate,
                                                requestModel: RequestModel,
                                                queryAttributes: QueryAttributes) extends DimensionQueryContext with FactualQueryContext {
  override def indexAliasOption: Option[String] = None
  override def primaryTableName: String = factBestCandidate.fact.name
  override def joinTypeHelper: JoinTypeHelper = DimFactJoinTypeHelper(factBestCandidate, requestModel)
}

case class DimensionBundle(dim: Dimension
                           , publicDim: PublicDimension
                           , fields: Set[String]
                           , filters: SortedSet[Filter]
                           , upperCandidates: List[Dimension]
                           , publicUpperCandidatesMap: Map[String, PublicDimension]
                           , lowerCandidates: List[Dimension]
                           , publicLowerCandidatesMap: Map[String, PublicDimension]
                           , partitionColAliasToColMap: Map[String, Column]
                           , isDrivingDimension: Boolean
                           , hasNonFKOrForcedFilters: Boolean
                           , hasNonFKSortBy: Boolean
                           , hasNonPushDownFilters: Boolean
                           , hasPKRequested: Boolean
                            ) {
  def debugString : String = {
    s"""
       dim.name=${dim.name}
       publicDim.name=${publicDim.name}
       fields=$fields
       filters=$filters
       upperCandidates=${upperCandidates.map(_.name)}
       lowerCandidates=${lowerCandidates.map(_.name)}
       hasNonFKOrForcedFilters=$hasNonFKOrForcedFilters
       hasNonFKSortBy=$hasNonFKSortBy
       hasNonPushDownFilters=$hasNonPushDownFilters
       hasPKRequested=$hasPKRequested
     """
  }

}

object DimensionBundle {
  implicit val ordering: Ordering[DimensionBundle] = Ordering.by(dc => s"${dc.dim.dimLevel.level}-${dc.dim.name}")
}

class QueryContextBuilder(queryType: QueryType, requestModel: RequestModel) {

  var dims : SortedSet[DimensionBundle] = SortedSet.empty
  var factBestCandidate : Option[FactBestCandidate] = None
  var indexAliasOption : Option[String] = None
  var queryAttributes : QueryAttributes = QueryAttributes.empty

  def addDimTable(dimension: DimensionBundle) = {
    require(queryType != FactOnlyQuery, "fact only query should not have dim table")
    dims += dimension
    this
  }

  def addDimTable(dimensions: SortedSet[DimensionBundle]) = {
    require(queryType != FactOnlyQuery, "fact only query should not have dim table")
    dims ++= dimensions
    this
  }

  def addFactBestCandidate(factBestCandidate: FactBestCandidate) = {
    require(queryType != DimOnlyQuery, "dim only query should not have fact table")
    this.factBestCandidate= Option(factBestCandidate)
    this
  }
  
  def addIndexAlias(indexAlias: String) = {
    require(queryType != DimFactQuery, "dim fact query should not have index alias")
    require(indexAliasOption.isEmpty, s"index alias already defined : indexAlias=${indexAliasOption.get} , cannot set to $indexAlias")
    this.indexAliasOption = Option(indexAlias)
    this
  }

  def setQueryAttributes(queryAttributes: QueryAttributes) = {
    this.queryAttributes = queryAttributes
    this
  }

  def build(): QueryContext = {
    queryType match {
      case DimOnlyQuery =>
        require(dims.nonEmpty, "dim only query should not have dimension empty")
        DimQueryContext(dims, requestModel, indexAliasOption, queryAttributes)
      case FactOnlyQuery =>
        require(factBestCandidate.isDefined, "fact only query should have fact defined")
        FactQueryContext(factBestCandidate.get, requestModel, indexAliasOption, queryAttributes)
      case DimFactQuery =>
        require(factBestCandidate.isDefined, "dim fact query should have fact defined")
        CombinedQueryContext(dims, factBestCandidate.get, requestModel, queryAttributes)
      case DimFactOuterGroupByQuery =>
        require(factBestCandidate.isDefined, "dim fact outer group by query should have fact defined")
        require(dims.nonEmpty, "dim fact outer group by query should not have dimension empty")
        DimFactOuterGroupByQueryQueryContext(dims, factBestCandidate.get, requestModel, queryAttributes)
    }
  }
}

object QueryContext {
  def newQueryContext( queryType: QueryType, requestModel: RequestModel): QueryContextBuilder = {
    new QueryContextBuilder(queryType, requestModel)
  }
}
