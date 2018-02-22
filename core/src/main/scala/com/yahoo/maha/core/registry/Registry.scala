// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.registry

import com.yahoo.maha.core.dimension.PublicDimension
import com.yahoo.maha.core.fact.{Fact, FactCandidate, PublicFact}
import com.yahoo.maha.core.request.{ReportingRequest, RequestType}
import com.yahoo.maha.core.{DefaultDimEstimator, DefaultFactEstimator, _}
import grizzled.slf4j.Logging
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.scalaz.JsonScalaz._

import scala.collection.{SortedSet, mutable}

/**
 * Created by jians on 10/14/15.
 */

class RegistryBuilder{

  private var publicDimMap: Map[(String, Int), PublicDimension] = Map()
  private var publicFactMap: Map[(String, Int), PublicFact] = Map()
  private var dimColToKeySetMap: Map[String, Set[String]] = Map()
  private var dimColToDimensionSetMap: Map[String, Set[(String, Int)]] = Map()

  def register(fact: PublicFact): RegistryBuilder = {
    require(!publicFactMap.contains((fact.name, fact.revision)), s"Cannot register multiple public facts with same name : ${fact.name} and revision ${fact.revision}")
    publicFactMap += ((fact.name, fact.revision) -> fact)
    this
  }

  def register(dimension: PublicDimension): RegistryBuilder = {
    require(!publicDimMap.contains((dimension.name, dimension.revision)), s"Cannot register multiple public dims with same name : ${dimension.name} and revision ${dimension.revision}")
    publicDimMap += ((dimension.name, dimension.revision) -> dimension)
    dimension.columnsByAlias foreach {
      attribute => {
        var keySet: Set[String] = dimColToKeySetMap.getOrElse(attribute, Set.empty)
        keySet += dimension.primaryKeyByAlias
        dimColToKeySetMap += (attribute -> keySet)

        var dimSet: Set[(String, Int)] = dimColToDimensionSetMap.getOrElse(attribute, Set.empty)
        dimSet += (dimension.name -> dimension.revision)
        dimColToDimensionSetMap += (attribute -> dimSet)
      }
    }
    this
  }

  def build( dimEstimator:DimCostEstimator=new DefaultDimEstimator,
             factEstimator: FactCostEstimator=new DefaultFactEstimator,
             defaultPublicFactRevisionMap: Map[String, Int] = Map.empty,
             defaultPublicDimRevisionMap: Map[String, Int] = Map.empty) : Registry = {

    val keySet:Set[String] = dimColToKeySetMap.values.flatten.toSet
    val dimColToKeyMap = dimColToKeySetMap.collect {
      //if we have duplicate dimension attributes, should get caught by dup processing
      case (k, ks) if !keySet.contains(k) && ks.size == 1 => (k, ks.head)
    }

    val cubeDimColToKeyMap = new collection.mutable.HashMap[(String, String), String]
    //duplicate dim attribute processing
    val duplicateDimAttributeMap : Map[String, Set[(String, Int)]] = dimColToDimensionSetMap.filter(tpl => !keySet(tpl._1) && tpl._2.map(_._1).size > 1)
    //check a given cube does not have relations to both dimensions
    duplicateDimAttributeMap.foreach {
      case (dimCol, dimsWithDuplicateAttribute) =>
        publicFactMap.map {
          case (k, pf) =>
            require(!dimsWithDuplicateAttribute.forall(ds => pf.foreignKeySources.contains(ds._1)),
              s"cube=${pf.name} has relations to multiple dimensions with same dimension attribute, no way to resolve, col=$dimCol, dims=$dimsWithDuplicateAttribute"
            )
            val sourceDimPrimaryKeySet : Set[String] = dimsWithDuplicateAttribute.filter(ds => pf.foreignKeySources.contains(ds._1)).map(publicDimMap.apply).map(_.primaryKeyByAlias)
            if(sourceDimPrimaryKeySet.nonEmpty) {
              require(sourceDimPrimaryKeySet.size == 1, s"Cannot have multiple primary key for cube=${pf.name}, dimCol=$dimCol, sourceDimPrimaryKeySet=$sourceDimPrimaryKeySet")
              cubeDimColToKeyMap.put((pf.name, dimCol), sourceDimPrimaryKeySet.head)
            }
        }
    }

    // populate defaultPublicFactRevisionMap with the Fact with least revision if it doesn't exist
    val missingDefaultRevision = publicFactMap.keys.groupBy(_._1).map {
      case (name, i) => i.minBy(_._2)
    }.filterNot(p => defaultPublicFactRevisionMap.contains(p._1))

    // populate defaultPublicDimRevisionMap with the Dimension with least revision if it doesn't exist
    val missingDimDefaultRevision = publicDimMap.keys.groupBy(_._1).map {
      case (name, i) => i.minBy(_._2)
    }.filterNot(p => defaultPublicDimRevisionMap.contains(p._1))

    new Registry(publicDimMap, publicFactMap, keySet, dimColToKeyMap, cubeDimColToKeyMap.toMap, dimEstimator, factEstimator, (defaultPublicFactRevisionMap ++ missingDefaultRevision), (defaultPublicDimRevisionMap ++ missingDimDefaultRevision))
  }

}

case class DimColIdentity(publcDimName: String, primaryKeyAlias: String, columnName: String)
case class FactRowsCostEstimate(rowsEstimate: Long, costEstimate: Long, isGrainOptimized: Boolean, isIndexOptimized: Boolean)
case class Registry private[registry](dimMap: Map[(String, Int), PublicDimension]
                                      , factMap: Map[(String, Int), PublicFact]
                                      , keySet:Set[String]
                                      , dimColToKeyMap: Map[String, String]
                                      , cubeDimColToKeyMap: Map[(String, String), String]
                                      , dimEstimator:DimCostEstimator=new DefaultDimEstimator
                                      , factEstimator: FactCostEstimator=new DefaultFactEstimator
                                      , defaultPublicFactRevisionMap: Map[String, Int]
                                      , defaultPublicDimRevisionMap: Map[String, Int]) extends Logging {

  private[this] val schemaToFactMap: Map[Schema, Set[PublicFact]] = factMap.values
    .map(f => f.factSchemaMap.values.flatten.map(s => (s, f))).flatten.groupBy(_._1).mapValues(_.map(_._2).toSet)

  private[this] val dimColByAliasIdentityMap: Map[String, DimColIdentity] = {
    dimMap.values.map { pdim =>
      pdim.columnsByAlias.map { col => col -> DimColIdentity(pdim.name, pdim.primaryKeyByAlias, pdim.aliasToNameMap(col))}.toList
    }.flatten.toMap
  }

  //private[this] val primaryKeyToDimMap : Map[String, PublicDimension] = dimMap.values.map(pd => pd.primaryKeyByAlias -> pd).toMap

  private[this] val primaryKeyToDimMap : Map[(String, Int), PublicDimension] = dimMap.map {
    case(k, pd) => {
      (pd.primaryKeyByAlias, k._2) -> pd
    }
  }

  private[this] val defaultPrimaryKeyToDImMap: Map[String, Int] = primaryKeyToDimMap.keys.groupBy(_._1).map {
    case(name, i) => i.minBy(_._2)
  }

    validate()
  
  //this must come after validate
  private[this] val cubeSchemaRequiredFilterAlias : Map[(String, Schema, String), Set[String]] = {
    val listOfFactNameSchemaWithPubDimensions = 
      factMap
        .values
        .map {
        f => 
          f.factSchemaMap.map {
            case (factName, schemaSet) =>
              val factNameSchemaList = schemaSet.map(s => (factName, s, f.name, f.revision))
              factNameSchemaList.map(factAndSchemaTuple => factAndSchemaTuple -> f.foreignKeySources.map(fks => getDimension(fks, Option.apply(f.revision)).get))
          }.flatten
      }.flatten
    val mapOfFactNameSchemaWithPubDimensions = listOfFactNameSchemaWithPubDimensions.toMap
    require(listOfFactNameSchemaWithPubDimensions.size == mapOfFactNameSchemaWithPubDimensions.size, 
      "Size mismatch where non expected after flattening and converting to map")
    mapOfFactNameSchemaWithPubDimensions.map {
      case ((factName, schema, publicFactName, revision), publicDimSet) =>
        (factName, schema, publicFactName) -> publicDimSet.map(_.schemaRequiredAlias(schema).map(_.alias)).flatten.toSet
    }
  }

  private[this] val dimensionPathMap : Map[(String, String), SortedSet[PublicDimension]] = {
    val dimPathMap = new collection.mutable.HashMap[(String, String), SortedSet[PublicDimension]]
    val dimsSortedByLevelAsc = dimMap.values.to[SortedSet]
    
    val dimToForeignKeyMap = dimMap.values.map {
      dim =>
        dim.name -> dim.foreignKeySources.map(fks => dimMap.get((fks, dim.revision)).get).to[SortedSet]
    }.filter(_._2.nonEmpty).toMap
    
    def processRelations(dim: PublicDimension, relations: SortedSet[PublicDimension], subRelation: PublicDimension, pathToSubRelation: SortedSet[PublicDimension]) : Unit = {
      dimToForeignKeyMap.get(subRelation.name).foreach {
        _.filterNot(d => relations.apply(d)).foreach {
          missingRelation =>
            val pathToMissingRelation = pathToSubRelation ++ SortedSet(subRelation)
            dimPathMap.put((missingRelation.name, dim.name), pathToMissingRelation)
            dimToForeignKeyMap.get(missingRelation.name).foreach {
              missingRelationRelations => missingRelationRelations.foreach(processRelations(dim, relations, _, pathToMissingRelation))
            }
        }
      }
    }
    
    dimsSortedByLevelAsc.foreach {
      dim => 
        if(dimToForeignKeyMap.contains(dim.name)) {
          val relations = dimToForeignKeyMap(dim.name)
          relations.foreach {
            relDim =>
              processRelations(dim, relations, relDim, SortedSet.empty)
          }
        }
    }
    
    dimPathMap.toMap
  }

  private[this] val publicFactToDimensionMap : Map[(String, Int), IndexedSeq[(String, Int)]] = {
    factMap.map {
      case ((name, rev), pf) =>
        val dimRev = pf.dimRevision
        val dimRevList = pf.foreignKeySources.map( pd => (pd, dimRev)).toIndexedSeq
        ((name, rev), dimRevList)
    }
  }

  private[this] def validate(): Unit = {
    //all the referenced foreign key tables in facts must exist
    val factFKSources = factMap.values.map(f => (f.name, f.revision, f.foreignKeySources))
    val factMissingSources = factFKSources.collect {
      case (f, r, sources) if ! sources.forall(fks => getDimension(fks, Option.apply(r)).isDefined) => (f, sources.filterNot(fks => getDimension(fks, Option.apply(r)).isDefined))
    }
    require(factMissingSources.isEmpty, s"Missing foreign key fact sources : $factMissingSources")

    //all the referenced foreign key tables in dimensions must exist
    val dimFKSources = dimMap.values.map(f => (f.name, f.revision, f.foreignKeySources))
    val dimMissingSources = dimFKSources.collect {
      case (f, r, sources) if ! sources.forall(fks => getDimension(fks, Option.apply(r)).isDefined) => (f, sources.filterNot(fks => getDimension(fks, Option.apply(r)).isDefined))
    }
    require(dimMissingSources.isEmpty, s"Missing foreign key dimension sources : $dimMissingSources")

    //all the entries in defaultPublicFactRevision must exist in publicFactMap
    val missingDefaultPublicFactRevision = defaultPublicFactRevisionMap.filterNot(factMap.contains)
    require(missingDefaultPublicFactRevision.isEmpty, s"Missing default public fact revision : $missingDefaultPublicFactRevision")

    //all the entries in defaultPublicDimRevision must exist in publicDimMap
    val missingDefaultPublicDimRevision = defaultPublicDimRevisionMap.filterNot(dimMap.contains)
    require(missingDefaultPublicDimRevision.isEmpty, s"Missing default public dim revision : $missingDefaultPublicDimRevision")
  }

  def getFact(name: String, revision: Option[Int] = None): Option[PublicFact] = {
    if (revision.isDefined && factMap.contains((name, revision.get))) {
      factMap.get((name, revision.get))
    } else if (defaultPublicFactRevisionMap.contains(name)) {
      factMap.get((name, defaultPublicFactRevisionMap.get(name).get))
    } else {
      None
    }
  }

  def getDimension(name: String, revision: Option[Int] = None): Option[PublicDimension] = {
    if(revision.isDefined && dimMap.contains((name, revision.get))) {
      dimMap.get((name, revision.get))
    } else if (defaultPublicDimRevisionMap.contains(name)) {
      dimMap.get((name, defaultPublicDimRevisionMap.get(name).get))
    } else {
      None
    }
  }

  def getPrimaryKeyAlias(publicFactName: String, revision: Option[Int], attribute: String): Option[String] = {
    dimColToKeyMap.get(attribute) orElse cubeDimColToKeyMap.get((publicFactName, attribute)) orElse {
      val rev = revision.getOrElse(defaultPublicFactRevisionMap(publicFactName))
      publicFactToDimensionMap.get((publicFactName, rev)).flatMap {
        dimRevList =>
          dimRevList.collectFirst {
            case pdRev if dimMap.contains(pdRev) && dimMap(pdRev).allColumnsByAlias(attribute) =>
              dimMap(pdRev).primaryKeyByAlias
          }
      }
    }
  }

  //this method has different contract then other method of same name, especially for ordering/sorting
  def getPrimaryKeyAlias(publicFactName: String, attribute: String): Option[String] = {
    dimColToKeyMap.get(attribute) orElse cubeDimColToKeyMap.get((publicFactName, attribute))
  }

  def getDimColIdentity(attribute: String): Option[DimColIdentity] = {
    dimColByAliasIdentityMap.get(attribute)
  }

  def getDimensionByPrimaryKeyAlias(alias: String, revision: Option[Int]) : Option[PublicDimension] = {
    if(revision.isDefined && primaryKeyToDimMap.contains((alias, revision.get))) {
      primaryKeyToDimMap.get((alias, revision.get))
    } else if(defaultPrimaryKeyToDImMap.contains(alias)) {
      primaryKeyToDimMap.get((alias, defaultPrimaryKeyToDImMap.get(alias).get))
    } else {
      None
    }
  }
  
  def isPrimaryKeyAlias(alias: String) : Boolean = {
    keySet(alias)
  }
  
  def getSchemaRequiredFilterAliasesForFact(factName: String, schema: Schema, publicFactName: String) : Set[String] = {
    cubeSchemaRequiredFilterAlias.getOrElse((factName, schema, publicFactName), Set.empty)
  }

  def isCubeDefined(cube: String, revision: Option[Int] = None): Boolean = {
    if(revision.isDefined && factMap.contains((cube, revision.get))) {
      true
    } else if (defaultPublicFactRevisionMap.contains(cube)) {
      true
    } else {
      false
    }
  }
  
  def getFactRowsCostEstimate(dimensionsCandidates: SortedSet[DimensionCandidate], factCandidate: FactCandidate, reportingRequest: ReportingRequest,
                              entitySet: Set[PublicDimension], filters: mutable.Map[String, Filter], isDebug: Boolean): FactRowsCostEstimate = {
    val schemaRequiredEntity = entitySet.map(_.grainKey)
    val highestLevelDim = dimensionsCandidates.lastOption
    val grainKey =  schemaRequiredEntity.headOption.map(s => s"$s-").getOrElse("") + highestLevelDim.map(_.dim.grainKey).getOrElse("")
    val rowsEstimate = factEstimator.getRowsEstimate(grainKey, reportingRequest, filters, factCandidate.fact.defaultRowCount)
    val costEstimate = factEstimator.getCostEstimate(rowsEstimate, factCandidate.fact.costMultiplierMap.get(reportingRequest.requestType))
    val isIndexOptimized = filters.keys.exists(factCandidate.publicFact.foreignKeyAliases)
    if(isDebug){
      info(s"Fact Cost estimated for request with grainKey=$grainKey defaultRowCount=${factCandidate.fact.defaultRowCount} rowsEstimate=$rowsEstimate costEstimate=$costEstimate isGrainOptimized=${rowsEstimate.isGrainOptimized} isIndexOptimized=$isIndexOptimized")
    }
    FactRowsCostEstimate(rowsEstimate = rowsEstimate.rows, costEstimate = costEstimate, isGrainOptimized = rowsEstimate.isGrainOptimized, isIndexOptimized = isIndexOptimized)
  }
  
  def getDimCardinalityEstimate(dimensionsCandidates: SortedSet[DimensionCandidate], 
                                reportingRequest: ReportingRequest,entitySet: Set[PublicDimension],
                                filters: mutable.Map[String, Filter],isDebug:Boolean): Option[Long] = {
    val schemaRequiredEntity = entitySet.map(_.grainKey)
    val highestLevelDim = dimensionsCandidates.lastOption
    val grainKey =  schemaRequiredEntity.headOption.map(s => s"$s-").getOrElse("") + highestLevelDim.map(_.dim.grainKey).getOrElse("")
    val estimate=dimEstimator.getCardinalityEstimate(grainKey, reportingRequest, filters )

    if(isDebug){
      info(s"Dim Cost estimated for request with grainKey:$grainKey filters:$filters : $estimate")
    }
    estimate
  }

  private[this] def getCubeJsonByName: Map[String, JObject] = {
    factMap.values.toList.filter(publicFact => ((publicFact.revision == defaultPublicFactRevisionMap.get(publicFact.name).get))).map { publicFact => (publicFact.name, {
      val dimensionFieldList = publicFact.dimCols.toList.sortBy(_.alias).collect {
        case  dimCol if !dimCol.hiddenFromJson => {
          val filterList = dimCol.filters.map(_.toString.toUpperCase).toList
          val filterOperations = if(filterList.isEmpty) {
            JNull
          } else {
            toJSON(filterList)
          }
          val incompatibleColumnsList = dimCol.incompatibleColumns.toList
          val incompatibleColumns = if(incompatibleColumnsList.isEmpty) {
            JNull
          } else {
            toJSON(incompatibleColumnsList)
          }
          makeObj(
            ("field" -> toJSON(dimCol.alias))
              :: ("type" -> toJSON("Dimension"))
              :: ("dataType" -> publicFact.dataTypeForAlias(dimCol.alias).asJson)
              :: ("dimensionName" -> toJSON(publicFact.foreignKeySourceForAlias(dimCol.alias)))
              :: ("filterable" -> toJSON(dimCol.filters.nonEmpty))
              :: ("filterOperations" -> filterOperations)
              :: ("required" -> toJSON(dimCol.required))
              :: ("filteringRequired" -> toJSON(dimCol.filteringRequired))
              :: ("incompatibleColumns" -> incompatibleColumns)
              :: Nil
          )
        }
      }

      val factFieldList = publicFact.factCols.toList.sortBy(_.alias).collect {
        case factCol if !factCol.hiddenFromJson => {
          val filterList = factCol.filters.map(_.toString.toUpperCase).toList
          val filterOperations = if(filterList.isEmpty) {
            JNull
          } else {
            toJSON(filterList)
          }
          val incompatibleColumnsList = factCol.incompatibleColumns.toList
          val incompatibleColumns = if(incompatibleColumnsList.isEmpty) {
            JNull
          } else {
            toJSON(incompatibleColumnsList)
          }
          val fc = publicFact.baseFact.factColMap(publicFact.aliasToNameColumnMap(factCol.alias))
          makeObj(
            ("field" -> toJSON(factCol.alias))
              :: ("type" -> toJSON("Fact"))
              :: ("dataType" -> publicFact.dataTypeForAlias(factCol.alias).asJson)
              :: ("dimensionName" -> JNull)
              :: ("filterable" -> toJSON(factCol.filters.nonEmpty))
              :: ("filterOperations" -> filterOperations)
              :: ("required" -> toJSON(factCol.required))
              :: ("filteringRequired" -> toJSON(factCol.filteringRequired))
              :: ("rollupExpression" -> toJSON(fc.rollupExpression.toString))
              :: ("incompatibleColumns" -> incompatibleColumns)
              :: Nil
          )
        }
      }

      val foreignSources = publicFact.foreignKeySources.collect {
        case name if dimMap.contains((name, publicFact.revision)) => dimMap((name, publicFact.revision))
      }

      val schemaColAliasMap = foreignSources
        .map(dim => dim.schemas.map(s => (s, dim.schemaRequiredAlias(s).map(_.alias)))
        .collect {
          case (s, Some(alias)) => (s.toString, toJSON(alias))
        }).flatten.toMap

      val maxDaysLookBack = JArray(toMaxDaysList(publicFact.maxDaysLookBack))
      val maxDaysWindow = JArray(toMaxDaysList(publicFact.maxDaysWindow))

      makeObj(
        ("name" -> toJSON(publicFact.name))
          :: ("mainEntityIds" -> makeObj(schemaColAliasMap))
          :: ("maxDaysLookBack" -> maxDaysLookBack)
          :: ("maxDaysWindow" -> maxDaysWindow)
          :: ("fields" -> JArray(dimensionFieldList ++ factFieldList))
          :: Nil
      )
    })}.toMap
  }

  private[this] def getFlattenCubeJsonByName: Map[(String, Int), JObject] = {
    factMap.values.toList.map { publicFact => ((publicFact.name, publicFact.revision), {
      val dimensionFieldList = publicFact.dimCols.toList.sortBy(_.alias).collect {
        case  dimCol if !dimCol.hiddenFromJson => {
          val filterList = dimCol.filters.map(_.toString.toUpperCase).toList
          val filterOperations = if(filterList.isEmpty) {
            JNull
          } else {
            toJSON(filterList)
          }
          makeObj(
            ("field" -> toJSON(dimCol.alias))
              :: ("type" -> toJSON("Dimension"))
              :: ("dataType" -> publicFact.dataTypeForAlias(dimCol.alias).asJson)
              :: ("dimensionName" -> toJSON(publicFact.foreignKeySourceForAlias(dimCol.alias)))
              :: ("filterable" -> toJSON(dimCol.filters.nonEmpty))
              :: ("filterOperations" -> filterOperations)
              :: ("required" -> toJSON(dimCol.required))
              :: ("filteringRequired" -> toJSON(dimCol.filteringRequired))
              :: Nil
          )
        }
      }


      // flatten dim cols without PK
     val flattenDimCols = new scala.collection.mutable.ListBuffer[JObject]
      publicFact.foreignKeyAliases.toList.foreach {
        case fk =>
          val dimension = getDimensionByPrimaryKeyAlias(fk, Some(publicFact.dimRevision))
          require(dimension.isDefined, s"Failed to find dimesion for $fk inside getFlattenCubeJsonByName")
          val colList = dimension.get.columnsByAliasMap.filter(!_._1.equals(fk)).toList.foreach {
            case dimCol =>
              val filterList = dimCol._2.filters.map(_.toString.toUpperCase).toList
              val filterOperations = if(filterList.isEmpty) {
                JNull
              } else {
                toJSON(filterList)
              }
              flattenDimCols += makeObj(
                 ("field" -> toJSON(dimCol._2.alias))
                   :: ("type" -> toJSON("Dimension"))
                   :: ("dataType" -> dimension.get.nameToDataTypeMap(dimCol._2.name).asJson)
                   :: ("dimensionName" -> toJSON(dimension.get.name))
                   :: ("filterable" -> toJSON(dimCol._2.filters.nonEmpty))
                   :: ("filterOperations" -> filterOperations)
                   :: ("required" -> toJSON(dimCol._2.required))
                   :: ("filteringRequired" -> toJSON(dimCol._2.filteringRequired))
                   :: Nil
               )
          }
      }

      val factFieldList = publicFact.factCols.toList.sortBy(_.alias).collect {
        case factCol if !factCol.hiddenFromJson => {
          val filterList = factCol.filters.map(_.toString.toUpperCase).toList
          val filterOperations = if(filterList.isEmpty) {
            JNull
          } else {
            toJSON(filterList)
          }
          val fc = publicFact.baseFact.factColMap(publicFact.aliasToNameColumnMap(factCol.alias))
          makeObj(
            ("field" -> toJSON(factCol.alias))
              :: ("type" -> toJSON("Fact"))
              :: ("dataType" -> publicFact.dataTypeForAlias(factCol.alias).asJson)
              :: ("dimensionName" -> JNull)
              :: ("filterable" -> toJSON(factCol.filters.nonEmpty))
              :: ("filterOperations" -> filterOperations)
              :: ("required" -> toJSON(factCol.required))
              :: ("filteringRequired" -> toJSON(factCol.filteringRequired))
              :: ("rollupExpression" -> toJSON(fc.rollupExpression.toString))
              :: Nil
          )
        }
      }

      val foreignSources = publicFact.foreignKeySources.collect {
        case name if dimMap.contains((name, publicFact.revision)) => dimMap((name, publicFact.revision))
      }

      val schemaColAliasMap = foreignSources
        .map(dim => dim.schemas.map(s => (s, dim.schemaRequiredAlias(s).map(_.alias)))
          .collect {
            case (s, Some(alias)) => (s.toString, toJSON(alias))
          }).flatten.toMap

      val maxDaysLookBack = JArray(toMaxDaysList(publicFact.maxDaysLookBack))

      val maxDaysWindow = JArray(toMaxDaysList(publicFact.maxDaysWindow))

      makeObj(
        ("name" -> toJSON(publicFact.name))
          :: ("mainEntityIds" -> makeObj(schemaColAliasMap))
          :: ("maxDaysLookBack" -> maxDaysLookBack)
          :: ("maxDaysWindow" -> maxDaysWindow)
          :: ("fields" -> JArray(dimensionFieldList ++ factFieldList ++ flattenDimCols.toList ))
          :: Nil
      )
    })}.toMap
  }


  private def toMaxDaysList(factDaysMap: Map[(RequestType, Grain), Int]) = {
    factDaysMap.map {
      case ((requestType, grain), value) =>
        makeObj(
          "requestType" -> toJSON(requestType.toString())
            ::"grain" -> toJSON(grain.toString)
            :: "days" -> toJSON(value)
            :: Nil)
    }.toList
  }

  val (domainJsonAsString : String, cubesJsonStringByName: Map[String, String]) = {
    val cubeJsonByName : Map[String, JObject] = getCubeJsonByName
    val cubesJsonArray: JArray = JArray(cubeJsonByName.toList.sortBy(_._1).map(_._2))

    val dimensionsJsonArray: JArray = JArray(
      dimMap.values.filter(pd => (pd.revision == defaultPublicDimRevisionMap.get(pd.name).get)).map { publicDim =>
        makeObj(
          ("name" -> toJSON(publicDim.name))
            :: ("fields" -> toJSON(publicDim.columnsByAliasMap.filter(rec => !rec._2.hiddenFromJson).keySet.toList))
            :: Nil
        )
      }.toList
    )

    val schemasJson: JObject = {
      val jsonMap = schemaToFactMap map {
        case (schema, publicFacts) => (schema.toString, toJSON(publicFacts.map(_.name).toList))
      }
      makeObj(jsonMap)
    } 
    
    val finalJson = makeObj(
      ("dimensions" -> dimensionsJsonArray)
      :: ("schemas" -> schemasJson)
      :: ("cubes" -> cubesJsonArray)
      :: Nil
    )
    (compact(render(finalJson)), cubeJsonByName.mapValues(j => compact(render(j))))
  }

  def getCubeJsonAsStringForCube(name: String): String = {
    require(cubesJsonStringByName.contains(name), s"cube name $name does not exist")
    cubesJsonStringByName(name)
  }

  val (flattenDomainJsonAsString : String, flattenCubesJsonStringByName: Map[String, String]) = {
    val flattenCubeJsonByName : Map[String, JObject] = getFlattenCubeJsonByName.
      filter(e=> defaultPublicFactRevisionMap.get(e._1._1).get == e._1._2).map(e=> e._1._1-> e._2).toMap

    val cubesJsonArray: JArray = JArray(flattenCubeJsonByName.toList.sortBy(_._1).map(_._2))

    val dimensionsJsonArray: JArray = JArray(
      dimMap.values.filter(pd => (pd.revision == defaultPublicDimRevisionMap.get(pd.name).get)).map { publicDim =>
        makeObj(
          ("name" -> toJSON(publicDim.name))
            :: ("fields" -> toJSON(publicDim.columnsByAliasMap.filter(rec => !rec._2.hiddenFromJson).keySet.toList))
            :: Nil
        )
      }.toList
    )

    val schemasJson: JObject = {
      val jsonMap = schemaToFactMap map {
        case (schema, publicFacts) => (schema.toString, toJSON(publicFacts.map(_.name).toList))
      }
      makeObj(jsonMap)
    }

    val finalJson = makeObj(
      ("dimensions" -> dimensionsJsonArray)
        :: ("schemas" -> schemasJson)
        :: ("cubes" -> cubesJsonArray)
        :: Nil
    )
    (compact(render(finalJson)), flattenCubeJsonByName.mapValues(j => compact(render(j))))
  }

  def getFlattenCubeJsonAsStringForCube(name: String, revison:Int = 0): String = {
    require(getFlattenCubeJsonByName.contains((name,revison)), s"cube name $name does not exist")
    require(factMap.contains((name, revison)), s" revison $revison for cube name $name does not exist")
    compact(render(getFlattenCubeJsonByName(name,revison)))
  }

  def findDimensionPath(fromDim: PublicDimension, toDim: PublicDimension) : SortedSet[PublicDimension] = {
    dimensionPathMap.getOrElse((fromDim.name, toDim.name), SortedSet.empty)
  }
}

trait FactRegistrationFactory {
  def register(registry: RegistryBuilder): Unit
}

trait DimensionRegistrationFactory {
  def register(registry: RegistryBuilder): Unit
}

class NoopFactRegistrationFactory extends FactRegistrationFactory {
  def register(registry: RegistryBuilder): Unit = {}
}

class NoopDimensionRegistrationFactory extends DimensionRegistrationFactory {
  def register(registry: RegistryBuilder): Unit = {}
}
