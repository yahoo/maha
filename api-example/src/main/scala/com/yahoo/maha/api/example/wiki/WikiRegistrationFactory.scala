// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.api.example.wiki

import com.yahoo.maha.api.example.ExampleSchema.WikiSchema
import com.yahoo.maha.core.DruidDerivedFunction.DRUID_TIME_FORMAT
import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension.{DruidFuncDimCol, DimCol, PubCol}
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.registry.{DimensionRegistrationFactory, FactRegistrationFactory, RegistryBuilder}
import com.yahoo.maha.core.request.{AsyncRequest, RequestType, SyncRequest}

/**
 * Created by pranavbhole on 17/10/17.
 */
class WikiFactRegistrationFactory extends FactRegistrationFactory {

 /*
   Max allowed days window to query data
  */
  protected[this] def getMaxDaysWindow: Map[(RequestType, Grain), Int] = {
    val result = 20
    Map(
      (SyncRequest, DailyGrain) -> result, (AsyncRequest, DailyGrain) -> result,
      (SyncRequest, HourlyGrain) -> result, (AsyncRequest, HourlyGrain) -> result
    )
  }

  /*
  Retention configuration on the data
   */
  protected[this] def getMaxDaysLookBack: Map[(RequestType, Grain), Int] = {
    val result = 9999
    Map(
      (SyncRequest, DailyGrain) -> result, (AsyncRequest, DailyGrain) -> result
    )
  }

  override def register(registry: RegistryBuilder): Unit = {

    def pubfact: PublicFactTable = {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        Fact.newFact(
          "wikipedia", DailyGrain, DruidEngine, Set(WikiSchema),
          Set(
            DimCol("channel", StrType())
            , DimCol("cityName", StrType())
            , DimCol("comment", StrType(), annotations = Set(EscapingRequired))
            , DimCol("countryIsoCode", StrType(10))
            , DimCol("countryName", StrType(100))
            , DimCol("isAnonymous", StrType(5))
            , DimCol("isMinor", StrType(5))
            , DimCol("isNew", StrType(5))
            , DimCol("isRobot", StrType(5))
            , DimCol("isUnpatrolled", StrType(5))
            , DimCol("metroCode", StrType(100))
            , DimCol("namespace", StrType(100, (Map("Main" -> "Main Namespace", "User" -> "User Namespace", "Category" -> "Category Namespace", "User Talk"-> "User Talk Namespace"), "Unknown Namespace")))
            , DimCol("page", StrType(100))
            , DimCol("regionIsoCode", StrType(10))
            , DimCol("regionName", StrType(200))
            , DimCol("user", StrType(200))
            , DruidFuncDimCol("Day", DateType(), DRUID_TIME_FORMAT("YYYY-MM-dd"))
          ),
          Set(
          FactCol("count", IntType())
          ,FactCol("added", IntType())
          ,FactCol("deleted", IntType())
          ,FactCol("delta", IntType())
          ,FactCol("user_unique", IntType())
          ,DruidDerFactCol("Delta Percentage", DecType(10, 8), "{delta} * 100 / {count} ")
          )
        )
      }
        .toPublicFact("wikiticker_stats",
          Set(
            PubCol("Day", "Day", InBetweenEquality),
            PubCol("channel", "Wiki Channel", InNotInEquality),
            PubCol("cityName", "City Name", InNotInEqualityLike),
            PubCol("countryIsoCode", "Country ISO Code", InNotInEqualityLike),
            PubCol("countryName", "Country Name", InNotInEqualityLike),
            PubCol("isAnonymous", "Is Anonymous", InNotInEquality),
            PubCol("isMinor", "Is Minor", InNotInEquality),
            PubCol("isNew", "Is New", InNotInEquality),
            PubCol("isRobot", "Is Robot", InNotInEquality),
            PubCol("isUnpatrolled", "Is Unpatrolled", InNotInEquality),
            PubCol("metroCode", "Metro Code", InNotInEquality),
            PubCol("namespace", "Namespace", InNotInEquality),
            PubCol("page", "Page", InNotInEquality),
            PubCol("regionIsoCode", "Region Iso Code", InNotInEquality),
            PubCol("regionName", "Region Name", InNotInEqualityLike),
            PubCol("user", "User", InNotInEquality)
          ),
          Set(
            PublicFactCol("count", "Total Count", InBetweenEquality),
            PublicFactCol("added", "Added Count", InBetweenEquality),
            PublicFactCol("deleted", "Deleted Count", InBetweenEquality),
            PublicFactCol("delta", "Delta Count", InBetweenEquality),
            PublicFactCol("user_unique", "Unique User Count", InBetweenEquality),
            PublicFactCol("Delta Percentage", "Delta Percentage", InBetweenEquality)
          ),
          Set.empty,
          getMaxDaysWindow, getMaxDaysLookBack
        )
    }

    registry.register(pubfact)
  }
}

class WikiDimensionRegistrationFactory extends DimensionRegistrationFactory {
  override def register(registry: RegistryBuilder): Unit =  {}
}
