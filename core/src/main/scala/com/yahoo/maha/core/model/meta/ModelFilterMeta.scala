package com.yahoo.maha.core.model.meta

import com.yahoo.maha.core.{Filter, OrFilterMeta}

import scala.collection.SortedSet

/**
  *
  * - factFilters
  * - utcTimeDayFilter
  * - localTimeDayFilter
  * - utcTimeHourFilter
  * - localTimeHourFilter
  * - utcTimeMinuteFilter
  * - localTimeMinuteFilter
  * - outerFilters
  * - orFilterMeta
  */
case class ModelFilterMeta (
                           factFilters: SortedSet[Filter]
                           , utcTimeDayFilter: Filter
                           , localTimeDayFilter: Filter
                           , utcTimeHourFilterOption: Option[Filter]
                           , localTimeHourFilterOption: Option[Filter]
                           , utcTimeMinuteFilterOption: Option[Filter]
                           , localTimeMinuteFilterOption: Option[Filter]
                           , outerFilters: SortedSet[Filter]
                           , orFilterMeta: SortedSet[OrFilterMeta]
                           ) {

}
