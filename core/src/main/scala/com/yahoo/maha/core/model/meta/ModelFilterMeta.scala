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
  override def toString: String =
    s"""${this.getClass.getSimpleName}:
       factFilters: $factFilters
       utcTimeDayFilter: $utcTimeDayFilter
       localTimeDayFilter: $localTimeDayFilter
       utcTimeHourFilterOption: $utcTimeHourFilterOption
       localTimeHourFilterOption: $localTimeHourFilterOption
       utcTimeMinuteFilterOption: $utcTimeMinuteFilterOption
       localTimeMinuteFilterOption: $localTimeMinuteFilterOption
       outerFilters: $outerFilters
       orFilterMeta: $orFilterMeta""".stripMargin
}
