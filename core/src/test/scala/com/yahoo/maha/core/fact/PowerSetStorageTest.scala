package com.yahoo.maha.core.fact

import scala.collection.SortedSet

class PowerSetStorageTest extends BaseFactTest {

  test("test default powerset storage") {
   val result = publicFact(fact1).getPowerSetStorage
    assert(result.size == 15)
    val searchResult = result.search(FactSearchKey(SortedSet("account_id", "campaign_id")))
    assert(searchResult.isDefined)
    assert(searchResult.get.head == "fact1")
  }

  test("test rocksb powerset storage") {
    val result = publicFact(fact1, powerSetStorage = RocksDBPowerSetStorage(Some("target/test"))).getPowerSetStorage
    assert(result.size == 15)
    val searchResult = result.search(FactSearchKey(SortedSet("account_id", "campaign_id")))
    assert(searchResult.isDefined)
    assert(searchResult.get.head == "fact1")
  }

}
