package com.yahoo.maha.core.fact

import com.twitter.chill.ScalaKryoInstantiator
import com.yahoo.maha.rocksdb.RocksDBAccessorBuilder
import com.yahoo.maha.serde.SerDe

import scala.collection.{SortedSet, mutable}

case class FactSearchKey(cube: String, fkSet: SortedSet[String])

trait PowerSetStorage {
  def store(searchKey: FactSearchKey, facts: SortedSet[String]): Unit
  def search(searchKey: FactSearchKey) : Option[SortedSet[String]]
  def size: Long
}

class DefaultPowerSetStorage extends PowerSetStorage {
  private[this] val secondaryDimFactMap = new mutable.HashMap[SortedSet[String], SortedSet[String]]()

  override def store(searchKey: FactSearchKey, facts: SortedSet[String]): Unit =  {
    secondaryDimFactMap.put(searchKey.fkSet, facts)
  }

  override def search(searchKey: FactSearchKey): Option[SortedSet[String]] = {
    secondaryDimFactMap.get(searchKey.fkSet)
  }

  override def size: Long = secondaryDimFactMap.size
}

case class RocksDBPowerSetStorage(baseDirOption: Option[String]) extends PowerSetStorage {

  private[this] val rocksDB = new RocksDBAccessorBuilder(s"PowerSetStorage_", baseDirOption)
    .addKeySerDe(FactSearchKeySerDe)
    .addValSerDe(FactSerchResultSerDe).toRocksDBAccessor // Using default settings for now
  private[this] var count: Long = 0;

  def store(searchKey: FactSearchKey, facts: SortedSet[String]): Unit = {
    rocksDB.put(searchKey, facts)
    count = count+1;
  }

  def search(searchKey: FactSearchKey) : Option[SortedSet[String]] = {
    rocksDB.get(searchKey)
  }

  override def size: Long = count
}

object FactSearchKeySerDe extends SerDe[FactSearchKey] {
  override def serialize(t: FactSearchKey): Array[Byte] = ScalaKryoInstantiator.defaultPool.toBytesWithClass(t)
  override def deserialize(bytes: Array[Byte]): FactSearchKey = ScalaKryoInstantiator.defaultPool.fromBytes(bytes).asInstanceOf[FactSearchKey]
}
object FactSerchResultSerDe extends SerDe[SortedSet[String]] {
  override def serialize(t: SortedSet[String]): Array[Byte] = ScalaKryoInstantiator.defaultPool.toBytesWithClass(t)
  override def deserialize(bytes: Array[Byte]): SortedSet[String] = ScalaKryoInstantiator.defaultPool.fromBytes(bytes).asInstanceOf[SortedSet[String]]
}
