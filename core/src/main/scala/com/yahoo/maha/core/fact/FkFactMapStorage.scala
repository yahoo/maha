package com.yahoo.maha.core.fact

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.twitter.chill.ScalaKryoInstantiator
import com.yahoo.maha.core.ForeignKey
import com.yahoo.maha.rocksdb.{RocksDBAccessor, RocksDBAccessorBuilder}
import com.yahoo.maha.serde.SerDe

import scala.collection.SortedSet

trait FkFactMapStorage {
  def store(facts: Iterable[Fact]): Unit

  def search(searchKey: SortedSet[String]): Option[SortedSet[Fact]]

  def size: Long

  def isEmpty: Boolean
}

class DefaultPowerSetFkFactMapStorage extends FkFactMapStorage {
  private[this] val secondaryDimFactMap = new scala.collection.concurrent.TrieMap[SortedSet[String], SortedSet[Fact]]()

  override def store(facts: Iterable[Fact]): Unit = {
    facts
      .map(f => (f.dimCols.filter(_.annotations.exists(_.isInstanceOf[ForeignKey])).map(col => col.name), f))
      .par
      .flatMap(tpl => utils.power(tpl._1, tpl._1.size).map(s => (s.to[SortedSet], tpl._2)))
      .toIndexedSeq
      .groupBy(_._1)
      .mapValues(_.map(tpl => tpl._2)
        .to[SortedSet]).foreach {
      entry =>
        secondaryDimFactMap.put(entry._1, entry._2)
    }
  }

  override def search(searchKey: SortedSet[String]): Option[SortedSet[Fact]] = {
    secondaryDimFactMap.get(searchKey)
  }

  override def size: Long = secondaryDimFactMap.size

  override def isEmpty: Boolean = secondaryDimFactMap.isEmpty
}

case class RocksDBFkFactMapStorage(baseDirOption: Option[String]) extends FkFactMapStorage {

  if (baseDirOption.isDefined) {
    val file = new File(baseDirOption.get)
    if (!file.exists()) {
      file.mkdirs()
    }
    RocksDBAccessor.cleanupBaseDir(baseDirOption.get)
  }

  private[this] val rocksDB = new RocksDBAccessorBuilder(s"PowerSetStorage_", baseDirOption)
    .addKeySerDe(FactSearchSerDe)
    .addValSerDe(FactSearchSerDe).toRocksDBAccessor // Using default settings for now
  private[this] val count = new AtomicInteger()

  private[this] val nameToFactMap = new collection.concurrent.TrieMap[String, Fact]

  override def store(facts: Iterable[Fact]): Unit = {
    facts.foreach {
      f => nameToFactMap.put(f.name, f)
    }
    facts.map(f => (f.dimCols.filter(_.annotations.exists(_.isInstanceOf[ForeignKey])).map(col => col.name), f.name))
      .par
      .flatMap(tpl => utils.power(tpl._1, tpl._1.size).map(s => (s.to[SortedSet], tpl._2)))
      .toIndexedSeq
      .groupBy(_._1)
      .mapValues(_.map(tpl => tpl._2)
        .to[SortedSet]).foreach {
      entry =>
        rocksDB.put(entry._1, entry._2)
        count.incrementAndGet()
    }
  }

  override def search(searchKey: SortedSet[String]): Option[SortedSet[Fact]] = {
    rocksDB.get(searchKey).map(_.map(nameToFactMap.apply))
  }

  override def size: Long = count.get()

  override def isEmpty: Boolean = (size == 0)
}

object FactSearchSerDe extends SerDe[SortedSet[String]] {
  override def serialize(t: SortedSet[String]): Array[Byte] = ScalaKryoInstantiator.defaultPool.toBytesWithClass(t)

  override def deserialize(bytes: Array[Byte]): SortedSet[String] = ScalaKryoInstantiator.defaultPool.fromBytes(bytes).asInstanceOf[SortedSet[String]]
}

case class RoaringBitmapFkFactMapStorage(lruSize: Int = 1000) extends FkFactMapStorage {

  import org.roaringbitmap.RoaringBitmap

  private[this] val fkCounter = new AtomicInteger()
  private[this] val fkMap = new collection.concurrent.TrieMap[String, Int]()
  private[this] val nameToFactMap = new collection.concurrent.TrieMap[String, Fact]
  private[this] val bitmaps = new collection.concurrent.TrieMap[String, RoaringBitmap]().withDefault(_ => new RoaringBitmap())
  private[this] val loader: CacheLoader[SortedSet[String], Option[SortedSet[Fact]]] = new CacheLoader[SortedSet[String], Option[SortedSet[Fact]]] {
    override def load(key: SortedSet[String]): Option[SortedSet[Fact]] = {
      internalSearch(key)
    }
  }
  private[this] val cache: LoadingCache[SortedSet[String], Option[SortedSet[Fact]]] = Caffeine
    .newBuilder()
    .maximumSize(lruSize)
    .build[SortedSet[String], Option[SortedSet[Fact]]](loader)

  private[this] def mapToInt(searchKey: SortedSet[String]): SortedSet[Int] = {
    val mapped = searchKey.filter(fkMap.contains).map(fkMap.apply)
    if (mapped.size != searchKey.size) {
      return SortedSet.empty
    }
    mapped
  }

  private[this] def assignAndMapToInt(searchKey: SortedSet[String]): SortedSet[Int] = {
    searchKey.foreach {
      fk =>
        if (!fkMap.contains(fk)) {
          fkMap.putIfAbsent(fk, fkCounter.incrementAndGet())
        }
    }
    mapToInt(searchKey)
  }

  override def store(facts: Iterable[Fact]): Unit = {
    facts.foreach {
      f => nameToFactMap.put(f.name, f)
    }
    facts.foreach {
      fact =>
        val searchKey = fact.dimCols.filter(_.annotations.exists(_.isInstanceOf[ForeignKey])).map(col => col.name).to[SortedSet]
        val mapped = assignAndMapToInt(searchKey)
        val rr = bitmaps(fact.name)
        mapped.foreach(rr.add)
        bitmaps.put(fact.name, rr)
    }
  }

  override def search(searchKey: SortedSet[String]): Option[SortedSet[Fact]] = {
    cache.get(searchKey)
  }

  private[this] def internalSearch(searchKey: SortedSet[String]): Option[SortedSet[Fact]] = {
    val mapped = mapToInt(searchKey)
    if (mapped.isEmpty)
      None
    else {
      val result = mapped.foldLeft(bitmaps.keySet) {
        case (facts, fkId) =>
          facts.filter {
            f => bitmaps(f).contains(fkId)
          }
      }
      if (result.nonEmpty)
        Option(result.map(nameToFactMap.apply).to[SortedSet])
      else None
    }
  }

  def size: Long = bitmaps.size

  def isEmpty: Boolean = bitmaps.isEmpty
}