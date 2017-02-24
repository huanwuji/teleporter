package teleporter.integration.utils

import java.util.concurrent.ConcurrentSkipListMap

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

/**
  * Author: kui.dai
  * Date: 2016/6/30.
  */
class BiMap[K, V](keyMap: mutable.Map[K, V], valueMap: mutable.Map[V, K]) {

  def +=(key: K, value: V): Unit = {
    keyMap += key → value
    valueMap += value → key
  }

  def applyKey(key: K): V = keyMap(key)

  def applyValue(value: V): K = valueMap(value)

  def getByKey(key: K): Option[V] = keyMap.get(key)

  def getByValue(value: V): Option[K] = valueMap.get(value)

  def removeKey(key: K): Option[K] = keyMap.remove(key).flatMap(valueMap.remove)

  def removeValue(value: V): Option[V] = valueMap.remove(value).flatMap(keyMap.remove)
}

class TwoIndexMap[K1, V](k1Map: mutable.Map[K1, V], k2Map: ConcurrentSkipListMap[String, V], biMap: BiMap[K1, String]) {
  private val tailRegexSplit = "[^\\w/]".r

  def +=(key1: K1, key2: String, value: V): Unit = {
    biMap += (key1, key2)
    k1Map += key1 → value
    k2Map.put(key2, value)
  }

  def regexRangeTo[C](regex: String): mutable.Map[String, C] = this.regexRange(regex).asInstanceOf[mutable.Map[String, C]]

  def regexRange(regex: String): mutable.Map[String, V] = {
    tailRegexSplit.findFirstMatchIn(regex) match {
      case Some(m) ⇒
        val prefixKey = regex.substring(0, m.start)
        val tailRegex = regex.substring(m.start).r
        this.range(prefixKey).filter { case (k, v) ⇒ tailRegex.findPrefixOf(k.substring(m.start)).isDefined }
      case None ⇒ this.range(regex)
    }
  }

  def rangeTo[C](key2: String): mutable.Map[String, C] = this.range(key2).asInstanceOf[mutable.Map[String, C]]

  def range(key2: String): mutable.Map[String, V] = {
    k2Map.tailMap(key2, false).headMap(key2.dropRight(1) + (key2.tail.toCharArray.apply(0) + 1).toChar).asScala
  }

  def modifyByKey1(key1: K1, modify: V ⇒ V): Unit = {
    val v = k1Map(key1)
    val m = modify(v)
    k1Map.put(key1, m)
    k2Map.put(biMap.applyKey(key1), m)
  }

  def modifyByKey2(key2: String, modify: V ⇒ V): Unit = {
    val v = k2Map.get(key2)
    val m = modify(v)
    k2Map.put(key2, m)
    k1Map.put(biMap.applyValue(key2), m)
  }

  def applyKey1(key: K1): V = getKey1(key) match {
    case None ⇒ throw new NoSuchElementException("key not found: " + key)
    case Some(value) ⇒ value
  }

  def applyKey2(key: String): V = getKey2(key) match {
    case None ⇒ throw new NoSuchElementException("key not found: " + key)
    case Some(value) ⇒ value
  }

  def getKey1(key: K1): Option[V] = k1Map.get(key)

  def getKey2(key: String): Option[V] = Option(k2Map.get(key))

  def removeKey1(key: K1): Option[V] = {
    biMap.getByKey(key).foreach(k2Map.remove(_))
    biMap.removeKey(key)
    k1Map.remove(key)
  }

  def removeKey2(key: String): V = {
    biMap.getByValue(key).flatMap(k1Map.remove)
    biMap.removeValue(key)
    k2Map.remove(key)
  }
}

object MultiIndexMap {
  def apply[K1, V](): TwoIndexMap[K1, V] = {
    val k1Map = TrieMap[K1, V]()
    val k2Map = new ConcurrentSkipListMap[String, V]()
    val biMap = new BiMap[K1, String](TrieMap[K1, String](), TrieMap[String, K1]())
    new TwoIndexMap[K1, V](k1Map, k2Map, biMap)
  }
}