package teleporter.integration.cache

import com.google.common.cache.Cache
import teleporter.integration.utils.Bytes
import teleporter.integration.utils.Bytes._

import scala.concurrent.duration.Duration

/**
  * Created by huanwuji 
  * date 2016/12/22.
  */
trait CacheValue {
  val created: Long
  val updated: Long

  def toBytes: Array[Byte]
}

class CacheValueImpl(val created: Long, val updated: Long) extends CacheValue {
  override def toBytes: Array[Byte] = Bytes.toBytes(created) ++ Bytes.toBytes(updated)
}

object CacheValue {
  def apply(created: Long, updated: Long): CacheValue = new CacheValueImpl(created, updated)

  def apply(bytes: Array[Byte]): CacheValue = new CacheValueImpl(bytes.slice(0, 8), bytes.slice(8, 16))
}

trait LocalCache {
  def get(key: Array[Byte]): Option[Array[Byte]]

  def getIfPresent(key: Array[Byte]): Array[Byte]

  def put(key: Array[Byte], value: Array[Byte]): Unit

  def clear(localCacheExpirePeriod: Duration): Unit

  def clear(): Unit
}

trait GuavaLocalCache extends LocalCache {
  def cache: Cache[String, Array[Byte]]

  override def get(key: Array[Byte]): Option[Array[Byte]] = Option(cache.getIfPresent())

  override def getIfPresent(key: Array[Byte]): Array[Byte] = get(key).orNull

  override def put(key: Array[Byte], value: Array[Byte]): Unit = cache.put(key, value)

  override def clear(expired: Duration): Unit = cache.cleanUp()

  override def clear(): Unit = clear(Duration.Zero)
}

class GuavaLocalCacheImpl(val cache: Cache[String, Array[Byte]]) extends GuavaLocalCache

object GuavaLocalCache {
  def apply(cache: Cache[String, Array[Byte]]): GuavaLocalCache = new GuavaLocalCacheImpl(cache)
}

trait CombineCache extends LocalCache {
  val localCaches: Array[LocalCache]

  override def get(key: Array[Byte]): Option[Array[Byte]] = {
    var i = 0
    while (i < localCaches.length) {
      val value = localCaches(i).get(key)
      if (value.isDefined) return value
      i += 1
    }
    None
  }

  override def getIfPresent(key: Array[Byte]): Array[Byte] = get(key).orNull

  override def put(key: Array[Byte], value: Array[Byte]): Unit = {
    localCaches.reverse.foreach(_.put(key, value))
  }

  override def clear(localCacheExpirePeriod: Duration): Unit = {
    localCaches.reverse.foreach(_.clear(localCacheExpirePeriod))
  }

  override def clear(): Unit = clear(Duration.Zero)
}

class CombineCacheImpl(val localCaches: Array[LocalCache]) extends CombineCache

object CombineCache {
  def apply(localCaches: Array[LocalCache]) = new CombineCacheImpl(localCaches)
}