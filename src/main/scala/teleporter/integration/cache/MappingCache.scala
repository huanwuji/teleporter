package teleporter.integration.cache

import java.util
import java.util.concurrent.locks.Lock
import javax.annotation.concurrent.ThreadSafe

import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.util.concurrent.Striped
import teleporter.integration.core.TeleporterCenter
import teleporter.integration.utils.Converter

import scala.concurrent.duration.{Duration, _}

/**
  * Created by huanwuji 
  * date 2016/12/22.
  */
trait MappingCache[T <: CacheValue] {
  val localCacheTrustTime: Long
  val localCacheExpirePeriod: Duration
  val localCache: LocalCache
  val stripes: Int
  val locks: Striped[Lock] = Striped.lazyWeakLock(stripes)

  @ThreadSafe
  def compareAndPut(key: Array[Byte], timeVersion: Long, externalGet: ⇒ Option[T], update: T, compare: (Option[T], T) ⇒ Boolean)
                   (implicit converter: Converter[T]): Boolean = {
    val lock = locks.get(util.Arrays.hashCode(key))
    lock.lock()
    try {
      val v1 = get(key, timeVersion, externalGet)
      val c = compare(v1, update)
      if (c && timeVersion > localCacheTrustTime) {
        put(key, update)
      }
      c
    } finally {
      lock.unlock()
    }
  }

  def get(key: Array[Byte], timeVersion: Long = -1, externalGet: ⇒ Option[T])
         (implicit converter: Converter[T]): Option[T] = {
    localCache.get(key).flatMap(converter.to(_))
      .orElse {
        if (timeVersion == -1 || timeVersion < localCacheTrustTime) {
          externalGet
        } else {
          None
        }
      }
  }

  def put(key: Array[Byte], value: T): Unit = {
    localCache.put(key, value.toBytes)
  }

  def clear(): Unit = {
    localCache.clear(localCacheExpirePeriod)
  }

  def clearAll(): Unit = {
    localCache.clear()
  }
}

class MappingCacheImpl[T <: CacheValue](val localCache: LocalCache, val localCacheTrustTime: Long, val localCacheExpirePeriod: Duration, val stripes: Int) extends MappingCache[T]

object MappingCache {
  def newCache[T <: CacheValue](localCache: LocalCache, localCacheTrustTime: Long, localCacheExpirePeriod: Duration, stripes: Int = 16): MappingCache[T] =
    new MappingCacheImpl(localCache, localCacheTrustTime, localCacheExpirePeriod, stripes)

  def apply[T <: CacheValue](cacheName: String, localCacheTrustTime: Long, localCacheExpirePeriod: Duration, stripes: Int = 16)(implicit center: TeleporterCenter): MappingCache[T] = {
    val guavaCache: Cache[String, Array[Byte]] = CacheBuilder.newBuilder().maximumSize(1000).build[String, Array[Byte]]()
    val guavaLocalCache = GuavaLocalCache(guavaCache)
    val kvdbCache = KVDBCache("../../cache", "leveldb", "leveldb", cacheName, 7.days)
    val combineCache = CombineCache(Array(guavaLocalCache, kvdbCache))
    newCache(combineCache, localCacheTrustTime, localCacheExpirePeriod, stripes)
  }
}