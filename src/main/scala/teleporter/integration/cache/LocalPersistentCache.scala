package teleporter.integration.cache

import akka.actor.{Actor, Props}
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.cache.KVDBCache.CacheType
import teleporter.integration.cache.PersistentCacheMonitor.ClearExpired
import teleporter.integration.component.kv.KVOperator
import teleporter.integration.component.kv.leveldb.{LevelDBs, LevelTable}
import teleporter.integration.component.kv.rocksdb.{RocksDBs, RocksTable}
import teleporter.integration.core.LocalStatus.StartLocalPersistentCache
import teleporter.integration.core.TeleporterCenter

import scala.concurrent.duration.{Duration, FiniteDuration}

/**
  * Created by huanwuji 
  * date 2016/12/27.
  */
class LocalPersistentCache

trait KVDBCache extends LocalCache {
  def kVOperator: KVOperator

  override def get(key: Array[Byte]): Option[Array[Byte]] = kVOperator.get(key)

  override def getIfPresent(key: Array[Byte]): Array[Byte] = get(key).orNull

  override def put(key: Array[Byte], value: Array[Byte]): Unit = kVOperator.put(key, value)

  override def clear(expired: Duration): Unit = {
    expired match {
      case Duration.Inf ⇒
      case Duration.Zero ⇒ kVOperator.range().foreach(t2 ⇒ kVOperator.remove(t2._1))
      case d: FiniteDuration ⇒
        kVOperator.range().foreach {
          case (key, value) ⇒
            val expireTime = System.currentTimeMillis() - d.toMillis
            if (CacheValue(value).updated < expireTime) {
              kVOperator.remove(key)
            }
        }
      case _ ⇒
    }
  }

  override def clear(): Unit = clear(Duration.Zero)
}

class KVDBCacheImpl(val kVOperator: KVOperator) extends KVDBCache

object KVDBCache {

  object CacheType {
    val levelDB = "levelDB"
    val rocksDB = "rocksDB"
  }

  def apply(path: String, database: String, storageType: String, tableName: String, expired: Duration)(implicit center: TeleporterCenter): LocalCache = {
    center.localStatusRef ! StartLocalPersistentCache(path, database, storageType, tableName, expired)
    this.newCache(path, database, storageType, tableName, expired)
  }

  def newCache(path: String, database: String, storageType: String, tableName: String, expired: Duration): LocalCache = {
    val kVOperator = storageType match {
      case CacheType.levelDB ⇒
        LevelTable(LevelDBs(path, database), tableName)
      case CacheType.rocksDB ⇒
        RocksTable(RocksDBs(path, database), tableName)
    }
    new KVDBCacheImpl(kVOperator)
  }
}


case class PersistentCacheInfo(path: String, database: String, storageType: String, tableName: String, created: Long, updated: Long, expired: Long, maxSize: Long, count: Long, latestClearTime: Long) {

  def canEqual(other: Any): Boolean = other.isInstanceOf[PersistentCacheInfo]

  override def equals(other: Any): Boolean = other match {
    case that: PersistentCacheInfo ⇒
      (that canEqual this) &&
        path == that.path &&
        database == that.database &&
        storageType == that.storageType &&
        tableName == that.tableName
    case _ ⇒ false
  }

  override def hashCode(): Int = {
    val state = Seq(path, database, storageType, tableName)
    state.map(_.hashCode()).foldLeft(0)((a, b) ⇒ 31 * a + b)
  }
}

object PersistentCacheMonitor {

  case class ClearExpired(cacheInfo: PersistentCacheInfo)

  case class Success(cachesInfo: PersistentCacheInfo)

  def props(): Props = Props[PersistentCacheMonitorActor]
}

class PersistentCacheMonitorActor extends Actor with Logging {
  override def receive: Receive = {
    case ClearExpired(cacheInfo) ⇒
      clearExpired(cacheInfo)
      sender() ! PersistentCacheMonitor.Success(cacheInfo)
  }

  def clearExpired(info: PersistentCacheInfo): PersistentCacheInfo = {
    val kVOperator = info.storageType match {
      case CacheType.levelDB ⇒ LevelTable(LevelDBs(info.database, info.path), info.tableName)
      case CacheType.rocksDB ⇒ RocksTable(RocksDBs(info.database, info.path), info.tableName)
    }
    var (count, delete) = (0, 0)
    kVOperator.range().foreach {
      case (k, v) ⇒
        val expiredTime = System.currentTimeMillis() - info.expired
        count += 1
        if (expiredTime - CacheValue(v).updated > 0) {
          kVOperator.remove(k)
          delete += 1
        }
    }
    logger.info(s"Clear local $delete expired records, count:$count, $info")
    info.copy(count = count, latestClearTime = System.currentTimeMillis())
  }
}