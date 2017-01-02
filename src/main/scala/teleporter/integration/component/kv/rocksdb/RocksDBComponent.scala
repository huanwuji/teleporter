package teleporter.integration.component.kv.rocksdb

import org.rocksdb.{Options, RocksDB}
import teleporter.integration.component.kv.KVOperator
import teleporter.integration.utils.Bytes

import scala.collection.concurrent.TrieMap

/**
  * Created by kui.dai on 2016/7/15.
  */
object RocksDBs {
  RocksDB.loadLibrary()
  val defaultPath = "../../rocksdb"
  val dbs: TrieMap[String, RocksDB] = TrieMap[String, RocksDB]()

  def apply(dbName: String, path: String): RocksDB = {
    dbs.getOrElseUpdate(dbName, {
      val options = new Options().setCreateIfMissing(true)
      RocksDB.open(options, s"$path/$dbName")
    })
  }

  def apply(dbName: String, op: ⇒ RocksDB): RocksDB = {
    dbs.getOrElseUpdate(dbName, op)
  }

  def applyTable(dbName: String, tableName: String): RocksTable = {
    val db = this.apply(dbName, defaultPath)
    RocksTable(db, tableName)
  }

  def close(dbName: String): Unit = dbs.get(dbName).foreach(_.close())

  def close(): Unit = dbs.keys.foreach(close)
}

class RocksTable(db: RocksDB, tableName: Array[Byte]) extends KVOperator {
  override def apply(key: Array[Byte]): Array[Byte] = db.get(fullKey(key))

  override def get(key: Array[Byte]): Option[Array[Byte]] = Option(db.get(fullKey(key)))

  override def put(key: Array[Byte], value: Array[Byte]): Unit = db.put(fullKey(key), value)

  override def atomicPut(key: Array[Byte], expectValue: Array[Byte], updateValue: Array[Byte]): Boolean = {
    synchronized {
      val _key = fullKey(key)
      get(_key) match {
        case Some(value) ⇒
          if (expectValue sameElements value) {
            put(_key, updateValue)
            true
          } else {
            false
          }
        case None ⇒ put(key, updateValue); true
      }
    }
  }

  override def remove(key: Array[Byte]): Unit = db.remove(fullKey(key))

  override def range(key: Array[Byte]): Iterator[(Array[Byte], Array[Byte])] = {
    val iterator = db.newIterator()
    val _key = fullKey(key)
    iterator.seek(_key)
    new Iterator[(Array[Byte], Array[Byte])] {
      override def hasNext: Boolean = iterator.isValid && iterator.key().startsWith(_key)

      override def next(): (Array[Byte], Array[Byte]) = {
        val v = iterator.key().drop(tableName.length) → iterator.value()
        iterator.next()
        v
      }
    }
  }

  private def fullKey(key: Array[Byte]): Array[Byte] = tableName ++ key
}

object RocksTable {
  def apply(db: RocksDB, tableName: String): RocksTable = new RocksTable(db, Bytes.toBytes(tableName))
}

object RocksDBComponent