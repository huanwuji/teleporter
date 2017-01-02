package teleporter.integration.component.kv.leveldb

import java.io.File
import java.util

import org.iq80.leveldb.impl.Iq80DBFactory
import org.iq80.leveldb.{DB, Options}
import teleporter.integration.component.kv.KVOperator
import teleporter.integration.utils.Bytes

import scala.collection.concurrent.TrieMap

/**
  * Created by kui.dai on 2016/7/15.
  */
object LevelDBs {
  val dbs: TrieMap[String, DB] = TrieMap[String, DB]()

  var defaultPath = "../../leveldb"

  def apply(dbName: String, path: String): DB = {
    dbs.getOrElseUpdate(dbName, {
      val options = new Options
      options.createIfMissing()
      Iq80DBFactory.factory.open(new File(path), options)
    })
  }

  def apply(dbName: String, op: ⇒ DB): DB = {
    dbs.getOrElseUpdate(dbName, op)
  }

  def applyTable(dbName: String, tableName: String): LevelTable = {
    val db = this.apply(dbName, defaultPath)
    LevelTable(db, tableName)
  }

  def close(dbName: String): Unit = dbs.get(dbName).foreach(_.close())

  def close(): Unit = dbs.keys.foreach(close)
}

class LevelTable(db: DB, tableName: Array[Byte]) extends KVOperator {
  override def apply(key: Array[Byte]): Array[Byte] = get(fullKey(key)).get

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

  override def remove(key: Array[Byte]): Unit = db.delete(fullKey(key))

  override def range(key: Array[Byte]): Iterator[(Array[Byte], Array[Byte])] = {
    val iterator = db.iterator()
    val _key = fullKey(key)
    if (key.isEmpty) iterator.seekToFirst() else iterator.seek(_key)
    new Iterator[(Array[Byte], Array[Byte])] {
      var currKV: util.Map.Entry[Array[Byte], Array[Byte]] = _

      override def hasNext: Boolean = {
        iterator.hasNext && {
          if (currKV == null) {
            currKV = iterator.next()
          }
          currKV.getKey.startsWith(_key)
        }
      }

      override def next(): (Array[Byte], Array[Byte]) = {
        val v = currKV.getKey.drop(tableName.length) → currKV.getValue
        currKV = null
        v
      }
    }
  }

  private def fullKey(key: Array[Byte]): Array[Byte] = tableName ++ key
}

object LevelTable {
  def apply(db: DB, tableName: String): LevelTable = new LevelTable(db, Bytes.toBytes(tableName))
}

object LevelDBComponent