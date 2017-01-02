package teleporter.integration.cluster.broker.leveldb

import teleporter.integration.cluster.broker.PersistentProtocol.KeyValue
import teleporter.integration.cluster.broker.PersistentService
import teleporter.integration.component.kv.leveldb.LevelTable
import teleporter.integration.utils.Bytes._

/**
  * Created by kui.dai on 2016/7/15.
  */
class LevelDBService(table: LevelTable) extends PersistentService {
  val idKey = "id"

  override def id(): Long = {
    this.synchronized {
      table.get(idKey) match {
        case Some(v) ⇒
          val id = 1L + toLong(v)
          table.put(idKey, id)
          id
        case None ⇒ table.put(idKey, 1L); 1L
      }
    }
  }

  override def range(key: String, start: Int, limit: Int): Seq[KeyValue] = {
    table.range(key).slice(start, start + limit).map { case (k, v) ⇒ KeyValue(k, v) }.toSeq
  }

  override def unsafePut(key: String, value: String): Unit = table.put(key, value)

  override def delete(key: String): Unit = table.remove(key)

  override def atomicPut(key: String, expect: String, update: String): Boolean = table.atomicPut(key, expect, update)

  override def apply(key: String): KeyValue = get(key).get

  override def get(key: String): Option[KeyValue] = table.get(key).map(KeyValue(key, _))
}

object LevelDBService {
  def apply(table: LevelTable): LevelDBService = new LevelDBService(table)
}