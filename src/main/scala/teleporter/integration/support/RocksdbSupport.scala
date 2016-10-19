package teleporter.integration.support

import org.apache.hadoop.hbase.util.Bytes
import org.rocksdb.{FlushOptions, RocksDB}
import teleporter.integration.core.TId

/**
  * Created by joker on 16/4/8
  */
trait RocksdbSupport[T] {

  protected var db: RocksDB = _

  def openDefaultDatabase(): RocksDB = {

    RocksDB.open("../RocksDb")
  }

  def openDatabase(path: String): RocksDB = {
    RocksDB.open(path)
  }

  def openTable(tableName: String): Unit = {}

  def getByKey(key: T): String

  def deleteByKey(key: T): Unit

  def put[I](key: T, msg: I): Unit = {

  }


  def next(): String = {
    val iter = this.db.newIterator()
    iter.seekToFirst()
    val message = Bytes.toString(iter.value())
    message
  }
}

class RocksdbSupportImpl extends RocksdbSupport[TId] {
  def init(): Unit = {
    this.db = openDefaultDatabase()
  }


  override def getByKey(key: TId): String = {
    val keyBytes = key.toBytes
    val sb = new StringBuffer()
    this.db.keyMayExist(keyBytes, sb)
    sb.toString
  }

  override def deleteByKey(key: TId): Unit = {
    this.db.remove(key.toBytes)
    db.flush(new FlushOptions().setWaitForFlush(true))
  }
}

object RocksdbSupportImpl {
  def apply(): RocksdbSupportImpl = new RocksdbSupportImpl()
}


trait RocksKey {
  def prefix: String

  def tableName: String

  def id: String

  def generate: String = {
    String.format("%s_%s_%s", prefix, tableName, id)
  }
}

case class KuduRocksKey(override val tableName: String, override val id: String) extends RocksKey {
  override def prefix: String = "kudu"
}

case class EsRocksKey(override val tableName: String, override val id: String) extends RocksKey {
  override def prefix: String = "Es"
}
