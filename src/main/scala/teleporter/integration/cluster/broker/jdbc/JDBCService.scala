package teleporter.integration.cluster.broker.jdbc

import javax.sql.DataSource

import org.apache.logging.log4j.scala.Logging
import teleporter.integration.cluster.broker.PersistentProtocol.KeyValue
import teleporter.integration.cluster.broker.PersistentService
import teleporter.integration.component.jdbc.{PreparedSql, ResultSetMapper, SqlSupport, Upsert}
import teleporter.integration.utils.MapBean
import teleporter.integration.utils.MapBean._

import scala.concurrent.duration._

/**
  * Created by kui.dai on 2016/7/15.
  */
class JDBCService(tableName: String)(implicit dataSource: DataSource) extends PersistentService with SqlSupport with Logging {

  import ResultSetMapper.MapResultSetMapper

  val timeout: FiniteDuration = 1.minutes
  val KEY_FIELD = "k"
  val VALUE_FIELD = "v"
  val ID_KEY = "id"

  override def id(): Long = {
    val seq = asLong(apply(ID_KEY).value).get
    update(dataSource.getConnection(), PreparedSql(s"update $tableName set $VALUE_FIELD = $VALUE_FIELD + 1 where $KEY_FIELD = ?", Seq(ID_KEY)))
    val incSeq = asLong(apply(ID_KEY).value).get
    if (seq + 1 == incSeq) {
      incSeq
    } else {
      Thread.sleep(100)
      id()
    }
  }

  private def mapToKeyValue(map: Map[String, Any]): KeyValue = {
    val mapBean: MapBean = map
    KeyValue(mapBean[String](KEY_FIELD), mapBean[String](VALUE_FIELD))
  }

  override def range(key: String, start: Int, limit: Int): Seq[KeyValue] =
    queryToMap(dataSource.getConnection(), PreparedSql(s"select * from $tableName where $KEY_FIELD like '$key%'"))
      .map(mapToKeyValue).toSeq

  override def apply(key: String): KeyValue = get(key) match {
    case None ⇒ throw new NoSuchElementException("key not found: " + key)
    case Some(v) ⇒ v
  }

  override def get(key: String): Option[KeyValue] =
    one(dataSource.getConnection, PreparedSql(s"select * from $tableName where $KEY_FIELD = ?", Seq(key))).map { m ⇒
      val mapBean: MapBean = m
      KeyValue(mapBean[String](KEY_FIELD), mapBean[String](VALUE_FIELD))
    }

  override def unsafePut(key: String, value: String): Unit = {
    val data = Map(KEY_FIELD → key, VALUE_FIELD → value)
    doAction(dataSource, Upsert(updateSql(tableName, KEY_FIELD, data), insertIgnoreSql(tableName, data)))
  }

  override def delete(key: String): Unit = update(dataSource.getConnection(), PreparedSql(s"delete from $tableName where $KEY_FIELD = '$key'"))

  override def unsafeAtomicPut(key: String, expect: String, updated: String): Boolean =
    update(dataSource.getConnection,
      PreparedSql(s"update $tableName set $VALUE_FIELD = ? where $KEY_FIELD = ? and $VALUE_FIELD = ?", Seq(updated, key, expect))) == 1
}

object JDBCService {
  def apply(tableName: String)(implicit dataSource: DataSource): JDBCService = new JDBCService(tableName)
}