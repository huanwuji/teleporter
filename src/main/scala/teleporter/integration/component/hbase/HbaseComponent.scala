package teleporter.integration.component.hbase

import org.apache.commons.io.IOUtils
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, TeleporterHBaseConfiguration}
import teleporter.integration.ClientApply
import teleporter.integration.core._


/**
  * Created by joker on 15/10/9
  */
object HbaseMetaBean {
  val FHbaseDefault = "hbase-default"
  val FHbaseSite = "hbase-site"
}

class HbaseMetaBean(override val underlying: Map[String, Any]) extends AddressMetaBean(underlying) {

  import HbaseMetaBean._

  def hbaseDefault: String = client[String](FHbaseDefault)

  def hbaseSite: String = client[String](FHbaseSite)
}

object HbaseComponent {
  def hbaseApply: ClientApply = (key, center) ⇒ {
    val config = center.context.getContext[AddressContext](key).config.mapTo[HbaseMetaBean]
    val conf = new TeleporterHBaseConfiguration()
    conf.addResource(IOUtils.toInputStream(config.hbaseDefault))
    conf.addResource(IOUtils.toInputStream(config.hbaseSite))
    conf.checkConfiguration()
    AutoCloseClientRef(key, ConnectionFactory.createConnection(conf))
  }
}

object HbaseUtil {
  def upsert(tableTmp: HbaseTmp, checkColumn: Column)(implicit conn: Connection): Boolean = {
    val htable = conn.getTable(TableName.valueOf(tableTmp.tableName))
    val put: Put = new Put(Bytes.toBytes(tableTmp.row))
    tableTmp.colummns.foreach(
      c ⇒ {
        put.addColumn(Bytes.toBytes(tableTmp.family), Bytes.toBytes(c.columnName), Bytes.toBytes(c.colmnValue))
      }
    )

    if (htable.checkAndPut(Bytes.toBytes(tableTmp.row), Bytes.toBytes(tableTmp.family), Bytes.toBytes(checkColumn.columnName), null, put)) {
      true
    }
    else {
      htable.checkAndPut(Bytes.toBytes(tableTmp.row), Bytes.toBytes(tableTmp.family), Bytes.toBytes(checkColumn.columnName), CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes(checkColumn.colmnValue), put)
    }
  }
}


case class HbaseTmp(tableName: String, row: String, family: String, colummns: Seq[Column])

case class Column(columnName: String, colmnValue: String)