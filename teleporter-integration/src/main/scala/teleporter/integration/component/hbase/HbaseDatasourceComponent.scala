package teleporter.integration.component.hbase

import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import teleporter.integration.conf.{Conf, PropsSupport}
import teleporter.integration.core._


/**
 * Created by joker on 15/10/9.
 */

class HbaseAddressBuilder(override val conf: Conf.Address)(implicit override val teleporterCenter: TeleporterCenter) extends AddressBuilder[Connection] with PropsSupport {
  override def build: Address[Connection] = {
    val config = HBaseConfiguration.create()
    cmpProps(conf.props).foreach(t2 ⇒ config.set(t2._1, String.valueOf(t2._2)))
    new Address[Connection] {
      override val _conf: Conf.Address = conf

      override val client: Connection = ConnectionFactory.createConnection(config)

      override def close(): Unit = client.close()
    }
  }
}

object HbaseUtil {
  def upsert(tableTmp: HbaseTmp, checkColumn: Column)(implicit conn: Connection): Boolean = {
    val htable = conn.getTable(TableName.valueOf(tableTmp.tableName))
    // htable.incrementColumnValue(Bytes.toBytes(tableTmp.row), Bytes.toBytes(tableTmp.family), Bytes.toBytes(tableTmp.qualifer), 1)
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

