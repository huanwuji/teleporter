package teleporter.integration.component.hbase

import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import teleporter.integration.ClientApply
import teleporter.integration.core._


/**
  * Created by joker on 15/10/9
  */

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

object HbaseComponent {
  val hbase: ClientApply[Connection] = (key, center) ⇒ {
    val hBaseConfig = HBaseConfiguration.create()
    AutoCloseClientRef(key, ConnectionFactory.createConnection(hBaseConfig))
  }
}