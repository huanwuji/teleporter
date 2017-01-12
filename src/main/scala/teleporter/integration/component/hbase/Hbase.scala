package teleporter.integration.component.hbase

import org.apache.commons.io.IOUtils
import org.apache.hadoop.hbase.TeleporterHBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import teleporter.integration.core._


/**
  * Created by joker on 15/10/9
  */
object HBase {
  def address(key: String)(implicit center: TeleporterCenter): AutoCloseClientRef[Connection] = {
    val config = center.context.getContext[AddressContext](key).config.mapTo[HBaseMetaBean]
    val conf = new TeleporterHBaseConfiguration()
    conf.addResource(IOUtils.toInputStream(config.hbaseDefault))
    conf.addResource(IOUtils.toInputStream(config.hbaseSite))
    conf.checkConfiguration()
    AutoCloseClientRef(key, ConnectionFactory.createConnection(conf))
  }
}

object HBaseMetaBean {
  val FHBaseDefault = "hbase-default"
  val FHBaseSite = "hbase-site"
}

class HBaseMetaBean(override val underlying: Map[String, Any]) extends AddressMetaBean(underlying) {

  import HBaseMetaBean._

  def hbaseDefault: String = client[String](FHBaseDefault)

  def hbaseSite: String = client[String](FHBaseSite)
}