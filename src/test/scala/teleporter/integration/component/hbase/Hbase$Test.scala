package teleporter.integration.component.hbase

import java.util.Properties

import com.google.common.io.Resources
import io.leopard.javahost.JavaHost
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.scalatest.FunSuite

/**
  * Created by huanwuji 
  * date 2017/3/1.
  */
class Hbase$Test extends FunSuite {
  val props = new Properties
  props.load(IOUtils.toInputStream(
    """
       host1=localhost
    """))
  JavaHost.updateVirtualDns(props)
  test("hdfs connect") {
    val conf = new Configuration(false)
    //    conf.addResource("/core-default.xml")
    //    conf.addResource("/core-site.xml")
    //    conf.addResource("/mapred-default.xml")
    //    conf.addResource("/mapred-site.xml")
    //    conf.addResource("/yarn-default.xml")
    //    conf.addResource("/yarn-site.xml")
    //    conf.addResource("/hdfs-default.xml")
    //    conf.addResource("/hdfs-site.xml")
    //    conf.reloadConfiguration()

    conf.addResource(Resources.getResource("core-site.xml").openStream())
    conf.addResource(Resources.getResource("hdfs-site.xml").openStream())
    conf.addResource(Resources.getResource("ssl-client.xml").openStream())
    conf.addResource(Resources.getResource("hbase-site.xml").openStream())
    conf.reloadConfiguration()

    //    val hbaseConf = HBaseConfiguration.create()

    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.META_TABLE_NAME)
    table.close()
    connection.close()
    //    conf.addResource("/core-default.xml")
    //    conf.addResource("/core-site.xml")
    //    conf.addResource("/mapred-default.xml")
    //    conf.addResource("/mapred-site.xml")
    //    conf.addResource("/yarn-default.xml")
    //    conf.addResource("/yarn-site.xml")
    //    conf.addResource("/hdfs-default.xml")
    //    conf.addResource("/hdfs-site.xml")
    //    conf.reloadConfiguration()

  }
}
