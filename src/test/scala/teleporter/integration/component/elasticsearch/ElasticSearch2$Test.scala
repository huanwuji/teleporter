package teleporter.integration.component.elasticsearch

import java.net.InetAddress

import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.VersionType
import org.scalatest.FunSuite

/**
  * Created by huanwuji 
  * date 2017/2/22.
  */
class ElasticSearch2$Test extends FunSuite {
  test("elasticsearch") {
    val settings = Settings.settingsBuilder()
      .put("cluster.name", "rube-es").build()
    val client = TransportClient.builder().settings(settings).build()
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300))
    val indexRequest = new IndexRequest()
    val updateRequest = new UpdateRequest()
    client.prepareIndex("test_mapping3", "teststop", "9")
      .setSource("""{"trade_from": "eeeeeeeeeeeggg"}""")
      .setVersionType(VersionType.EXTERNAL)
      .setVersion(System.currentTimeMillis())
      .execute().get()
  }
}