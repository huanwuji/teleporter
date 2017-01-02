package teleporter.integration.component

import java.net.InetSocketAddress

import akka.actor.Props
import akka.stream.actor.ActorSubscriberMessage.OnNext
import com.typesafe.scalalogging.LazyLogging
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import teleporter.integration.ClientApply
import teleporter.integration.core._
import teleporter.integration.utils.MapBean

/**
  * Created by Yukai.wu on 2015/9/24.
  */
object ElasticAddressMetaBean {
  val FHosts = "hosts"
  val FSetting = "setting"
}

class ElasticAddressMetaBean(override val underlying: Map[String, Any]) extends AddressMetaBean(underlying) {

  import ElasticAddressMetaBean._

  def setting: MapBean = client[MapBean](FSetting)

  def hosts: String = client[String](FHosts)
}

object ElasticComponent {
  def elasticClientApply: ClientApply = (key, center) ⇒ {
    implicit val config = center.context.getContext[AddressContext](key).config.mapTo[ElasticAddressMetaBean]
    val addresses = config.hosts.split(",").map(_.split(":")).map {
      case Array(host, port) ⇒ new InetSocketTransportAddress(new InetSocketAddress(host, port.toInt))
    }
    val client = new PreBuiltTransportClient(Settings.builder().put(config.setting.toMap).build())
      .addTransportAddresses(addresses: _*)
    AutoCloseClientRef[TransportClient](key, client)
  }
}

class ElasticSubscriberWork(val client: TransportClient) extends SubscriberWorker[TransportClient] with LazyLogging {
  override def handle(onNext: OnNext, nrOfRetries: Int): Unit = {
    onNext.element match {
      case message: TeleporterElasticRecord ⇒
        client.update(message.data, new ActionListener[UpdateResponse] {
          override def onFailure(e: Exception): Unit = {
            failure(onNext, e, nrOfRetries)
          }

          override def onResponse(response: UpdateResponse): Unit = {
            success(onNext)
          }
        })
      case messages: Seq[TeleporterElasticRecord] ⇒
        val bulkRequest = client.prepareBulk()
        messages.map(msg ⇒ bulkRequest.add(msg.data))
        bulkRequest.execute(new ActionListener[BulkResponse] {
          override def onFailure(e: Exception): Unit = {
            failure(onNext, e, nrOfRetries)
          }

          override def onResponse(response: BulkResponse): Unit = {
            if (response.hasFailures) {
              failure(onNext, new RuntimeException(response.buildFailureMessage()), nrOfRetries)
            } else {
              success(onNext)
            }
          }
        })
    }
  }
}

class ElasticSubscriber(val key: String)(implicit val center: TeleporterCenter) extends SubscriberSupport[TransportClient] {
  override def workProps: Props = Props(classOf[ElasticSubscriberWork], client)
}