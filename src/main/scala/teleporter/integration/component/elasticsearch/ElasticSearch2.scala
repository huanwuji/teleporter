package teleporter.integration.component.elasticsearch

import java.net.InetSocketAddress

import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.stream.{Attributes, TeleporterAttributes}
import akka.{Done, NotUsed}
import org.elasticsearch.action._
import org.elasticsearch.action.bulk.{BulkRequest, BulkResponse}
import org.elasticsearch.action.delete.{DeleteRequest, DeleteResponse}
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.action.update.{UpdateRequest, UpdateResponse}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import teleporter.integration.component.{CommonSinkAsyncUnordered, ElasticRecord}
import teleporter.integration.core._
import teleporter.integration.metrics.Metrics
import teleporter.integration.utils.MapBean

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * Created by Yukai.wu on 2015/9/24.
  */
object ElasticSearch2 {
  type ElasticResponse = ActionResponse

  def sink(sinkKey: String)(implicit center: TeleporterCenter): Sink[Message[ElasticRecord], Future[Done]] = {
    flow(sinkKey).toMat(Sink.ignore)(Keep.right)
  }

  def bulkSink(sinkKey: String)(implicit center: TeleporterCenter): Sink[Seq[Message[ElasticRecord]], Future[Done]] = {
    bulkFlow(sinkKey).toMat(Sink.ignore)(Keep.right)
  }

  def flow(sinkKey: String)(implicit center: TeleporterCenter): Flow[Message[ElasticRecord], Message[ElasticRecord], NotUsed] = {
    implicit val ec: ExecutionContext = center.blockExecutionContext
    val sinkContext = center.context.getContext[SinkContext](sinkKey)
    val sinkConfig = sinkContext.config.mapTo[ElasticSearch2SinkMetaBean]
    val bind = Option(sinkConfig.addressBind).getOrElse(sinkKey)
    val addressKey = sinkContext.address().key
    Flow.fromGraph(new ElasticSearch2Sink(
      name = sinkKey,
      parallelism = sinkConfig.parallelism,
      _create = () ⇒ Future {
        center.context.register(addressKey, bind, () ⇒ address(addressKey)).client
      },
      _close = {
        _ ⇒
          center.context.unRegister(addressKey, bind)
          Future.successful(Done)
      })).addAttributes(Attributes(TeleporterAttributes.SupervisionStrategy(sinkKey, sinkContext.config)))
      .via(Metrics.count[Message[ElasticRecord]](sinkKey)(center.metricsRegistry))
  }

  def bulkFlow(sinkKey: String)(implicit center: TeleporterCenter): Flow[Seq[Message[ElasticRecord]], Seq[Message[ElasticRecord]], NotUsed] = {
    val sinkContext = center.context.getContext[SinkContext](sinkKey)
    val sinkConfig = sinkContext.config.mapTo[ElasticSearch2SinkMetaBean]
    val bind = Option(sinkConfig.addressBind).getOrElse(sinkKey)
    val addressKey = sinkContext.address().key
    Flow.fromGraph(new ElasticSearch2BulkSink(
      name = sinkKey,
      parallelism = sinkConfig.parallelism,
      _create = ec ⇒ Future {
        center.context.register(addressKey, bind, () ⇒ address(addressKey)).client
      }(ec),
      _close = {
        (_, _) ⇒
          center.context.unRegister(addressKey, bind)
          Future.successful(Done)
      })).addAttributes(Attributes(TeleporterAttributes.SupervisionStrategy(sinkKey, sinkContext.config)))
      .via(Metrics.count[Seq[Message[ElasticRecord]]](sinkKey)(center.metricsRegistry))
  }

  def address(key: String)(implicit center: TeleporterCenter): AutoCloseClientRef[TransportClient] = {
    implicit val config = center.context.getContext[AddressContext](key).config.mapTo[ElasticAddressMetaBean]
    val addresses = config.hosts.split(",").map(_.split(":")).map {
      case Array(host, port) ⇒ new InetSocketTransportAddress(new InetSocketAddress(host, port.toInt))
    }
    val client = TransportClient.builder()
      .settings(Settings.builder().put(config.settings.toMap.map { case (k, v) ⇒ (k, String.valueOf(v)) }.asJava)).build()
      .addTransportAddresses(addresses: _*)
    AutoCloseClientRef[TransportClient](key, client)
  }
}

class ElasticAddressMetaBean(override val underlying: Map[String, Any]) extends AddressMetaBean(underlying) {
  val FHosts = "hosts"
  val FSettings = "settings"

  def settings: MapBean = client[MapBean](FSettings)

  def hosts: String = client[String](FHosts)
}

class ElasticSearch2SinkMetaBean(override val underlying: Map[String, Any]) extends SinkMetaBean(underlying) {
  val FParallelism = "parallelism"

  def parallelism: Int = client.get[Int](FParallelism).getOrElse(1)
}

class ElasticSearch2Sink(name: String = "elasticsearch2.sink",
                         parallelism: Int,
                         _create: () ⇒ Future[TransportClient],
                         _close: TransportClient ⇒ Future[Done])
  extends CommonSinkAsyncUnordered[TransportClient, Message[ElasticRecord], Message[ElasticRecord]](name, parallelism, identity) {

  override val initialAttributes: Attributes = super.initialAttributes and TeleporterAttributes.BlockingDispatcher

  override def create(executionContext: ExecutionContext): Future[TransportClient] = _create()

  override def write(client: TransportClient, elem: Message[ElasticRecord], executionContext: ExecutionContext): Future[Message[ElasticRecord]] = {
    val promise = Promise[Message[ElasticRecord]]()
    elem.data match {
      case request: UpdateRequest ⇒
        client.update(request, new ActionListener[UpdateResponse] {
          override def onFailure(e: Throwable): Unit = {
            promise.failure(e)
          }

          override def onResponse(response: UpdateResponse): Unit = {
            promise.success(elem)
          }
        })
      case request: IndexRequest ⇒
        client.index(request, new ActionListener[IndexResponse] {
          override def onFailure(e: Throwable): Unit = {
            promise.failure(e)
          }

          override def onResponse(response: IndexResponse): Unit = {
            promise.success(elem)
          }
        })
      case request: DeleteRequest ⇒
        client.delete(request, new ActionListener[DeleteResponse] {
          override def onFailure(e: Throwable): Unit = {
            promise.failure(e)
          }

          override def onResponse(response: DeleteResponse): Unit = {
            promise.success(elem)
          }
        })
    }
    promise.future
  }

  override def close(client: TransportClient, executionContext: ExecutionContext): Future[Done] = _close(client)
}

class ElasticSearch2BulkSink(name: String = "elasticsearch2.sink",
                             parallelism: Int,
                             _create: (ExecutionContext) ⇒ Future[TransportClient],
                             _close: (TransportClient, ExecutionContext) ⇒ Future[Done])
  extends CommonSinkAsyncUnordered[TransportClient, Seq[Message[ElasticRecord]], Seq[Message[ElasticRecord]]](name, parallelism, identity) {

  override val initialAttributes: Attributes = super.initialAttributes and TeleporterAttributes.BlockingDispatcher

  override def create(ec: ExecutionContext): Future[TransportClient] = _create(ec)

  override def write(client: TransportClient, elems: Seq[Message[ElasticRecord]], executionContext: ExecutionContext): Future[Seq[Message[ElasticRecord]]] = {
    val promise = Promise[Seq[Message[ElasticRecord]]]()
    val bulkRequest = new BulkRequest()
    elems.foreach { elem ⇒
      elem.data match {
        case data: IndexRequest ⇒ bulkRequest.add(data)
        case data: UpdateRequest ⇒ bulkRequest.add(data)
        case data: DeleteRequest ⇒ bulkRequest.add(data)
      }
    }
    client.bulk(bulkRequest, new ActionListener[BulkResponse] {
      override def onFailure(e: Throwable): Unit = {
        promise.failure(e)
      }

      override def onResponse(response: BulkResponse): Unit = {
        if (response.hasFailures) {
          promise.failure(new RuntimeException(response.buildFailureMessage()))
        } else {
          promise.success(elems)
        }
      }
    })
    promise.future
  }

  override def close(client: TransportClient, executionContext: ExecutionContext): Future[Done] = _close(client, executionContext)
}