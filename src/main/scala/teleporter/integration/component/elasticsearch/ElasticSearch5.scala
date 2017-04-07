//package teleporter.integration.component.elasticsearch
//
//import java.net.InetSocketAddress
//
//import akka.stream.scaladsl.{Flow, Keep, Sink}
//import akka.stream.{Attributes, TeleporterAttributes}
//import akka.{Done, NotUsed}
//import org.elasticsearch.action.ActionListener
//import org.elasticsearch.action.bulk.BulkResponse
//import org.elasticsearch.action.update.UpdateResponse
//import org.elasticsearch.client.transport.TransportClient
//import org.elasticsearch.common.settings.Settings
//import org.elasticsearch.common.transport.InetSocketTransportAddress
//import teleporter.integration.component.{CommonSinkAsyncUnordered, ElasticRecord}
//import teleporter.integration.core._
//import teleporter.integration.utils.MapBean
//
//import scala.collection.JavaConverters._
//import scala.concurrent.{ExecutionContext, Future, Promise}
//
///**
//  * Created by Yukai.wu on 2015/9/24.
//  */
//object ElasticSearch5 {
//  def sink(sinkKey: String)(implicit center: TeleporterCenter): Sink[Message[ElasticRecord], Future[Done]] = {
//    flow(sinkKey).toMat(Sink.ignore)(Keep.right)
//  }
//
//  def bulkSink(sinkKey: String)(implicit center: TeleporterCenter): Sink[Seq[Message[ElasticRecord]], Future[Done]] = {
//    bulkFlow(sinkKey).toMat(Sink.ignore)(Keep.right)
//  }
//
//  def flow(sinkKey: String)(implicit center: TeleporterCenter): Flow[Message[ElasticRecord], Message[ElasticRecord], NotUsed] = {
//    implicit val ec: ExecutionContext = center.blockExecutionContext
//    val sinkContext = center.context.getContext[SinkContext](sinkKey)
//    val sinkConfig = sinkContext.config.mapTo[ElasticSearch5SinkMetaBean]
//    val bind = Option(sinkConfig.addressBind).getOrElse(sinkKey)
//    val addressKey = sinkContext.address().key
//    Flow.fromGraph(new ElasticSearch5Sink(
//      parallelism = sinkConfig.parallelism,
//      _create = () ⇒ Future {
//        center.context.register(addressKey, bind, () ⇒ address(addressKey)).client
//      },
//      _close = {
//        _ ⇒
//          center.context.unRegister(addressKey, bind)
//          Future.successful(Done)
//      })).addAttributes(Attributes(TeleporterAttributes.SupervisionStrategy(sinkKey, sinkContext.config)))
//  }
//
//  def bulkFlow(sinkKey: String)(implicit center: TeleporterCenter): Flow[Seq[Message[ElasticRecord]], Seq[Message[ElasticRecord]], NotUsed] = {
//    val sinkContext = center.context.getContext[SinkContext](sinkKey)
//    val sinkConfig = sinkContext.config.mapTo[ElasticSearch5SinkMetaBean]
//    val bind = Option(sinkConfig.addressBind).getOrElse(sinkKey)
//    val addressKey = sinkContext.address().key
//    Flow.fromGraph(new ElasticSearch5BulkSink(
//      parallelism = sinkConfig.parallelism,
//      _create = ec ⇒ Future {
//        center.context.register(addressKey, bind, () ⇒ address(addressKey)).client
//      }(ec),
//      _close = {
//        (_, _) ⇒
//          center.context.unRegister(addressKey, bind)
//          Future.successful(Done)
//      })).addAttributes(Attributes(TeleporterAttributes.SupervisionStrategy(sinkKey, sinkContext.config)))
//  }
//
//  def address(key: String)(implicit center: TeleporterCenter): AutoCloseClientRef[TransportClient] = {
//    implicit val config = center.context.getContext[AddressContext](key).config.mapTo[ElasticAddressMetaBean]
//    val addresses = config.hosts.split(",").map(_.split(":")).map {
//      case Array(host, port) ⇒ new InetSocketTransportAddress(new InetSocketAddress(host, port.toInt))
//    }
//    val client = new PreBuiltTransportClient(Settings.builder().put(config.setting.toMap.asJava).build())
//      .addTransportAddresses(addresses: _*)
//    AutoCloseClientRef[TransportClient](key, client)
//  }
//}
//
//object ElasticAddressMetaBean {
//  val FHosts = "hosts"
//  val FSetting = "setting"
//}
//
//class ElasticAddressMetaBean(override val underlying: Map[String, Any]) extends AddressMetaBean(underlying) {
//
//  import ElasticAddressMetaBean._
//
//  def setting: MapBean = client[MapBean](FSetting)
//
//  def hosts: String = client[String](FHosts)
//}
//
//object ElasticSearch5SinkMetaBean {
//  val FParallelism = "parallelism"
//}
//
//class ElasticSearch5SinkMetaBean(override val underlying: Map[String, Any]) extends SinkMetaBean(underlying) {
//
//  import ElasticSearch5SinkMetaBean._
//
//  def parallelism: Int = client.get[Int](FParallelism).getOrElse(1)
//}
//
//class ElasticSearch5Sink(parallelism: Int,
//                         _create: () ⇒ Future[TransportClient],
//                         _close: TransportClient ⇒ Future[Done])
//  extends CommonSinkAsyncUnordered[TransportClient, Message[ElasticRecord], Message[ElasticRecord]]("kafka.sink", parallelism) {
//
//  override val initialAttributes: Attributes = super.initialAttributes and TeleporterAttributes.BlockingDispatcher
//
//  override def create(executionContext: ExecutionContext): Future[TransportClient] = _create()
//
//  override def write(client: TransportClient, elem: Message[ElasticRecord], executionContext: ExecutionContext): Future[Message[ElasticRecord]] = {
//    val promise = Promise[Message[ElasticRecord]]()
//    client.update(elem.data, new ActionListener[UpdateResponse] {
//      override def onFailure(e: Exception): Unit = {
//        promise.failure(e)
//      }
//
//      override def onResponse(response: UpdateResponse): Unit = {
//        promise.success(elem)
//      }
//    })
//    promise.future
//  }
//
//  override def close(client: TransportClient, executionContext: ExecutionContext): Future[Done] = _close(client)
//}
//
//class ElasticSearch5BulkSink(parallelism: Int,
//                             _create: (ExecutionContext) ⇒ Future[TransportClient],
//                             _close: (TransportClient, ExecutionContext) ⇒ Future[Done])
//  extends CommonSinkAsyncUnordered[TransportClient, Seq[Message[ElasticRecord]], Seq[Message[ElasticRecord]]]("kafka.sink", parallelism) {
//
//  override val initialAttributes: Attributes = super.initialAttributes and TeleporterAttributes.BlockingDispatcher
//
//  override def create(ec: ExecutionContext): Future[TransportClient] = _create(ec)
//
//  override def write(client: TransportClient, elem: Seq[Message[ElasticRecord]], executionContext: ExecutionContext): Future[Seq[Message[ElasticRecord]]] = {
//    val promise = Promise[Seq[Message[ElasticRecord]]]()
//    val bulkRequest = client.prepareBulk()
//    elem.foreach(e ⇒ bulkRequest.add(e.data))
//    bulkRequest.execute(new ActionListener[BulkResponse] {
//      override def onFailure(e: Exception): Unit = {
//        promise.failure(e)
//      }
//
//      override def onResponse(response: BulkResponse): Unit = {
//        if (response.hasFailures) {
//          promise.failure(new RuntimeException(response.buildFailureMessage()))
//        } else {
//          promise.success(elem)
//        }
//      }
//    })
//    promise.future
//  }
//
//  override def close(client: TransportClient, executionContext: ExecutionContext): Future[Done] = _close(client, executionContext)
//}