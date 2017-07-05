package teleporter.integration.component.hbase

import java.util.{Collections, Properties}

import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.stream.{Attributes, TeleporterAttributes}
import akka.{Done, NotUsed}
import io.leopard.javahost.JavaHost
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Row, Table}
import teleporter.integration.component._
import teleporter.integration.core._
import teleporter.integration.metrics.Metrics

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}


/**
  * Created by joker on 15/10/9
  */
object Hbase {
  def sink(sinkKey: String)(implicit center: TeleporterCenter): Sink[Message[HbaseRecord], Future[Done]] = {
    flow(sinkKey).toMat(Sink.ignore)(Keep.right)
  }

  def bulkSink(sinkKey: String)(implicit center: TeleporterCenter): Sink[Seq[Message[HbaseRecord]], Future[Done]] = {
    bulkFlow(sinkKey).toMat(Sink.ignore)(Keep.right)
  }

  def flow(sinkKey: String)(implicit center: TeleporterCenter): Flow[Message[HbaseRecord], Message[HbaseResult], NotUsed] = {
    implicit val ec: ExecutionContext = center.blockExecutionContext
    val sinkContext = center.context.getContext[SinkContext](sinkKey)
    val sinkConfig = sinkContext.config.mapTo[HbaseSinkMetaBean]
    val bind = Option(sinkConfig.addressBind).getOrElse(sinkKey)
    val addressKey = sinkContext.address().key
    Flow.fromGraph(new HbaseSink(
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
      .via(Metrics.count[Message[HbaseResult]](sinkKey)(center.metricsRegistry))
  }

  def bulkFlow(sinkKey: String)(implicit center: TeleporterCenter): Flow[Seq[Message[HbaseRecord]], Seq[Message[HbaseResult]], NotUsed] = {
    val sinkContext = center.context.getContext[SinkContext](sinkKey)
    val sinkConfig = sinkContext.config.mapTo[HbaseSinkMetaBean]
    val bind = Option(sinkConfig.addressBind).getOrElse(sinkKey)
    val addressKey = sinkContext.address().key
    Flow.fromGraph(new HbaseBulkSink(
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
      .via(Metrics.count[Seq[Message[HbaseResult]]](sinkKey)(center.metricsRegistry))
  }

  def address(addressKey: String)(implicit center: TeleporterCenter): AutoCloseClientRef[Connection] = {
    val config = center.context.getContext[AddressContext](addressKey).config.mapTo[HbaseMetaBean]
    config.hosts.foreach { hosts ⇒
      val properties = new Properties()
      properties.load(IOUtils.toInputStream(hosts))
      JavaHost.updateVirtualDns(properties)
    }
    val conf = new Configuration(false)
    config.coreSite.foreach(t ⇒ conf.addResource(IOUtils.toInputStream(t)))
    config.hdfsSite.foreach(t ⇒ conf.addResource(IOUtils.toInputStream(t)))
    config.sslClient.foreach(t ⇒ conf.addResource(IOUtils.toInputStream(t)))
    config.hbaseSite.foreach(t ⇒ conf.addResource(IOUtils.toInputStream(t)))
    AutoCloseClientRef(addressKey, ConnectionFactory.createConnection(conf))
  }
}

case class HbaseAction(tableName: String, row: Row)

case class HbaseOut(source: HbaseAction, result: Object)

class HbaseSinkMetaBean(override val underlying: Map[String, Any]) extends SinkMetaBean(underlying) {
  val FParallelism = "parallelism"

  def parallelism: Int = client.get[Int](FParallelism).getOrElse(1)
}

class HbaseSink(name: String = "hbase.sink",
                parallelism: Int,
                _create: () ⇒ Future[Connection],
                _close: Connection ⇒ Future[Done])
  extends CommonSinkAsyncUnordered[Connection, Message[HbaseRecord], Message[HbaseResult]](name, parallelism, m ⇒ m.map {
    m ⇒ HbaseOut(m, new Array[Object](0))
  }) {

  override val initialAttributes: Attributes = super.initialAttributes and TeleporterAttributes.BlockingDispatcher

  private val tables: mutable.Map[String, Table] = mutable.Map[String, Table]()

  override def create(executionContext: ExecutionContext): Future[Connection] = {
    _create()
  }

  override def write(client: Connection, elem: Message[HbaseRecord], executionContext: ExecutionContext): Future[Message[HbaseResult]] = {
    implicit val ec = executionContext
    val data = elem.data
    val table = tables.getOrElseUpdate(data.tableName, client.getTable(TableName.valueOf(data.tableName)))
    Future {
      val result = new Array[Object](1)
      table.batch(Collections.singletonList(data.row), result)
      elem.to(HbaseOut(data, result))
    }
  }

  override def close(client: Connection, executionContext: ExecutionContext): Future[Done] = {
    tables.values.foreach(_.close())
    _close(client)
  }
}

class HbaseBulkSink(name: String = "hbase.sink",
                    parallelism: Int,
                    _create: (ExecutionContext) ⇒ Future[Connection],
                    _close: (Connection, ExecutionContext) ⇒ Future[Done])
  extends CommonSinkAsyncUnordered[Connection, Seq[Message[HbaseRecord]], Seq[Message[HbaseResult]]](name, parallelism, s ⇒ s.map {
    m ⇒ m.map(e ⇒ HbaseOut(e, new Array[Object](0)))
  }) {

  override val initialAttributes: Attributes = super.initialAttributes and TeleporterAttributes.BlockingDispatcher
  private val tables: mutable.Map[String, Table] = mutable.Map[String, Table]()

  override def create(ec: ExecutionContext): Future[Connection] = _create(ec)

  override def write(client: Connection, elem: Seq[Message[HbaseRecord]], executionContext: ExecutionContext): Future[Seq[Message[HbaseResult]]] = {
    implicit val ec = executionContext
    val tableName = elem.head.data.tableName
    val table = tables.getOrElseUpdate(tableName, client.getTable(TableName.valueOf(tableName)))
    Future {
      val rows = elem.map(_.data.row)
      val results = new Array[Object](elem.size)
      table.batch(rows.asJava, results)
      elem.zip(results).map {
        case (m, r) ⇒ m.to[HbaseResult](HbaseOut(m.data, r))
      }
    }
  }

  override def close(client: Connection, executionContext: ExecutionContext): Future[Done] = {
    tables.values.foreach(_.close())
    _close(client, executionContext)
  }
}

class HbaseMetaBean(override val underlying: Map[String, Any]) extends AddressMetaBean(underlying) {
  val FHosts = "hosts"
  val FCoreSite = "core-site.xml"
  val FHdfsSite = "hdfs-site.xml"
  val FSslClient = "ssl-client.xml"
  val FHbaseSite = "hbase-site.xml"

  def hosts: Option[String] = client.get[String](FHosts)

  def coreSite: Option[String] = client.get[String](FCoreSite)

  def hdfsSite: Option[String] = client.get[String](FHdfsSite)

  def sslClient: Option[String] = client.get[String](FSslClient)

  def hbaseSite: Option[String] = client.get[String](FHbaseSite)
}