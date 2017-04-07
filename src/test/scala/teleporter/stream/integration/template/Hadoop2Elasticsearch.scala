//package teleporter.stream.integration.transaction

import akka.Done
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{KillSwitch, KillSwitches}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.index.VersionType
import teleporter.integration.component.elasticsearch.ElasticSearch2
import teleporter.integration.component.hdfs.Hdfs
import teleporter.integration.core.Streams._
import teleporter.integration.core.{SourceAck, StreamContext, TeleporterCenter}
import teleporter.integration.utils.Converters._

import scala.concurrent.Future

/**
  * Created by huanwuji on 2016/10/20.
  * arguments: fields, index, type
  */
object Hadoop2Elasticsearch extends StreamLogic {
  override def apply(key: String, center: TeleporterCenter): (KillSwitch, Future[Done]) = {
    import center.{materializer, self}
    val context = center.context.getContext[StreamContext](key)
    val arguments = context.config.arguments
    val fields = arguments[String]("fields").split(",")
    val index = arguments[String]("index")
    val indexType = arguments[String]("type")
    Hdfs.sourceAck("/source/test/hadoop_es_test/hadoop2es/hdfs_source")
      .map { m ⇒
        m.map { bs ⇒
          val data = fields.zip(bs.utf8String.split(",")).toMap
          new IndexRequest()
            .index(index)
            .`type`(indexType)
            .id(data("id"))
            .source(data)
            .versionType(VersionType.EXTERNAL)
            .version(System.currentTimeMillis())
        }
      }
      .via(ElasticSearch2.flow("/sink/test/hadoop_es_test/hadoop2es/item_es"))
      .via(SourceAck.confirmFlow())
      .viaMat(KillSwitches.single)(Keep.right).watchTermination()(Keep.both)
      .to(Sink.ignore).run()
  }
}