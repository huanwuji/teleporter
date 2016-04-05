package teleporter.task.tests

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.component.{KafkaRecord, TeleporterJdbcMessage, TeleporterKafkaRecord}
import teleporter.integration.conf.PropsSupport
import teleporter.integration.core.StreamManager.StreamInvoke
import teleporter.integration.core.{StreamManager, TeleporterCenter}

/**
 * date 2015/8/3.
 * @author daikui
 */
object MysqlToKafka extends App with LazyLogging {
  val decider: Supervision.Decider = {
    case e: Exception => logger.error(e.getLocalizedMessage, e); Supervision.Resume
  }
  implicit val system = ActorSystem()
  implicit val mater = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

  import system.dispatcher

  implicit val center = TeleporterCenter()
  val task = center.task("multi:db:sync")
  center.streamManager(task.id, StreamManager(task.id))

//  center.source[TeleporterJdbcMessage]("sh:154:mysql:etl:Wechat_User_Info").map {
//    msg ⇒
//            println(msg.data)
//      msg.data
//          val dbData = msg.data
//          val kafkaRecord = new KafkaRecord("mysql", dbData("id").toString.getBytes, dbData.toString().getBytes)
//          new TeleporterKafkaRecord(
//            id = msg.id,
//            data = kafkaRecord
//          )
//  }
////    .runForeach(println)
//      .to(center.sink("kafka_test_sink")).run()
}

class StreamImpl {
  val streamDef: StreamInvoke = (stream, center) ⇒ {
    import PropsSupport._
    import center.materializer
    val props = stream.props
    center.source[TeleporterJdbcMessage](getStringOpt(props,"source").get).runForeach(println)
  }
  streamDef
}