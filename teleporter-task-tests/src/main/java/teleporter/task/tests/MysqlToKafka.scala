package teleporter.task.tests

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.component.TeleporterJdbcMessage
import teleporter.integration.core.TeleporterCenter

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

  val center = TeleporterCenter()
  center.source[TeleporterJdbcMessage]("sh:154:mysql:etl:Wechat_User_Info").map {
    msg ⇒
      //      println(msg.data)
      msg.data
    //      val dbData = msg.data
    //      val kafkaRecord = new KafkaRecord("mysql", dbData("id").toString.getBytes, dbData.toString().getBytes)
    //      new TeleporterKafkaRecord(
    //        id = msg.id,
    //        data = kafkaRecord
    //      )
  }
//    .runForeach(println)
      .to(center.sink("kafka_test_sink")).run()
}