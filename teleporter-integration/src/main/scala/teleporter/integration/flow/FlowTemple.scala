package teleporter.integration.flow

import akka.stream.scaladsl.Flow
import org.mongodb.scala.MongoDatabase
import teleporter.integration.component._
import teleporter.integration.core.TeleporterCenter

/**
 * Created by joker  on 15/12/14
 */
trait FlowTemple {

  val kafkaFlow = Flow[KafkaMessage].map(
    x ⇒ {
      import teleporter.integration.script.MsgParser._
      x.headers
      x.payload
    }
  )
}

//object FlowFactory {
//  def build(sid: Int): Flow = {
//    val flow: Flow =null
//    val center = TeleporterCenter()
//    val database = center.addressing[MongoDatabase](sid).get
//    flow
//  }
//
//}


case class FlowResult()
