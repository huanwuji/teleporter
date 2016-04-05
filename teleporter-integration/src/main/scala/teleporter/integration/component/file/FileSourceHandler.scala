package teleporter.integration.component.file

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.Source
import teleporter.integration.CmpType
import teleporter.integration.component._
import teleporter.integration.core.TeleporterCenter

/**
 * Created by yadong.li on 2016/1/19.
 */
object FileSourceHandler {

  def fromFile(id: Int, center: TeleporterCenter)(implicit system: ActorSystem, materlizer: ActorMaterializer): Source[TeleporterFileRecord, NotUsed] = {

    val actRef = system.actorOf(FilePublisher.props(id))
    center.actorAddresses.idMapper += ((id, CmpType.Source) → actRef)
    val source: Source[TeleporterFileRecord, NotUsed] = Source.fromPublisher(ActorPublisher(actRef))
    source
  }
}