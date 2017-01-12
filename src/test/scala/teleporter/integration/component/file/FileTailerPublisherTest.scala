package teleporter.integration.component.file

import java.nio.file.Paths
import java.util.concurrent.locks.LockSupport

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Framing
import akka.util.ByteString
import com.google.common.io.Resources
import org.scalatest.FunSuite

/**
  * @author kui.dai Created 2016/9/9
  */
class FileTailerPublisherTest extends FunSuite {
  implicit val system = ActorSystem()
  implicit val mater = ActorMaterializer()
  test("tail publish") {
    val path = Paths.get(Resources.getResource(getClass, "/").toURI).getParent.getParent.getParent.resolve("logs/server.log")
    FileTailer.source(path).via(Framing.delimiter(ByteString.fromString("\n"), 1024 * 5)).runForeach(bs ⇒ println(bs.utf8String))
    LockSupport.park()
  }
}