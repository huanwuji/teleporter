package teleporter.integration.component.file

import java.nio.file.Paths
import java.nio.file.StandardOpenOption._
import java.util.concurrent.locks.LockSupport

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.util.ByteString
import com.google.common.io.Resources
import com.typesafe.config.ConfigFactory
import org.apache.logging.log4j.scala.Logging
import org.scalatest.FunSuite
import teleporter.integration.component.SinkRoller.SinkRollerSetting
import teleporter.integration.component.{Cron, SinkRoller}
import teleporter.integration.core.Message

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by huanwuji 
  * date 2017/2/20.
  */
class FileStreams$Test extends FunSuite with Logging {
  implicit val system = ActorSystem("instance", ConfigFactory.load("instance.conf"))
  val decider: Supervision.Decider = {
    case e: Exception ⇒ logger.warn(e.getMessage, e); Supervision.Stop
  }
  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))
  test("file reader") {
    val fu = Source.fromGraph(new FileSource(path = Paths.get(Resources.getResource("./broker.conf").toURI), offset = 100, bufferSize = 1024))
      .runForeach(bs ⇒ println(bs.utf8String))
    Await.result(fu, 100.seconds)
    println("end")
  }
  test("file write") {
    var inc = 0
    val fu = Source.tick(0.seconds, 1.seconds, "fjdkfj").map { i ⇒ inc += 1; Message(FileByteString(byteString = ByteString(inc + "\r\n"), path = Some(s"/tmp/file/test_{0,date,yyyy-MM-dd_HH_mm_ss}_{1}.txt"))) }
      .via(SinkRoller.flow[Message[FileByteString], Message[FileByteString]](
        setting = SinkRollerSetting(cron = Some("* * * * *"), size = Some(100)),
        cronRef = Cron.cronRef("teleporter_crontab"),
        catchSize = _.data.byteString.length,
        catchField = _.data.path.get,
        rollKeep = (in, path) ⇒ in.map(data ⇒ data.copy(path = Some(path))),
        rollDo = path ⇒ Message(FileByteString(ByteString.empty, Some(path), total = 0))
      )).via(Flow.fromGraph(new FileSink(path = Some("/tmp/file/test.txt"), openOptions = Set(CREATE, WRITE, APPEND))))
      .toMat(Sink.ignore)(Keep.right).run()
    Await.result(fu, 10.minutes)
    println("end")
  }
  test("test") {
    Source.single("test").map(_ ⇒ throw new RuntimeException()).runForeach(println)
    LockSupport.park()
  }
}
