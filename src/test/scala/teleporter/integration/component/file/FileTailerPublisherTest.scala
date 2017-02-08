package teleporter.integration.component.file

import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.LockSupport

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Framing, Sink, Source}
import akka.util.ByteString
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.appender.RollingFileAppender
import org.apache.logging.log4j.scala.Logging
import org.scalatest.FunSuite

import scala.collection.JavaConverters._
import scala.concurrent.duration._

/**
  * @author kui.dai Created 2016/9/9
  */
class FileTailerPublisherTest extends FunSuite with Logging {
  implicit val system = ActorSystem()
  implicit val mater = ActorMaterializer()
  test("tail publish") {
    val loggerContext = LogManager.getContext(true).asInstanceOf[LoggerContext]
    val path = loggerContext.getConfiguration.getAppenders.asScala.collect {
      case (_, v: RollingFileAppender) ⇒ Paths.get(v.getFileName)
    }.head
    println(path.toFile.getCanonicalPath)
    FileTailer.source(path)
      .via(Framing.delimiter(ByteString(System.lineSeparator()), 1024 * 5))
      .runForeach(bs ⇒ println(bs.utf8String))
    val counter = new AtomicLong()
    Source.tick(2.seconds, 2.seconds, "ttttet").map(x ⇒ logger.info(counter.incrementAndGet().toString)).to(Sink.ignore).run()
    logger.info("====================")
    LockSupport.park()
  }
}