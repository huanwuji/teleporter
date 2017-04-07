package teleporter.integration.component.log

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.appender.RollingFileAppender
import teleporter.integration.component.file.FileCmd

import scala.collection.JavaConverters._

/**
  * @author kui.dai Created 2016/9/1
  */
trait Logs {
  private val logFileAppender: RollingFileAppender = {
    val loggerContext = LogManager.getContext(true).asInstanceOf[LoggerContext]
    loggerContext.getConfiguration.getAppenders.asScala.collect {
      case (_, v: RollingFileAppender) ⇒ v
    }.head
  }

  def tailLog()(implicit mater: Materializer): Source[ByteString, NotUsed] = {
    FileCmd.exec(s"tail ${logFileAppender.getFileName}")
  }
}

object Logs extends Logs