package akka.stream.scaladsl

import java.io.File

import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.impl.io.TeleporterFileSink
import akka.stream.{Inlet, SinkShape}
import akka.util.ByteString

import scala.concurrent.Future

/**
 * Author: kui.dai
 * Date: 2015/12/1.
 */
object TeleporterSink {
  private[stream] def shape[T](name: String): SinkShape[T] = SinkShape(Inlet(name + ".in"))

  def file(f: File, append: Boolean = false, actorNameOpt: Option[String] = None): Sink[ByteString, Future[Long]] =
    new Sink(new TeleporterFileSink(f, append, DefaultAttributes.fileSink, shape("FileSink"), actorNameOpt))
}