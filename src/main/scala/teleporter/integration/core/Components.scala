package teleporter.integration.core

import akka.actor.ActorRef
import akka.stream.scaladsl.{Sink, Source}

/**
  * Author: kui.dai
  * Date: 2016/7/4.
  */
trait Component {
  val key: String

  def id(): Long = center.context.getContext[ComponentContext](key).id

  implicit val center: TeleporterCenter
}

object Component {
  def getIfPresent[T](iterator: Iterator[T]): Option[T] = if (iterator.hasNext) Some(iterator.next()) else None
}

trait Components {
  implicit val center: TeleporterCenter
  val teleporterSource: TeleporterSource
  val teleporterSink: TeleporterSink
  val teleporterAddress: TeleporterAddress

  def source[T](id: Long): Source[T, ActorRef] = source[T](center.context.getContext[SourceContext](id).key)

  def source[T](key: String): Source[T, ActorRef] = teleporterSource[T](key)

  def sink[T](id: Long): Sink[T, ActorRef] = sink(center.context.getContext[SinkContext](id).key)

  def sink[T](key: String): Sink[T, ActorRef] = teleporterSink[T](key)

  def address[T](id: Long): T = address(center.context.getContext[AddressContext](id).key)

  def address[T](key: String): T = teleporterAddress(key)
}

class ComponentsImpl(
                      val teleporterSource: TeleporterSource,
                      val teleporterSink: TeleporterSink,
                      val teleporterAddress: TeleporterAddress
                    )(implicit val center: TeleporterCenter) extends Components

object Components {
  def apply()(implicit center: TeleporterCenter): Components = {
    new ComponentsImpl(TeleporterSource(), TeleporterSink(), TeleporterAddress())
  }
}