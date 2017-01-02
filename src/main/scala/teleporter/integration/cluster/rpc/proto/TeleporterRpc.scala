package teleporter.integration.cluster.rpc.proto

import teleporter.integration.cluster.rpc.proto.Rpc.{EventStatus, EventType, TeleporterEvent}

/**
  * Created by kui.dai on 2016/7/29.
  */
object TeleporterRpc {
  type EventHandle = PartialFunction[(EventType, TeleporterEvent), Unit]

  def success(event: TeleporterEvent): TeleporterEvent = {
    TeleporterEvent.newBuilder(event).setStatus(EventStatus.Success).build()
  }

  def failure(event: TeleporterEvent): TeleporterEvent = {
    TeleporterEvent.newBuilder(event).setStatus(EventStatus.Failure).build()
  }
}