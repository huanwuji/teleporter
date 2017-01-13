package teleporter.integration.cluster.rpc

import akka.util.ByteString
import com.google.flatbuffers.FlatBufferBuilder
import teleporter.integration.cluster.rpc.EventBody.Empty
import teleporter.integration.cluster.rpc.fbs._

/**
  * Created by huanwuji 
  * date 2017/1/3.
  */
case class TeleporterEvent[T <: EventBody](var seqNr: Long = 0, eventType: Byte, status: Byte = EventStatus.None, role: Byte, body: T) {
  def toBody[E <: EventBody]: E = this.body.asInstanceOf[E]
}

object TeleporterEvent {
  type EventHandle = PartialFunction[(Byte, TeleporterEvent[_ <: EventBody]), Unit]

  trait Action[I <: EventBody, O <: EventBody] {
    def event[T <: EventBody](event: TeleporterEvent[T]): Array[Byte] = {
      event.role match {
        case Role.Request ⇒ request(event.asInstanceOf[TeleporterEvent[I]])
        case Role.Response ⇒ response(event.asInstanceOf[TeleporterEvent[O]])
      }
    }

    def event[T <: EventBody](event: fbs.RpcEvent): TeleporterEvent[T] = {
      event.role() match {
        case Role.Request ⇒ request(event).asInstanceOf[TeleporterEvent[T]]
        case Role.Response ⇒ response(event).asInstanceOf[TeleporterEvent[T]]
      }
    }

    def request(event: TeleporterEvent[I]): Array[Byte] = {
      val builder = new FlatBufferBuilder()
      val bodyOffset = fbs.RpcEvent.createBodyVector(builder, requestBody(event.body))
      val root = fbs.RpcEvent.createRpcEvent(
        builder, event.seqNr, event.eventType, event.role, event.status, bodyOffset
      )
      builder.finish(root)
      builder.sizedByteArray()
    }

    protected def requestBody(body: I): Array[Byte]

    def request(event: fbs.RpcEvent): TeleporterEvent[I] = {
      TeleporterEvent(
        seqNr = event.seqNr(),
        eventType = event.eventType(),
        status = event.status(),
        role = Role.Request,
        body = requestBody(Array.tabulate(event.bodyLength())(event.body))
      )
    }

    protected def requestBody(bytes: Array[Byte]): I

    def response(event: TeleporterEvent[O]): Array[Byte] = {
      val builder = new FlatBufferBuilder()
      val bodyOffset = fbs.RpcEvent.createBodyVector(builder, responseBody(event.body))
      val root = fbs.RpcEvent.createRpcEvent(
        builder, event.seqNr, event.eventType, event.role, event.status, bodyOffset
      )
      builder.finish(root)
      builder.sizedByteArray()
    }

    protected def responseBody(body: O): Array[Byte]

    def response(event: fbs.RpcEvent): TeleporterEvent[O] = {
      TeleporterEvent(
        seqNr = event.seqNr(),
        eventType = event.eventType(),
        status = event.status(),
        role = Role.Response,
        body = responseBody(Array.tabulate(event.bodyLength())(event.body))
      )
    }

    protected def responseBody(bytes: Array[Byte]): O
  }

  object KVGetAction extends Action[EventBody.KVGet, EventBody.KV] {
    override protected def requestBody(body: EventBody.KVGet): Array[Byte] = EventBody.KVGet.toArray(body)

    override protected def requestBody(bytes: Array[Byte]): EventBody.KVGet = EventBody.KVGet(bytes)

    override protected def responseBody(body: EventBody.KV): Array[Byte] = EventBody.KV.toArray(body)

    override protected def responseBody(bytes: Array[Byte]): EventBody.KV = EventBody.KV(bytes)
  }

  object RangeRegexAction extends Action[EventBody.RangeRegexKV, EventBody.KVS] {
    override protected def requestBody(body: EventBody.RangeRegexKV): Array[Byte] = EventBody.RangeRegexKV.toArray(body)

    override protected def requestBody(bytes: Array[Byte]): EventBody.RangeRegexKV = EventBody.RangeRegexKV(bytes)

    override protected def responseBody(body: EventBody.KVS): Array[Byte] = EventBody.KVS.toArray(body)

    override protected def responseBody(bytes: Array[Byte]): EventBody.KVS = EventBody.KVS(bytes)
  }

  object KVSaveAction extends Action[EventBody.KV, EventBody.Empty] {
    override protected def requestBody(body: EventBody.KV): Array[Byte] = EventBody.KV.toArray(body)

    override protected def requestBody(bytes: Array[Byte]): EventBody.KV = EventBody.KV(bytes)

    override protected def responseBody(body: EventBody.Empty): Array[Byte] = EventBody.Empty.toArray

    override protected def responseBody(bytes: Array[Byte]): EventBody.Empty = EventBody.Empty.empty
  }

  object AtomicSaveAction extends Action[EventBody.AtomicKV, EventBody.Empty] {
    override protected def requestBody(body: EventBody.AtomicKV): Array[Byte] = EventBody.AtomicKV.toArray(body)

    override protected def requestBody(bytes: Array[Byte]): EventBody.AtomicKV = EventBody.AtomicKV(bytes)

    override protected def responseBody(body: EventBody.Empty): Array[Byte] = EventBody.Empty.toArray

    override protected def responseBody(bytes: Array[Byte]): EventBody.Empty = EventBody.Empty.empty
  }

  object KVRemoveAction extends Action[EventBody.KVRemove, EventBody.Empty] {
    override protected def requestBody(body: EventBody.KVRemove): Array[Byte] = EventBody.KVRemove.toArray(body)

    override protected def requestBody(bytes: Array[Byte]): EventBody.KVRemove = EventBody.KVRemove(bytes)

    override protected def responseBody(body: EventBody.Empty): Array[Byte] = EventBody.Empty.toArray

    override protected def responseBody(bytes: Array[Byte]): EventBody.Empty = EventBody.Empty.empty
  }

  object LogTraceAction extends Action[EventBody.LogTailRequest, EventBody.LogTailResponse] {
    override protected def requestBody(body: EventBody.LogTailRequest): Array[Byte] = EventBody.LogTailRequest.toArray(body)

    override protected def requestBody(bytes: Array[Byte]): EventBody.LogTailRequest = EventBody.LogTailRequest(bytes)

    override protected def responseBody(body: EventBody.LogTailResponse): Array[Byte] = EventBody.LogTailResponse.toArray(body)

    override protected def responseBody(bytes: Array[Byte]): EventBody.LogTailResponse = EventBody.LogTailResponse(bytes)
  }

  object LinkInstanceAction extends Action[EventBody.LinkInstance, EventBody.Empty] {
    override protected def requestBody(body: EventBody.LinkInstance): Array[Byte] = EventBody.LinkInstance.toArray(body)

    override protected def requestBody(bytes: Array[Byte]): EventBody.LinkInstance = EventBody.LinkInstance(bytes)

    override protected def responseBody(body: EventBody.Empty): Array[Byte] = EventBody.Empty.toArray

    override protected def responseBody(bytes: Array[Byte]): EventBody.Empty = EventBody.Empty.empty
  }

  object LinkAddressAction extends Action[EventBody.LinkAddress, EventBody.Empty] {
    override protected def requestBody(body: EventBody.LinkAddress): Array[Byte] = EventBody.LinkAddress.toArray(body)

    override protected def requestBody(bytes: Array[Byte]): EventBody.LinkAddress = EventBody.LinkAddress(bytes)

    override protected def responseBody(body: EventBody.Empty): Array[Byte] = EventBody.Empty.toArray

    override protected def responseBody(bytes: Array[Byte]): EventBody.Empty = EventBody.Empty.empty
  }

  object LinkVariableAction extends Action[EventBody.LinkVariable, EventBody.Empty] {
    override protected def requestBody(body: EventBody.LinkVariable): Array[Byte] = EventBody.LinkVariable.toArray(body)

    override protected def requestBody(bytes: Array[Byte]): EventBody.LinkVariable = EventBody.LinkVariable(bytes)

    override protected def responseBody(body: EventBody.Empty): Array[Byte] = EventBody.Empty.toArray

    override protected def responseBody(bytes: Array[Byte]): EventBody.Empty = EventBody.Empty.empty
  }

  object ConfigChangeNotifyAction extends Action[EventBody.ConfigChangeNotify, EventBody.Empty] {
    override protected def requestBody(body: EventBody.ConfigChangeNotify): Array[Byte] = EventBody.ConfigChangeNotify.toArray(body)

    override protected def requestBody(bytes: Array[Byte]): EventBody.ConfigChangeNotify = EventBody.ConfigChangeNotify(bytes)

    override protected def responseBody(body: EventBody.Empty): Array[Byte] = EventBody.Empty.toArray

    override protected def responseBody(bytes: Array[Byte]): EventBody.Empty = EventBody.Empty.empty
  }

  def toArray(event: TeleporterEvent[_ <: EventBody]): Array[Byte] = {
    event.eventType match {
      case EventType.KVGet ⇒ KVGetAction.event(event)
      case EventType.RangeRegexKV ⇒ RangeRegexAction.event(event)
      case EventType.KVSave ⇒ KVSaveAction.event(event)
      case EventType.AtomicSaveKV ⇒ AtomicSaveAction.event(event)
      case EventType.KVRemove ⇒ KVRemoveAction.event(event)
      case EventType.LogTail ⇒ LogTraceAction.event(event)
      case EventType.LinkInstance ⇒ LinkInstanceAction.event(event)
      case EventType.LinkAddress ⇒ LinkAddressAction.event(event)
      case EventType.LinkVariable ⇒ LinkVariableAction.event(event)
      case EventType.ConfigChangeNotify ⇒ ConfigChangeNotifyAction.event(event)
    }
  }

  def apply[T <: EventBody](bs: ByteString): TeleporterEvent[T] = {
    val event = fbs.RpcEvent.getRootAsRpcEvent(bs.asByteBuffer)
    event.eventType() match {
      case EventType.KVGet ⇒ KVGetAction.event(event)
      case EventType.RangeRegexKV ⇒ RangeRegexAction.event(event)
      case EventType.KVSave ⇒ KVSaveAction.event(event)
      case EventType.AtomicSaveKV ⇒ AtomicSaveAction.event(event)
      case EventType.KVRemove ⇒ KVRemoveAction.event(event)
      case EventType.LogTail ⇒ LogTraceAction.event(event)
      case EventType.LinkInstance ⇒ LinkInstanceAction.event(event)
      case EventType.LinkAddress ⇒ LinkAddressAction.event(event)
      case EventType.LinkVariable ⇒ LinkVariableAction.event(event)
      case EventType.ConfigChangeNotify ⇒ ConfigChangeNotifyAction.event(event)
    }
  }

  def success[T <: EventBody](event: TeleporterEvent[T]): TeleporterEvent[Empty] = {
    event.copy(role = Role.Response, status = EventStatus.Success, body = EventBody.Empty.empty)
  }

  def failure[T <: EventBody](event: TeleporterEvent[T]): TeleporterEvent[Empty] = {
    event.copy(role = Role.Response, status = EventStatus.Failure, body = EventBody.Empty.empty)
  }
}