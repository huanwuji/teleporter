package teleporter.integration.cluster.rpc

import akka.util.ByteString
import com.google.flatbuffers.FlatBufferBuilder
import teleporter.integration.cluster.rpc.fbs._

/**
  * Created by huanwuji 
  * date 2017/1/3.
  */
case class TeleporterEvent[T <: EventBody](var seqNr: Long = 0, eventType: Byte, status: Byte = EventStatus.None, message: String = null, role: Byte, body: Option[T]) {
  def toBody[E <: EventBody]: E = {
    require(body.isDefined, s"${this} body must defined!")
    this.body.get.asInstanceOf[E]
  }
}

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
    val bodyOffset = event.body.map(b ⇒ fbs.RpcEvent.createBodyVector(builder, requestBody(b))).getOrElse(0)
    val messageOffset = if (event.message == null) 0 else builder.createString(event.message)
    val root = fbs.RpcEvent.createRpcEvent(
      builder, event.seqNr, event.eventType, event.role, event.status, messageOffset, bodyOffset
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
      message = event.message(),
      role = Role.Request,
      body = if (event.bodyLength() == 0) None else Some(requestBody(Array.tabulate(event.bodyLength())(event.body)))
    )
  }

  protected def requestBody(bytes: Array[Byte]): I

  def response(event: TeleporterEvent[O]): Array[Byte] = {
    val builder = new FlatBufferBuilder()
    val bodyOffset = event.body.map(b ⇒ fbs.RpcEvent.createBodyVector(builder, responseBody(b))).getOrElse(0)
    val messageOffset = if (event.message == null) 0 else builder.createString(event.message)
    val root = fbs.RpcEvent.createRpcEvent(
      builder, event.seqNr, event.eventType, event.role, event.status, messageOffset, bodyOffset
    )
    builder.finish(root)
    builder.sizedByteArray()
  }

  protected def responseBody(body: O): Array[Byte]

  def response(event: fbs.RpcEvent): TeleporterEvent[O] = {
    val body = if (event.bodyLength() == 0) None else Some(responseBody(Array.tabulate(event.bodyLength())(event.body)))
    TeleporterEvent(
      seqNr = event.seqNr(),
      eventType = event.eventType(),
      status = event.status(),
      message = event.message(),
      role = Role.Response,
      body = body
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

  override protected def responseBody(body: EventBody.Empty): Array[Byte] = EventBody.Empty.toArray(body)

  override protected def responseBody(bytes: Array[Byte]): EventBody.Empty = EventBody.Empty(bytes)
}

object AtomicSaveAction extends Action[EventBody.AtomicKV, EventBody.KV] {
  override protected def requestBody(body: EventBody.AtomicKV): Array[Byte] = EventBody.AtomicKV.toArray(body)

  override protected def requestBody(bytes: Array[Byte]): EventBody.AtomicKV = EventBody.AtomicKV(bytes)

  override protected def responseBody(body: EventBody.KV): Array[Byte] = EventBody.KV.toArray(body)

  override protected def responseBody(bytes: Array[Byte]): EventBody.KV = EventBody.KV(bytes)
}

object KVRemoveAction extends Action[EventBody.KVRemove, EventBody.Empty] {
  override protected def requestBody(body: EventBody.KVRemove): Array[Byte] = EventBody.KVRemove.toArray(body)

  override protected def requestBody(bytes: Array[Byte]): EventBody.KVRemove = EventBody.KVRemove(bytes)

  override protected def responseBody(body: EventBody.Empty): Array[Byte] = EventBody.Empty.toArray(body)

  override protected def responseBody(bytes: Array[Byte]): EventBody.Empty = EventBody.Empty(bytes)
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

  override protected def responseBody(body: EventBody.Empty): Array[Byte] = EventBody.Empty.toArray(body)

  override protected def responseBody(bytes: Array[Byte]): EventBody.Empty = EventBody.Empty(bytes)
}

object LinkAddressAction extends Action[EventBody.LinkAddress, EventBody.Empty] {
  override protected def requestBody(body: EventBody.LinkAddress): Array[Byte] = EventBody.LinkAddress.toArray(body)

  override protected def requestBody(bytes: Array[Byte]): EventBody.LinkAddress = EventBody.LinkAddress(bytes)

  override protected def responseBody(body: EventBody.Empty): Array[Byte] = EventBody.Empty.toArray(body)

  override protected def responseBody(bytes: Array[Byte]): EventBody.Empty = EventBody.Empty(bytes)
}

object LinkVariableAction extends Action[EventBody.LinkVariable, EventBody.Empty] {
  override protected def requestBody(body: EventBody.LinkVariable): Array[Byte] = EventBody.LinkVariable.toArray(body)

  override protected def requestBody(bytes: Array[Byte]): EventBody.LinkVariable = EventBody.LinkVariable(bytes)

  override protected def responseBody(body: EventBody.Empty): Array[Byte] = EventBody.Empty.toArray(body)

  override protected def responseBody(bytes: Array[Byte]): EventBody.Empty = EventBody.Empty(bytes)
}

object ConfigChangeNotifyAction extends Action[EventBody.ConfigChangeNotify, EventBody.Empty] {
  override protected def requestBody(body: EventBody.ConfigChangeNotify): Array[Byte] = EventBody.ConfigChangeNotify.toArray(body)

  override protected def requestBody(bytes: Array[Byte]): EventBody.ConfigChangeNotify = EventBody.ConfigChangeNotify(bytes)

  override protected def responseBody(body: EventBody.Empty): Array[Byte] = EventBody.Empty.toArray(body)

  override protected def responseBody(bytes: Array[Byte]): EventBody.Empty = EventBody.Empty(bytes)
}

object TeleporterEvent {
  type EventHandle = PartialFunction[(Byte, TeleporterEvent[_ <: EventBody]), Unit]

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
    val rpcEvent = fbs.RpcEvent.getRootAsRpcEvent(bs.asByteBuffer)
    rpcEvent.eventType() match {
      case EventType.KVGet ⇒ KVGetAction.event(rpcEvent)
      case EventType.RangeRegexKV ⇒ RangeRegexAction.event(rpcEvent)
      case EventType.KVSave ⇒ KVSaveAction.event(rpcEvent)
      case EventType.AtomicSaveKV ⇒ AtomicSaveAction.event(rpcEvent)
      case EventType.KVRemove ⇒ KVRemoveAction.event(rpcEvent)
      case EventType.LogTail ⇒ LogTraceAction.event(rpcEvent)
      case EventType.LinkInstance ⇒ LinkInstanceAction.event(rpcEvent)
      case EventType.LinkAddress ⇒ LinkAddressAction.event(rpcEvent)
      case EventType.LinkVariable ⇒ LinkVariableAction.event(rpcEvent)
      case EventType.ConfigChangeNotify ⇒ ConfigChangeNotifyAction.event(rpcEvent)
    }
  }

  def request[T <: EventBody](seqNr: Long = 0, eventType: Byte, body: T): TeleporterEvent[T] = {
    TeleporterEvent(seqNr = seqNr, eventType = eventType, status = EventStatus.None, role = Role.Request, body = Some(body))
  }

  def response[T <: EventBody](event: TeleporterEvent[_], body: T): TeleporterEvent[T] = {
    response(seqNr = event.seqNr, eventType = event.eventType, body = body)
  }


  def response[T <: EventBody](seqNr: Long = 0, eventType: Byte, message: String = null, body: T): TeleporterEvent[T] = {
    TeleporterEvent(seqNr = seqNr, eventType = eventType, status = EventStatus.Success, message = message, role = Role.Response, body = Some(body))
  }

  def success[T <: EventBody](event: TeleporterEvent[T]): TeleporterEvent[T] = {
    TeleporterEvent(seqNr = event.seqNr, eventType = event.eventType, status = EventStatus.Success, role = Role.Response, body = None)
  }

  def failure[T <: EventBody](event: TeleporterEvent[T], message: String): TeleporterEvent[T] = {
    TeleporterEvent(seqNr = event.seqNr, eventType = event.eventType, status = EventStatus.Failure, message = message, role = Role.Response, body = None)
  }
}