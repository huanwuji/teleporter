package teleporter.integration.cluster.rpc

import java.nio.ByteBuffer

import akka.util.ByteString
import com.google.flatbuffers.FlatBufferBuilder
import teleporter.integration.cluster.broker.PersistentProtocol.{AtomicKeyValue, KeyValue}
import teleporter.integration.cluster.rpc.fbs._
import teleporter.integration.cluster.rpc.fbs.generate.{EventStatus, EventType, Role}

/**
  * Created by huanwuji 
  * date 2017/1/3.
  */
case class TeleporterEvent[T](var seqNr: Long = 0, eventType: Byte, role: Byte = Role.None, status: Byte = EventStatus.None, body: T) {
  def toFlat: FlatBufferBuilder = {
    TeleporterEvent.toFlat(this)
  }

  def toArray: Array[Byte] = toFlat.sizedByteArray()

  def toBody[E]: E = this.body.asInstanceOf[E]
}

object TeleporterEvent {
  type EventHandle = PartialFunction[(Byte, TeleporterEvent[Any]), Unit]

  def toFlat(event: TeleporterEvent[_]): FlatBufferBuilder = {
    val builder = new FlatBufferBuilder()
    val body = event.eventType match {
      case EventType.KVGet ⇒ KVGet.toArray(event.toBody[KVGet])
      case EventType.RangeRegexKV ⇒ RangeRegexKV.toArray(event.toBody[RangeRegexKV])
      case EventType.KVSave ⇒ KV.toArray(event.toBody[KeyValue])
      case EventType.AtomicSaveKV ⇒ AtomicKV.toArray(event.toBody[AtomicKeyValue])
      case EventType.KVRemove ⇒ KVRemove.toArray(event.toBody[KVRemove])
      case EventType.LogRequest ⇒ LogRequest.toArray(event.toBody[LogRequest])
      case EventType.LogResponse ⇒ LogRequest.toArray(event.toBody[LogRequest])
      case EventType.LinkInstance ⇒ LinkInstance.toArray(event.toBody[LinkInstance])
      case EventType.LinkAddress ⇒ LinkAddress.toArray(event.toBody[LinkAddress])
      case EventType.LinkVariable ⇒ LinkVariable.toArray(event.toBody[LinkVariable])
      case EventType.TaskState ⇒ TaskState.toArray(event.toBody[TaskState])
      case EventType.BrokerState ⇒ BrokerState.toArray(event.toBody[BrokerState])
      case EventType.InstanceState ⇒ InstanceState.toArray(event.toBody[InstanceState])
      case EventType.ConfigChangeNotify ⇒ ConfigChangeNotify.toArray(event.toBody[ConfigChangeNotify])
    }
    val bodyOffset = generate.TeleporterEvent.createBodyVector(builder, body)
    val root = generate.TeleporterEvent.createTeleporterEvent(
      builder, event.seqNr, event.eventType, event.role, event.status, bodyOffset
    )
    builder.finish(root)
    builder
  }

  def apply[T](bs: ByteString): TeleporterEvent[Any] = {
    val event = generate.TeleporterEvent.getRootAsTeleporterEvent(bs.asByteBuffer)
    val body = event.eventType() match {
      case EventType.KVGet ⇒ KVGet(Array.tabulate(event.bodyLength())(event.body))
      case EventType.RangeRegexKV ⇒ RangeRegexKV(Array.tabulate(event.bodyLength())(event.body))
      case EventType.KVSave ⇒ KV(Array.tabulate(event.bodyLength())(event.body))
      case EventType.AtomicSaveKV ⇒ AtomicKV(Array.tabulate(event.bodyLength())(event.body))
      case EventType.KVRemove ⇒ KVRemove(Array.tabulate(event.bodyLength())(event.body))
      case EventType.LogRequest ⇒ LogRequest(Array.tabulate(event.bodyLength())(event.body))
      case EventType.LogResponse ⇒ LogRequest(Array.tabulate(event.bodyLength())(event.body))
      case EventType.LinkInstance ⇒ LinkInstance(Array.tabulate(event.bodyLength())(event.body))
      case EventType.LinkAddress ⇒ LinkAddress(Array.tabulate(event.bodyLength())(event.body))
      case EventType.LinkVariable ⇒ LinkVariable(Array.tabulate(event.bodyLength())(event.body))
      case EventType.TaskState ⇒ TaskState(Array.tabulate(event.bodyLength())(event.body))
      case EventType.BrokerState ⇒ BrokerState(Array.tabulate(event.bodyLength())(event.body))
      case EventType.InstanceState ⇒ InstanceState(Array.tabulate(event.bodyLength())(event.body))
      case EventType.ConfigChangeNotify ⇒ ConfigChangeNotify(Array.tabulate(event.bodyLength())(event.body))
    }
    TeleporterEvent(
      seqNr = event.seqNr(),
      eventType = event.eventType(),
      role = event.role(),
      status = event.status(),
      body = body
    )
  }

  def success[T](event: TeleporterEvent[T]): TeleporterEvent[T] = {
    event.copy(status = EventStatus.Success)
  }

  def failure[T](event: TeleporterEvent[T]): TeleporterEvent[T] = {
    event.copy(status = EventStatus.Failure)
  }
}

object KV {
  def apply(t: generate.KV): KeyValue = {
    KeyValue(key = t.key(), value = t.value())
  }

  def apply(bytes: Array[Byte]): KeyValue = {
    val t = generate.KV.getRootAsKV(ByteBuffer.wrap(bytes))
    apply(t)
  }

  def toOffset(body: KeyValue, builder: FlatBufferBuilder): Int = {
    generate.KV.createKV(builder, builder.createString(body.key), builder.createString(body.value))
  }

  def toArray(body: KeyValue): Array[Byte] = {
    val builder = new FlatBufferBuilder()
    val root = toOffset(body, builder)
    builder.finish(root)
    builder.sizedByteArray()
  }
}

object AtomicKV {
  def apply(bytes: Array[Byte]): AtomicKeyValue = {
    val t = generate.AtomicKV.getRootAsAtomicKV(ByteBuffer.wrap(bytes))
    AtomicKeyValue(key = t.key(), expect = t.expect(), update = t.update())
  }

  def toArray(body: AtomicKeyValue): Array[Byte] = {
    val builder = new FlatBufferBuilder()
    val root = generate.AtomicKV.createAtomicKV(builder,
      builder.createString(body.key),
      builder.createString(body.expect),
      builder.createString(body.update)
    )
    builder.finish(root)
    builder.sizedByteArray()
  }
}

case class KVS(kvs: Array[KeyValue])

object KVS {
  def apply(bytes: Array[Byte]): KVS = {
    val t = generate.KVS.getRootAsKVS(ByteBuffer.wrap(bytes))
    KVS(Array.tabulate(t.kvsLength())(i ⇒ KV(t.kvs(i))))
  }

  def toArray(body: KVS): Array[Byte] = {
    val builder = new FlatBufferBuilder()
    val kvs = generate.KVS.createKvsVector(builder, body.kvs.map(KV.toOffset(_, builder)))
    val root = generate.KVS.createKVS(builder, kvs)
    builder.finish(root)
    builder.sizedByteArray()
  }
}