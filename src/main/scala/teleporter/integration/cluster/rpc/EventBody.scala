package teleporter.integration.cluster.rpc

import java.nio.ByteBuffer

import com.google.flatbuffers.FlatBufferBuilder
import teleporter.integration.cluster.broker.PersistentProtocol.KeyValue

/**
  * Created by huanwuji 
  * date 2017/1/12.
  */
trait EventBody {
  def toArray: Array[Byte]
}

object EventBody {

  case class ConfigChangeNotify(key: String, action: Byte, timestamp: Long) extends EventBody {
    override def toArray: Array[Byte] = ConfigChangeNotify.toArray(this)
  }

  object ConfigChangeNotify {
    def apply(bytes: Array[Byte]): ConfigChangeNotify = {
      val t = fbs.ConfigChangeNotify.getRootAsConfigChangeNotify(ByteBuffer.wrap(bytes))
      ConfigChangeNotify(
        key = t.key(),
        action = t.action(),
        timestamp = t.timestamp()
      )
    }

    def toArray(body: ConfigChangeNotify): Array[Byte] = {
      val builder = new FlatBufferBuilder()
      val root = fbs.ConfigChangeNotify.createConfigChangeNotify(
        builder, builder.createString(body.key), body.action, body.timestamp
      )
      builder.finish(root)
      builder.sizedByteArray()
    }
  }

  case class HealthResponse(totalMemory: Float, freeMemory: Float) extends EventBody {
    override def toArray: Array[Byte] = HealthResponse.toArray(this)
  }

  object HealthResponse {
    def apply(bytes: Array[Byte]): HealthResponse = {
      val t = fbs.HealthResponse.getRootAsHealthResponse(ByteBuffer.wrap(bytes))
      HealthResponse(
        totalMemory = t.totalMemory(),
        freeMemory = t.freeMemory()
      )
    }

    def toArray(body: HealthResponse): Array[Byte] = {
      val builder = new FlatBufferBuilder()
      val root = fbs.HealthResponse.createHealthResponse(
        builder, body.totalMemory, body.freeMemory
      )
      builder.finish(root)
      builder.sizedByteArray()
    }
  }

  case class LogTailResponse(line: String) extends EventBody {
    override def toArray: Array[Byte] = LogTailResponse.toArray(this)
  }

  object LogTailResponse {
    def apply(bytes: Array[Byte]): LogTailResponse = {
      val t = fbs.LogTailResponse.getRootAsLogTailResponse(ByteBuffer.wrap(bytes))
      LogTailResponse(line = t.line())
    }

    def toArray(body: LogTailResponse): Array[Byte] = {
      val builder = new FlatBufferBuilder()
      val root = fbs.LogTailResponse.createLogTailResponse(
        builder, builder.createString(body.line)
      )
      builder.finish(root)
      builder.sizedByteArray()
    }
  }

  case class LogTailRequest(request: Int, cmd: String) extends EventBody {
    override def toArray: Array[Byte] = LogTailRequest.toArray(this)
  }

  object LogTailRequest {
    def apply(bytes: Array[Byte]): LogTailRequest = {
      val t = fbs.LogTailRequest.getRootAsLogTailRequest(ByteBuffer.wrap(bytes))
      LogTailRequest(
        request = t.request(),
        cmd = t.cmd()
      )
    }

    def toArray(body: LogTailRequest): Array[Byte] = {
      val builder = new FlatBufferBuilder()
      val root = fbs.LogTailRequest.createLogTailRequest(
        builder, body.request, builder.createString(body.cmd)
      )
      builder.finish(root)
      builder.sizedByteArray()
    }
  }

  case class Partition(key: String, bootKeys: Array[String]) extends EventBody {
    override def toArray: Array[Byte] = Partition.toArray(this)
  }

  object Partition {
    def apply(bytes: Array[Byte]): Partition = {
      val t = fbs.Partition.getRootAsPartition(ByteBuffer.wrap(bytes))
      Partition(
        key = t.key(),
        bootKeys = Array.tabulate(t.bootKeysLength())(t.bootKeys)
      )
    }

    def toArray(body: Partition): Array[Byte] = {
      val builder = new FlatBufferBuilder()
      val bootKeys = fbs.Partition.createBootKeysVector(builder, body.bootKeys.map(builder.createString))
      val root = fbs.Partition.createPartition(
        builder, builder.createString(body.key), bootKeys
      )
      builder.finish(root)
      builder.sizedByteArray()
    }
  }

  case class LinkAddress(address: String, instance: String, keys: Array[String], timestamp: Long) extends EventBody {
    override def toArray: Array[Byte] = LinkAddress.toArray(this)
  }

  object LinkAddress {
    def apply(bytes: Array[Byte]): LinkAddress = {
      val t = fbs.LinkAddress.getRootAsLinkAddress(ByteBuffer.wrap(bytes))
      LinkAddress(
        address = t.address(),
        instance = t.instance(),
        keys = Array.tabulate(t.keysLength())(t.keys),
        timestamp = t.timestamp()
      )
    }

    def toArray(body: LinkAddress): Array[Byte] = {
      val builder = new FlatBufferBuilder()
      val keys = fbs.LinkAddress.createKeysVector(builder, body.keys.map(builder.createString))
      val root = fbs.LinkAddress.createLinkAddress(
        builder, builder.createString(body.address), builder.createString(body.instance), keys, body.timestamp
      )
      builder.finish(root)
      builder.sizedByteArray()
    }
  }

  case class LinkVariable(variableKey: String, instance: String, keys: Array[String], timestamp: Long) extends EventBody {
    override def toArray: Array[Byte] = LinkVariable.toArray(this)
  }

  object LinkVariable {
    def apply(bytes: Array[Byte]): LinkVariable = {
      val t = fbs.LinkVariable.getRootAsLinkVariable(ByteBuffer.wrap(bytes))
      LinkVariable(
        variableKey = t.variableKey(),
        instance = t.instance(),
        keys = Array.tabulate(t.keysLength())(t.keys),
        timestamp = t.timestamp()
      )
    }

    def toArray(body: LinkVariable): Array[Byte] = {
      val builder = new FlatBufferBuilder()
      val keys = fbs.LinkVariable.createKeysVector(builder, body.keys.map(builder.createString))
      val root = fbs.LinkVariable.createLinkVariable(
        builder, builder.createString(body.variableKey), builder.createString(body.instance), keys, body.timestamp
      )
      builder.finish(root)
      builder.sizedByteArray()
    }
  }

  case class LinkInstance(instance: String, broker: String, ip: String, port: Int, timestamp: Long) extends EventBody {
    override def toArray: Array[Byte] = LinkInstance.toArray(this)
  }

  object LinkInstance {
    def apply(bytes: Array[Byte]): LinkInstance = {
      val t = fbs.LinkInstance.getRootAsLinkInstance(ByteBuffer.wrap(bytes))
      LinkInstance(
        instance = t.instance(),
        broker = t.broker(),
        ip = t.ip(),
        port = t.port(),
        timestamp = t.timestamp()
      )
    }

    def toArray(body: LinkInstance): Array[Byte] = {
      val builder = new FlatBufferBuilder()
      val root = fbs.LinkInstance.createLinkInstance(
        builder,
        builder.createString(body.instance),
        builder.createString(body.broker),
        builder.createString(body.ip),
        body.port,
        body.timestamp
      )
      builder.finish(root)
      builder.sizedByteArray()
    }
  }

  case class TaskState(task: String, broker: String, timestamp: Long) extends EventBody {
    override def toArray: Array[Byte] = TaskState.toArray(this)
  }

  object TaskState {
    def apply(bytes: Array[Byte]): TaskState = {
      val t = fbs.TaskState.getRootAsTaskState(ByteBuffer.wrap(bytes))
      TaskState(
        task = t.task(),
        broker = t.broker(),
        timestamp = t.timestamp()
      )
    }

    def toArray(body: TaskState): Array[Byte] = {
      val builder = new FlatBufferBuilder()
      val root = fbs.TaskState.createTaskState(
        builder,
        builder.createString(body.task),
        builder.createString(body.broker),
        body.timestamp
      )
      builder.finish(root)
      builder.sizedByteArray()
    }
  }

  case class BrokerState(broker: String, task: String, timestamp: Long) extends EventBody {
    override def toArray: Array[Byte] = BrokerState.toArray(this)
  }

  object BrokerState {
    def apply(bytes: Array[Byte]): BrokerState = {
      val t = fbs.BrokerState.getRootAsBrokerState(ByteBuffer.wrap(bytes))
      BrokerState(
        broker = t.broker(),
        task = t.task(),
        timestamp = t.timestamp()
      )
    }

    def toArray(body: BrokerState): Array[Byte] = {
      val builder = new FlatBufferBuilder()
      val root = fbs.BrokerState.createBrokerState(
        builder,
        builder.createString(body.broker),
        builder.createString(body.task),
        body.timestamp
      )
      builder.finish(root)
      builder.sizedByteArray()
    }
  }

  case class InstanceState(instance: String, broker: String, ip: String, port: Int, partitions: Array[String], timestamp: Long) extends EventBody {
    override def toArray: Array[Byte] = InstanceState.toArray(this)
  }

  object InstanceState {
    def apply(bytes: Array[Byte]): InstanceState = {
      val t = fbs.InstanceState.getRootAsInstanceState(ByteBuffer.wrap(bytes))
      InstanceState(
        instance = t.instance(),
        broker = t.broker(),
        ip = t.ip(),
        port = t.port(),
        partitions = Array.tabulate(t.partitionsLength())(t.partitions),
        timestamp = t.timestamp()
      )
    }

    def toArray(body: InstanceState): Array[Byte] = {
      val builder = new FlatBufferBuilder()
      val partitions = fbs.InstanceState.createPartitionsVector(builder, body.partitions.map(builder.createString))
      val root = fbs.InstanceState.createInstanceState(
        builder,
        builder.createString(body.instance),
        builder.createString(body.broker),
        builder.createString(body.ip),
        body.port,
        partitions,
        body.timestamp
      )
      builder.finish(root)
      builder.sizedByteArray()
    }
  }

  case class KVGet(key: String) extends EventBody {
    override def toArray: Array[Byte] = KVGet.toArray(this)
  }

  object KVGet {
    def apply(bytes: Array[Byte]): KVGet = {
      val t = fbs.KVGet.getRootAsKVGet(ByteBuffer.wrap(bytes))
      KVGet(key = t.key())
    }

    def toArray(body: KVGet): Array[Byte] = {
      val builder = new FlatBufferBuilder()
      val root = fbs.KVGet.createKVGet(builder, builder.createString(body.key))
      builder.finish(root)
      builder.sizedByteArray()
    }
  }

  case class KVRemove(key: String) extends EventBody {
    override def toArray: Array[Byte] = KVRemove.toArray(this)
  }

  object KVRemove {
    def apply(bytes: Array[Byte]): KVRemove = {
      val t = fbs.KVRemove.getRootAsKVRemove(ByteBuffer.wrap(bytes))
      KVRemove(key = t.key())
    }

    def toArray(body: KVRemove): Array[Byte] = {
      val builder = new FlatBufferBuilder()
      val root = fbs.KVRemove.createKVRemove(builder, builder.createString(body.key))
      builder.finish(root)
      builder.sizedByteArray()
    }
  }

  case class RangeRegexKV(key: String, start: Int, limit: Int) extends EventBody {
    override def toArray: Array[Byte] = RangeRegexKV.toArray(this)
  }

  object RangeRegexKV {
    def apply(bytes: Array[Byte]): RangeRegexKV = {
      val t = fbs.RangeRegexKV.getRootAsRangeRegexKV(ByteBuffer.wrap(bytes))
      RangeRegexKV(key = t.key(), start = t.start(), limit = t.limit())
    }

    def toArray(body: RangeRegexKV): Array[Byte] = {
      val builder = new FlatBufferBuilder()
      val root = fbs.RangeRegexKV.createRangeRegexKV(builder, builder.createString(body.key), body.start, body.limit)
      builder.finish(root)
      builder.sizedByteArray()
    }
  }

  case class KV(key: String, value: String) extends EventBody {
    def keyValue = KeyValue(key, value)

    override def toArray: Array[Byte] = KV.toArray(this)
  }

  object KV {
    def apply(t: fbs.KV): KV = {
      KV(key = t.key(), value = t.value())
    }

    def apply(bytes: Array[Byte]): KV = {
      val t = fbs.KV.getRootAsKV(ByteBuffer.wrap(bytes))
      apply(t)
    }

    def toOffset(body: KV, builder: FlatBufferBuilder): Int = {
      fbs.KV.createKV(builder, builder.createString(body.key), builder.createString(body.value))
    }

    def toArray(body: KV): Array[Byte] = {
      val builder = new FlatBufferBuilder()
      val root = toOffset(body, builder)
      builder.finish(root)
      builder.sizedByteArray()
    }
  }

  case class AtomicKV(key: String, expect: String, update: String) extends EventBody {
    override def toArray: Array[Byte] = AtomicKV.toArray(this)
  }

  object AtomicKV {
    def apply(bytes: Array[Byte]): AtomicKV = {
      val t = fbs.AtomicKV.getRootAsAtomicKV(ByteBuffer.wrap(bytes))
      AtomicKV(key = t.key(), expect = t.expect(), update = t.update())
    }

    def toArray(body: AtomicKV): Array[Byte] = {
      val builder = new FlatBufferBuilder()
      val root = fbs.AtomicKV.createAtomicKV(builder,
        builder.createString(body.key),
        builder.createString(body.expect),
        builder.createString(body.update)
      )
      builder.finish(root)
      builder.sizedByteArray()
    }
  }

  case class KVS(kvs: Seq[KV]) extends EventBody {
    override def toArray: Array[Byte] = KVS.toArray(this)
  }

  object KVS {
    def apply(bytes: Array[Byte]): KVS = {
      val t = fbs.KVS.getRootAsKVS(ByteBuffer.wrap(bytes))
      KVS(Array.tabulate(t.kvsLength())(i ⇒ KV(t.kvs(i))))
    }

    def toArray(body: KVS): Array[Byte] = {
      val builder = new FlatBufferBuilder()
      val kvs = fbs.KVS.createKvsVector(builder, body.kvs.map(KV.toOffset(_, builder)).toArray)
      val root = fbs.KVS.createKVS(builder, kvs)
      builder.finish(root)
      builder.sizedByteArray()
    }
  }

  class Empty extends EventBody {
    override def toArray: Array[Byte] = Empty.toArray
  }

  object Empty {
    val empty = new Empty

    def apply(bytes: Array[Byte]): Empty = empty

    def toArray: Array[Byte] = Array.emptyByteArray
  }

}