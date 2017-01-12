package teleporter.integration.cluster.rpc

import java.nio.ByteBuffer

import com.google.flatbuffers.FlatBufferBuilder
import teleporter.integration.cluster.rpc.fbs.generate._

/**
  * Created by huanwuji 
  * date 2017/1/3.
  */
object Broker

case class LogRequest(request: Int, cmd: String)

object LogRequest {
  def apply(bytes: Array[Byte]): LogRequest = {
    val t = broker.LogRequest.getRootAsLogRequest(ByteBuffer.wrap(bytes))
    LogRequest(
      request = t.request(),
      cmd = t.cmd()
    )
  }

  def toArray(body: LogRequest): Array[Byte] = {
    val builder = new FlatBufferBuilder()
    val root = broker.LogRequest.createLogRequest(
      builder, body.request, builder.createString(body.cmd)
    )
    builder.finish(root)
    builder.sizedByteArray()
  }
}

case class Partition(key: String, bootKeys: Array[String])

object Partition {
  def apply(bytes: Array[Byte]): Partition = {
    val t = broker.Partition.getRootAsPartition(ByteBuffer.wrap(bytes))
    Partition(
      key = t.key(),
      bootKeys = Array.tabulate(t.bootKeysLength())(t.bootKeys)
    )
  }

  def toArray(body: Partition): Array[Byte] = {
    val builder = new FlatBufferBuilder()
    val bootKeys = broker.Partition.createBootKeysVector(builder, body.bootKeys.map(builder.createString))
    val root = broker.Partition.createPartition(
      builder, builder.createString(body.key), bootKeys
    )
    builder.finish(root)
    builder.sizedByteArray()
  }
}

case class LinkAddress(address: String, instance: String, keys: Array[String], timestamp: Long)

object LinkAddress {
  def apply(bytes: Array[Byte]): LinkAddress = {
    val t = broker.LinkAddress.getRootAsLinkAddress(ByteBuffer.wrap(bytes))
    LinkAddress(
      address = t.address(),
      instance = t.instance(),
      keys = Array.tabulate(t.keysLength())(t.keys),
      timestamp = t.timestamp()
    )
  }

  def toArray(body: LinkAddress): Array[Byte] = {
    val builder = new FlatBufferBuilder()
    val keys = broker.LinkAddress.createKeysVector(builder, body.keys.map(builder.createString))
    val root = broker.LinkAddress.createLinkAddress(
      builder, builder.createString(body.address), builder.createString(body.instance), keys, body.timestamp
    )
    builder.finish(root)
    builder.sizedByteArray()
  }
}

case class LinkVariable(variableKey: String, instance: String, keys: Array[String], timestamp: Long)

object LinkVariable {
  def apply(bytes: Array[Byte]): LinkVariable = {
    val t = broker.LinkVariable.getRootAsLinkVariable(ByteBuffer.wrap(bytes))
    LinkVariable(
      variableKey = t.variableKey(),
      instance = t.instance(),
      keys = Array.tabulate(t.keysLength())(t.keys),
      timestamp = t.timestamp()
    )
  }

  def toArray(body: LinkVariable): Array[Byte] = {
    val builder = new FlatBufferBuilder()
    val keys = broker.LinkVariable.createKeysVector(builder, body.keys.map(builder.createString))
    val root = broker.LinkVariable.createLinkVariable(
      builder, builder.createString(body.variableKey), builder.createString(body.instance), keys, body.timestamp
    )
    builder.finish(root)
    builder.sizedByteArray()
  }
}

case class LinkInstance(instance: String, broker: String, ip: String, port: Int, timestamp: Long)

object LinkInstance {
  def apply(bytes: Array[Byte]): LinkInstance = {
    val t = broker.LinkInstance.getRootAsLinkInstance(ByteBuffer.wrap(bytes))
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
    val root = broker.LinkInstance.createLinkInstance(
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

case class TaskState(task: String, broker: String, timestamp: Long)

object TaskState {
  def apply(bytes: Array[Byte]): TaskState = {
    val t = broker.TaskState.getRootAsTaskState(ByteBuffer.wrap(bytes))
    TaskState(
      task = t.task(),
      broker = t.broker(),
      timestamp = t.timestamp()
    )
  }

  def toArray(body: TaskState): Array[Byte] = {
    val builder = new FlatBufferBuilder()
    val root = broker.TaskState.createTaskState(
      builder,
      builder.createString(body.task),
      builder.createString(body.broker),
      body.timestamp
    )
    builder.finish(root)
    builder.sizedByteArray()
  }
}

case class BrokerState(broker: String, task: String, timestamp: Long)

object BrokerState {
  def apply(bytes: Array[Byte]): BrokerState = {
    val t = broker.BrokerState.getRootAsBrokerState(ByteBuffer.wrap(bytes))
    BrokerState(
      broker = t.broker(),
      task = t.task(),
      timestamp = t.timestamp()
    )
  }

  def toArray(body: BrokerState): Array[Byte] = {
    val builder = new FlatBufferBuilder()
    val root = broker.BrokerState.createBrokerState(
      builder,
      builder.createString(body.broker),
      builder.createString(body.task),
      body.timestamp
    )
    builder.finish(root)
    builder.sizedByteArray()
  }
}

case class InstanceState(instance: String, broker: String, ip: String, port: Int, partitions: Array[String], timestamp: Long)

object InstanceState {
  def apply(bytes: Array[Byte]): InstanceState = {
    val t = broker.InstanceState.getRootAsInstanceState(ByteBuffer.wrap(bytes))
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
    val partitions = broker.InstanceState.createPartitionsVector(builder, body.partitions.map(builder.createString))
    val root = broker.InstanceState.createInstanceState(
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

case class KVGet(key: String)

object KVGet {
  def apply(bytes: Array[Byte]): KVGet = {
    val t = broker.KVGet.getRootAsKVGet(ByteBuffer.wrap(bytes))
    KVGet(key = t.key())
  }

  def toArray(body: KVGet): Array[Byte] = {
    val builder = new FlatBufferBuilder()
    val root = broker.KVGet.createKVGet(builder, builder.createString(body.key))
    builder.finish(root)
    builder.sizedByteArray()
  }
}

case class KVRemove(key: String)

object KVRemove {
  def apply(bytes: Array[Byte]): KVRemove = {
    val t = broker.KVRemove.getRootAsKVRemove(ByteBuffer.wrap(bytes))
    KVRemove(key = t.key())
  }

  def toArray(body: KVRemove): Array[Byte] = {
    val builder = new FlatBufferBuilder()
    val root = broker.KVRemove.createKVRemove(builder, builder.createString(body.key))
    builder.finish(root)
    builder.sizedByteArray()
  }
}

case class RangeRegexKV(key: String, start: Int, limit: Int)

object RangeRegexKV {
  def apply(bytes: Array[Byte]): RangeRegexKV = {
    val t = broker.RangeRegexKV.getRootAsRangeRegexKV(ByteBuffer.wrap(bytes))
    RangeRegexKV(key = t.key(), start = t.start(), limit = t.limit())
  }

  def toArray(body: RangeRegexKV): Array[Byte] = {
    val builder = new FlatBufferBuilder()
    val root = broker.RangeRegexKV.createRangeRegexKV(builder, builder.createString(body.key), body.start, body.limit)
    builder.finish(root)
    builder.sizedByteArray()
  }
}