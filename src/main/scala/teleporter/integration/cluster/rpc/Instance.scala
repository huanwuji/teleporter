package teleporter.integration.cluster.rpc

import java.nio.ByteBuffer

import com.google.flatbuffers.FlatBufferBuilder
import teleporter.integration.cluster.rpc.fbs.generate._

/**
  * Created by huanwuji 
  * date 2017/1/3.
  */
case class ConfigChangeNotify(key: String, action: Byte, timestamp: Long)

object ConfigChangeNotify {
  def apply(bytes: Array[Byte]): ConfigChangeNotify = {
    val t = instance.ConfigChangeNotify.getRootAsConfigChangeNotify(ByteBuffer.wrap(bytes))
    ConfigChangeNotify(
      key = t.key(),
      action = t.action(),
      timestamp = t.timestamp()
    )
  }

  def toArray(body: ConfigChangeNotify): Array[Byte] = {
    val builder = new FlatBufferBuilder()
    val root = instance.ConfigChangeNotify.createConfigChangeNotify(
      builder, builder.createString(body.key), body.action, body.timestamp
    )
    builder.finish(root)
    builder.sizedByteArray()
  }
}

case class HealthResponse(totalMemory: Float, freeMemory: Float)

object HealthResponse {
  def apply(bytes: Array[Byte]): HealthResponse = {
    val t = instance.HealthResponse.getRootAsHealthResponse(ByteBuffer.wrap(bytes))
    HealthResponse(
      totalMemory = t.totalMemory(),
      freeMemory = t.freeMemory()
    )
  }

  def toArray(body: HealthResponse): Array[Byte] = {
    val builder = new FlatBufferBuilder()
    val root = instance.HealthResponse.createHealthResponse(
      builder, body.totalMemory, body.freeMemory
    )
    builder.finish(root)
    builder.sizedByteArray()
  }
}

case class LogResponse(line: String)

object LogResponse {
  def apply(bytes: Array[Byte]): LogResponse = {
    val t = instance.LogResponse.getRootAsLogResponse(ByteBuffer.wrap(bytes))
    LogResponse(line = t.line())
  }

  def toArray(body: LogResponse): Array[Byte] = {
    val builder = new FlatBufferBuilder()
    val root = instance.LogResponse.createLogResponse(
      builder, builder.createString(body.line)
    )
    builder.finish(root)
    builder.sizedByteArray()
  }
}