package teleporter.integration.cluster.rpc

import java.nio.charset.StandardCharsets

import akka.util.ByteString
import com.google.flatbuffers.FlatBufferBuilder
import teleporter.integration.cluster.rpc.fbs.{MessageStatus, Role}

/**
  * Created by huanwuji on 2017/4/10.
  */
case class RpcMessage[T <: EventBody](var seqNr: Long = 0, messageType: Byte, status: Byte = MessageStatus.None, role: Byte, body: Option[T])

object RpcMessage {
  def decode(bs: ByteString): fbs.RpcMessage = {
    fbs.RpcMessage.getRootAsRpcMessage(bs.asByteBuffer)
  }
}

case class RpcRequest[T](var seqNr: Long, messageType: Byte, body: Option[T])

object RpcRequest {
  def encodeByteString(seqNr: Long, messageType: Byte, array: Array[Byte]): ByteString = ByteString(encodeArray(seqNr, messageType, array))

  def encodeArray(seqNr: Long, messageType: Byte, array: Array[Byte]): Array[Byte] = {
    val builder = new FlatBufferBuilder(array.length + 50)
    val bodyOffset = if (array.isEmpty) 0 else fbs.RpcMessage.createBodyVector(builder, array)
    val root = fbs.RpcMessage.createRpcMessage(
      builder, seqNr, messageType, Role.Request, 0, bodyOffset
    )
    builder.finish(root)
    builder.sizedByteArray()
  }

  def decode[T](response: fbs.RpcMessage, decodeBody: Array[Byte] ⇒ T): RpcRequest[T] = {
    RpcRequest(
      seqNr = response.seqNr(),
      messageType = response.messageType(),
      body = if (response.bodyLength() == 0) None
      else Some(decodeBody(Array.tabulate(response.bodyLength())(response.body)))
    )
  }
}

case class RpcResponse[T](var seqNr: Long, messageType: Byte, status: Byte, body: Option[T])

object RpcResponse {
  def encode[T](seqNr: Long, messageType: Byte, status: Byte, array: Array[Byte]): ByteString = {
    val builder = new FlatBufferBuilder()
    val bodyOffset = if (array.isEmpty) 0 else fbs.RpcMessage.createBodyVector(builder, array)
    val root = fbs.RpcMessage.createRpcMessage(
      builder, seqNr, messageType, Role.Response, status, bodyOffset
    )
    builder.finish(root)
    ByteString(builder.sizedByteArray())
  }

  def success[T](seqNr: Long, messageType: Byte, array: Array[Byte]): ByteString = {
    encode(seqNr, messageType, MessageStatus.Success, array)
  }

  def success[T](message: fbs.RpcMessage, array: Array[Byte]): ByteString = {
    encode(message.seqNr(), message.messageType(), MessageStatus.Success, array)
  }

  def success(message: fbs.RpcMessage): ByteString = {
    success(message.seqNr(), message.messageType(), EventBody.Empty.toArray)
  }

  def failure(seqNr: Long, messageType: Byte, errorMessage: String): ByteString = {
    encode(seqNr, messageType, MessageStatus.Failure, errorMessage.getBytes(StandardCharsets.UTF_8))
  }

  def failure(message: fbs.RpcMessage, errorMessage: String): ByteString = {
    encode(message.seqNr(), message.messageType(), MessageStatus.Failure, errorMessage.getBytes(StandardCharsets.UTF_8))
  }

  def decode[T](response: fbs.RpcMessage, decodeBody: Array[Byte] ⇒ T): RpcResponse[T] = {
    RpcResponse(
      seqNr = response.seqNr(),
      messageType = response.messageType(),
      status = response.status(),
      body = if (response.bodyLength() == 0) None
      else Some(decodeBody(Array.tabulate(response.bodyLength())(response.body)))
    )
  }
}