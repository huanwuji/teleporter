package teleporter.integration.core

import java.nio.ByteBuffer

import akka.stream.stage.AsyncCallback

/**
  * date 2015/8/3.
  *
  * @author daikui
  */
case class TId(persistenceId: Long, seqNr: Long, channelId: Int = 0) {
  def toBytes: Array[Byte] = TId.keyToBytes(this)

  def switchChannel(channelId: Int): TId = this.copy(channelId = channelId)
}

object TId {
  val length = 16
  val empty = TId(-1, -1, -1)

  def keyToByteBuffer(key: TId): ByteBuffer = {
    val bb = ByteBuffer.allocate(length)
    bb.putLong(key.persistenceId)
    bb.putLong(key.seqNr)
    bb.putInt(key.channelId)
  }

  def keyToBytes(key: TId): Array[Byte] = {
    keyToByteBuffer(key).array
  }

  def keyFromBytes(bytes: Array[Byte]): TId = {
    val bb = ByteBuffer.wrap(bytes)
    val aid = bb.getInt
    val snr = bb.getLong
    val cid = bb.getInt
    new TId(aid, snr, cid)
  }
}

trait Message[+T] {
  def data: T
}

case class DefaultMessage[T](data: T) extends Message[T]

case class SourceMessage[XY, T](coordinate: XY, data: T) extends Message[T]

case class AckMessage[XY, T](id: TId, coordinate: XY, data: T, confirmed: AsyncCallback[TId]) extends Message[T] {
  def toTransferMessage: TransferMessage[T] = TransferMessage(id, data)

  def map[B](f: T ⇒ B): AckMessage[XY, B] = this.copy(data = f(data))

  def toMessage[B]: Message[B] = asInstanceOf[Message[B]]
}

case class TransferMessage[T](id: TId, data: T) extends Message[T]

object Message {
  def apply[T](data: T) = DefaultMessage(data)

  def source[XY, T](coordinate: XY, data: T) = SourceMessage(coordinate, data)

  def ack[XY, T](id: TId, coordinate: XY, data: T, confirmed: AsyncCallback[TId]) = AckMessage(id, coordinate, data, confirmed)

  def transfer[T](id: TId, data: T) = TransferMessage(id, data)
}