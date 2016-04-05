package teleporter.integration.core

import java.nio.ByteBuffer

import akka.actor.ActorRef

/**
 * date 2015/8/3.
 * @author daikui
 */
case class TId(persistenceId: Int, seqNr: Long, channelId: Int = 0) {
  def toBytes: Array[Byte] = TId.keyToBytes(this)

  def switchChannel(channelId: Int) = this.copy(channelId = 1 << channelId - 1)
}

object TId {
  val length = 16
  val empty = TId(0, 0, 0)

  def keyToByteBuffer(key: TId): ByteBuffer = {
    val bb = ByteBuffer.allocate(length)
    bb.putInt(key.persistenceId)
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

case class TeleporterMessage[A](id: TId = TId.empty,
                                sourceRef: ActorRef = null,
                                data: A,
                                toNext: TeleporterMessage[A] ⇒ Unit = { msg: TeleporterMessage[A] ⇒ if (msg.sourceRef != null) msg.sourceRef ! msg.id })