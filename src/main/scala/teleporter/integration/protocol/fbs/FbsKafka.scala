package teleporter.integration.protocol.fbs

import java.nio.ByteBuffer

import akka.actor.ActorRef
import com.google.flatbuffers.FlatBufferBuilder
import teleporter.integration.component._
import teleporter.integration.core.{TId, TeleporterMessage}
import teleporter.integration.protocol.fbs.generate.{FbsKafkaRecord, FbsKafkaRecords, JdbcMessages}

/**
  * Author: kui.dai
  * Date: 2016/4/18.
  */
object FbsKafka {
  def apply(message: TeleporterKafkaMessage, builder: FlatBufferBuilder): Int = {
    val kafkaData = message.data
    val tId = FbsKafkaRecord.createTIdVector(builder, message.id.toBytes)
    val topic = builder.createString(kafkaData.topic)
    val key = if (kafkaData.key() == null) 0 else FbsKafkaRecord.createKeyVector(builder, kafkaData.key())
    val data = FbsKafkaRecord.createDataVector(builder, kafkaData.message())
    FbsKafkaRecord.createFbsKafkaRecord(builder, tId, topic, key, kafkaData.partition, data)
  }

  def apply(messages: Seq[TeleporterKafkaMessage], initialCapacity: Int): FlatBufferBuilder = {
    val builder = new FlatBufferBuilder(initialCapacity)
    val records = JdbcMessages.createMessagesVector(builder, messages.map(apply(_, builder)).toArray)
    val root = JdbcMessages.createJdbcMessages(builder, records)
    builder.finish(root)
    builder
  }

  def unapply(record: generate.FbsKafkaRecord, sourceRef: ActorRef): TeleporterKafkaRecord = {
    val tId = TId.keyFromBytes(Array.tabulate(record.tIdLength())(record.tId))
    val key = Array.tabulate(record.keyLength())(record.key)
    val data = Array.tabulate(record.dataLength())(record.data)
    val kafkaRecord = new KafkaRecord(record.topic(), record.partition(), key, data)
    TeleporterMessage[KafkaRecord](id = tId, sourceRef = sourceRef, data = kafkaRecord)
  }

  def unapply(byteBuffer: ByteBuffer, sourceRef: ActorRef): scala.collection.immutable.Seq[TeleporterKafkaRecord] = {
    val records = FbsKafkaRecords.getRootAsFbsKafkaRecords(byteBuffer)
    scala.collection.immutable.Seq.tabulate(records.recordsLength())(records.records).map(unapply(_, sourceRef))
  }

  def unapply(bytes: Array[Byte], sourceRef: ActorRef): scala.collection.immutable.Seq[TeleporterKafkaRecord] = {
    val records = FbsKafkaRecords.getRootAsFbsKafkaRecords(ByteBuffer.wrap(bytes))
    scala.collection.immutable.Seq.tabulate(records.recordsLength())(records.records).map(unapply(_, sourceRef))
  }
}