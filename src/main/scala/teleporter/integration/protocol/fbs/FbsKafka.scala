package teleporter.integration.protocol.fbs

import java.nio.ByteBuffer

import com.google.flatbuffers.FlatBufferBuilder
import kafka.message.MessageAndMetadata
import org.apache.kafka.clients.producer.ProducerRecord
import teleporter.integration.core.{AckMessage, TId, TransferMessage}
import teleporter.integration.protocol.fbs.generate.{FbsKafkaRecord, FbsKafkaRecords, JdbcMessages}

import scala.collection.immutable.Seq

/**
  * Author: kui.dai
  * Date: 2016/4/18.
  */
object FbsKafka {
  def apply(record: generate.FbsKafkaRecord): TransferMessage[ProducerRecord[Array[Byte], Array[Byte]]] = {
    val tId = TId.keyFromBytes(Array.tabulate(record.tIdLength())(record.tId))
    val key = Array.tabulate(record.keyLength())(record.key)
    val data = Array.tabulate(record.dataLength())(record.data)
    val kafkaRecord = new ProducerRecord[Array[Byte], Array[Byte]](record.topic(), record.partition(), key, data)
    TransferMessage[ProducerRecord[Array[Byte], Array[Byte]]](id = tId, data = kafkaRecord)
  }

  def apply(byteBuffer: ByteBuffer): Seq[TransferMessage[ProducerRecord[Array[Byte], Array[Byte]]]] = {
    val records = FbsKafkaRecords.getRootAsFbsKafkaRecords(byteBuffer)
    scala.collection.immutable.Seq.tabulate(records.recordsLength())(records.records).map(apply)
  }

  def apply(bytes: Array[Byte]): Seq[TransferMessage[ProducerRecord[Array[Byte], Array[Byte]]]] = {
    val records = FbsKafkaRecords.getRootAsFbsKafkaRecords(ByteBuffer.wrap(bytes))
    scala.collection.immutable.Seq.tabulate(records.recordsLength())(records.records).map(apply)
  }

  def unapply(message: AckMessage[_, MessageAndMetadata[Array[Byte], Array[Byte]]], builder: FlatBufferBuilder): Int = {
    val kafkaData = message.data
    val tId = FbsKafkaRecord.createTIdVector(builder, message.id.toBytes)
    val topic = builder.createString(kafkaData.topic)
    val key = if (kafkaData.key() == null) 0 else FbsKafkaRecord.createKeyVector(builder, kafkaData.key())
    val data = FbsKafkaRecord.createDataVector(builder, kafkaData.message())
    FbsKafkaRecord.createFbsKafkaRecord(builder, tId, topic, key, kafkaData.partition, data)
  }

  def unapply(messages: Seq[AckMessage[_, MessageAndMetadata[Array[Byte], Array[Byte]]]], initialCapacity: Int): FlatBufferBuilder = {
    val builder = new FlatBufferBuilder(initialCapacity)
    val records = JdbcMessages.createMessagesVector(builder, messages.map(unapply(_, builder)).toArray)
    val root = JdbcMessages.createJdbcMessages(builder, records)
    builder.finish(root)
    builder
  }
}