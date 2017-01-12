package teleporter.integration.utils

import scala.util.hashing.MurmurHash3

/**
  * Created by huanwuji 
  * date 2016/12/22.
  */
trait Hashing {
  val defaultHashCode: Int = -1
  val defaultHashByteArray: Array[Byte] = Bytes.toBytes(defaultHashCode)

  def hashValue(value: Any): Int = {
    val bytes = value match {
      case value: Boolean ⇒ Bytes.toBytes(value)
      case value: Char ⇒ Bytes.toBytes(value)
      case value: Short ⇒ Bytes.toBytes(value)
      case value: Int ⇒ Bytes.toBytes(value)
      case value: Long ⇒ Bytes.toBytes(value)
      case value: Float ⇒ Bytes.toBytes(value)
      case value: Double ⇒ Bytes.toBytes(value)
      case value: BigDecimal ⇒ Bytes.toBytes(value)
      case value: String ⇒ Bytes.toBytes(value)
      case null ⇒ defaultHashByteArray
      case x ⇒ Bytes.toBytes(String.valueOf(x))
    }
    MurmurHash3.bytesHash(bytes)
  }

  def hash(seq: Traversable[Any]): Seq[Byte] = {
    seq.map(hashValue).flatMap(i ⇒ Bytes.toBytes(i)).toSeq
  }

  def hash(m: Map[String, Any]): Seq[Byte] = {
    hash(m.values)
  }

  def hash(keys: Seq[String], m: Map[String, Any]): Seq[Byte] = {
    hash(keys.map(m(_)))
  }
}

object Hashing extends Hashing

trait ColumnFilter extends Hashing {
  def filterChanges(m: Map[String, Any], hashCodes: Array[Byte]): Map[String, Any] = {
    if (m.size * 4 != hashCodes.length) return m
    var i = -1
    m.filter { case (_, v) ⇒ i += 1; hashValue(v) != Bytes.toInt(hashCodes.slice(i * 4, i * 4 + 4)) }
  }

  def filterChanges(keys: Seq[String], m: Map[String, Any], hashCodes: Array[Byte]): Map[String, Any] = {
    if (keys.size == hashCodes.length) return m
    var i = -1
    val filterKeys = keys.filter {
      key ⇒ i += 1; m.getOrElse(key, defaultHashCode) != Bytes.toInt(hashCodes.slice(i * 4, i * 4 + 4))
    }.toSet
    m.filterKeys(filterKeys.contains)
  }
}

object ColumnFilter extends ColumnFilter