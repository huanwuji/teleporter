package teleporter.integration.utils

import scala.util.hashing.MurmurHash3

/**
  * Created by huanwuji 
  * date 2016/12/22.
  */
trait FieldsChange {
  def hash(value: Any): Int = {
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
      case x ⇒ Bytes.toBytes(String.valueOf(x))
    }
    MurmurHash3.bytesHash(bytes)
  }

  def hash(seq: Seq[Any]): Seq[Byte] = {
    seq.map(hash).flatMap(Bytes.toBytes)
  }
}

class FieldsChangeImpl extends FieldsChange

object FieldsChange {
  def apply(): FieldsChange = new FieldsChangeImpl
}