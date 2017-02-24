package teleporter.integration.utils

import java.sql.Timestamp
import java.time.LocalDateTime

import scala.concurrent.duration.Duration

/**
  * Author: kui.dai
  * Date: 2016/4/22.
  */
trait Conversions {
  private val NULL: String = "null"

  def asInt(v: Any): Option[Int] = v match {
    case NULL ⇒ None
    case s: String if s.isEmpty ⇒ None
    case s: String ⇒ Some(s.toInt)
    case i: Int ⇒ Some(i)
    case d: Double => Some(d.toInt)
    case bi: BigInt ⇒ Some(bi.toInt)
    case _ ⇒ throw new IllegalArgumentException(s"No match int type convert for type:${v.getClass}, value:$v")
  }

  def asLong(v: Any): Option[Long] = v match {
    case s: String if s.isEmpty ⇒ None
    case NULL ⇒ None
    case s: String if s.isEmpty ⇒ None
    case s: String ⇒ Some(s.toLong)
    case bi: BigInt ⇒ Some(bi.toLong)
    case i: Int ⇒ Some(i.toLong)
    case d: Double => Some(d.toLong)
    case l: Long ⇒ Some(l)
    case _ ⇒ throw new IllegalArgumentException(s"No match long type convert for type:${v.getClass}, value:$v")
  }

  def asString(v: Any): String = v match {
    case d: LocalDateTime ⇒ d.format(Dates.DEFAULT_DATE_FORMATTER)
    case _ ⇒ v.toString
  }

  def asNonEmptyString(v: Any): Option[String] = v match {
    case s: String if s.isEmpty ⇒ None
    case x ⇒ Option(x.toString)
  }

  def asDuration(v: Any): Option[Duration] = v match {
    case s: String if s.isEmpty ⇒ None
    case s: String ⇒ Some(Duration(s))
    case _ ⇒ throw new IllegalArgumentException(s"No match duration type convert for type:${v.getClass}, value:$v")
  }

  def asBoolean(v: Any): Option[Boolean] = v match {
    case s: String if s.isEmpty ⇒ None
    case NULL ⇒ None
    case s: String ⇒ Some(s.toBoolean)
    case b: Boolean ⇒ Some(b)
    case jb: java.lang.Boolean ⇒ Some(jb)
    case _ ⇒ throw new IllegalArgumentException(s"No match boolean type convert for type:${v.getClass}, value:$v")
  }

  def asDouble(v: Any): Option[Double] = v match {
    case s: String if s.isEmpty ⇒ None
    case s: String ⇒ Some(s.toDouble)
    case _ ⇒ throw new IllegalArgumentException(s"No match double type convert for type:${v.getClass}, value:$v")
  }

  def asFloat(v: Any): Option[Float] = v match {
    case NULL ⇒ None
    case s: String if s.isEmpty ⇒ None
    case s: String ⇒ Some(s.toFloat)
    case s: Int ⇒ Some(s.toFloat)
    case _ ⇒ throw new IllegalArgumentException(s"No match float type convert for type:${v.getClass}, value:$v")
  }

  def asSqlDate(v: Any): Option[java.sql.Date] = v match {
    case d: java.util.Date ⇒ Some(new java.sql.Date(d.getTime))
    case NULL ⇒ None
    case s: String if s.isEmpty ⇒ None
    case s: String ⇒ Some(java.sql.Date.valueOf(s))
    case _ ⇒ throw new IllegalArgumentException(s"No match sqlDate type convert for type:${v.getClass}, value:$v")
  }

  def asTimestamp(v: Any): Option[Timestamp] = v match {
    case d: java.util.Date ⇒ Some(new Timestamp(d.getTime))
    case NULL ⇒ None
    case s: String if s.isEmpty ⇒ None
    case s: String ⇒ Some(Timestamp.valueOf(s))
    case _ ⇒ throw new IllegalArgumentException(s"No match timestamp type convert for type:${v.getClass}, value:$v")
  }

  def asDate(v: Any): Option[LocalDateTime] = v match {
    case d: java.time.LocalDateTime ⇒ Some(d)
    case NULL ⇒ None
    case s: String if s.isEmpty ⇒ None
    case s: String ⇒ Some(Timestamp.valueOf(s).toLocalDateTime)
    case _ ⇒ throw new IllegalArgumentException(s"No match localDateTime type convert for type:${v.getClass}, value:$v")
  }

  def asMapBean(v: Any): Option[MapBean] = v match {
    case null ⇒ None
    case map: Map[String, Any] ⇒ Some(MapBean(map))
    case mapBean: MapBean ⇒ Some(mapBean)
    case _ ⇒ throw new IllegalArgumentException(s"No match mapBean type convert for type:${v.getClass}, value:$v")
  }

  def asSeq(v: Any): Option[Seq[Any]] = v match {
    case null ⇒ None
    case s: Seq[Any] ⇒ Some(s)
    case _ ⇒ throw new IllegalArgumentException(s"No match seq type convert for type:${v.getClass}, value:$v")
  }
}

trait Converter[A] {
  def to(v: Any): Option[A]
}

trait Converters extends Conversions {

  implicit object IntConverter extends Converter[Int] {
    override def to(v: Any): Option[Int] = asInt(v)
  }

  implicit object LongConverter extends Converter[Long] {
    override def to(v: Any): Option[Long] = asLong(v)
  }

  implicit object StringConverter extends Converter[String] {
    override def to(v: Any): Option[String] = asNonEmptyString(v)
  }

  implicit object BooleanConverter extends Converter[Boolean] {
    override def to(v: Any): Option[Boolean] = asBoolean(v)
  }

  implicit object FloatConverter extends Converter[Float] {
    override def to(v: Any): Option[Float] = asFloat(v)
  }

  implicit object DoubleConverter extends Converter[Double] {
    override def to(v: Any): Option[Double] = asDouble(v)
  }

  implicit object SqlDateConverter extends Converter[java.sql.Date] {
    override def to(v: Any): Option[java.sql.Date] = asSqlDate(v)
  }

  implicit object TimestampConverter extends Converter[Timestamp] {
    override def to(v: Any): Option[Timestamp] = asTimestamp(v)
  }

  implicit object LocalDateTimeConverter extends Converter[LocalDateTime] {
    override def to(v: Any): Option[LocalDateTime] = asDate(v)
  }

  implicit object DurationConverter extends Converter[Duration] {
    override def to(v: Any): Option[Duration] = asDuration(v)
  }

  implicit object MapBeanConverter extends Converter[MapBean] {
    override def to(v: Any): Option[MapBean] = asMapBean(v)
  }

  implicit object SeqConverter extends Converter[Seq[Any]] {
    override def to(v: Any): Option[Seq[Any]] = asSeq(v)
  }

}

trait Convert {
  def toOption[A](v: Any)(implicit converter: Converter[A]): Option[A] =
    v match {
      case null ⇒ None
      case value ⇒ converter.to(value)
    }

  def to[A](v: Any)(implicit converter: Converter[A]): A = toOption[A](v).get
}

object Converters extends Converters with Convert