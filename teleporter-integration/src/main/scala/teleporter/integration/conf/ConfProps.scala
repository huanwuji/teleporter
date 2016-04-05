package teleporter.integration.conf

import java.time.LocalDateTime

import teleporter.integration.DEFAULT_DATE_FORMATTER
import teleporter.integration.conf.Conf.Props

import scala.collection.immutable.Map
import scala.concurrent.duration.Duration

/**
 * Author: kui.dai
 * Date: 2015/11/26.
 */
trait BasicConversions {
  def asInt(v: Any): Int = v match {
    case s: String ⇒ s.toInt
    case i: Int ⇒ i
    case d: Double => d.toInt
    case bi: BigInt ⇒ bi.toInt
    case _ ⇒ throw new IllegalArgumentException(s"No match type convert for type:${v.getClass}, value:$v")
  }

  def asLong(v: Any): Long = v match {
    case s: String ⇒ s.toLong
    case bi: BigInt ⇒ bi.toLong
    case d: Double => d.toLong
    case l: Long ⇒ l
    case _ ⇒ throw new IllegalArgumentException(s"No match type convert for type:${v.getClass}, value:$v")
  }

  def asString(v: Any): String = v.toString

  def asBoolean(v: Any): Boolean = v match {
    case s: String ⇒ s.toBoolean
    case _ ⇒ throw new IllegalArgumentException(s"No match type convert for type:${v.getClass}, value:$v")
  }
}

trait PropsSupport extends BasicConversions {
  val COMPONENT_NS = "cmp"

  def get[T](props: Props, key: String): T = props(key).asInstanceOf[T]

  def getStringOpt(props: Props, key: String): Option[String] = props.get(key).map(asString)

  def getStringOrElse(props: Props, key: String, default: String): String = getStringOpt(props, key).getOrElse(default)

  def getIntOpt(props: Props, key: String): Option[Int] = props.get(key).map(asInt)

  def getIntOrElse(props: Props, key: String, default: Int): Int = getIntOpt(props, key).getOrElse(default)

  def getLongOpt(props: Props, key: String): Option[Long] = props.get(key).map(asLong)

  def getLongOrElse(props: Props, key: String, default: Long): Long = getLongOpt(props, key).getOrElse(default)

  def getBooleanOpt(props: Props, key: String): Option[Boolean] = props.get(key).map(asBoolean)

  def getBooleanOrElse(props: Props, key: String, default: Boolean): Boolean = getBooleanOpt(props, key).getOrElse(default)

  def ns(props: Props, namespace: String): Props = props.filter(_._1.startsWith(namespace))

  def cmpProps(props: Props): Props = get[Props](props, COMPONENT_NS)

  private val _upperCaseReg = """[A-Z]""".r

  def camel2Point(props: Props): Props = props.foldLeft(Map[String,Any]()) {
    (m, entry) ⇒ m + (_upperCaseReg.replaceAllIn(entry._1, "." + _.toString().toLowerCase) → entry._2)
  }
}

trait ConfProps[T]

trait PropsAdapter extends PropsSupport {
  val NS_CMP = "cmp"
  val props: Props
  val cmpProps = get[Props](props, NS_CMP)

  def updateProp(kv: (String, String)): Props = updateProp(Nil, kv)

  def updateProps(kv: (String, String)*): Props = updateProps(Nil, kv: _*)

  def updateProp(path: String, kv: (String, String)): Props = updateProp(Seq(path), kv)

  def updateProps(path: String, kv: (String, String)*): Props = updateProps(Seq(path), kv: _*)

  def updateProp(path: Seq[String], kv: (String, String)): Props = updateProps(path, kv)

  def updateProps(path: Seq[String], kv: (String, String)*) = traversalUpdate(path, props, kv: _*)

  private def traversalUpdate(path: Seq[String], _props: Props, kv: (String, String)*): Props = {
    path match {
      case Nil ⇒ _props ++ kv
      case _ ⇒
        val head = path.head
        _props + (head → traversalUpdate(path.tail, get[Props](_props, head), kv: _*))
    }
  }
}

trait SourceProps extends PropsAdapter

trait SinkProps extends PropsAdapter

trait PublisherProps extends SourceProps

trait SubscriberProps extends SinkProps {
  val NS_SUBSCRIBE = "subscriber"
  private val _subscribeProps: Props = get[Props](props,NS_SUBSCRIBE)

  def highWatermark = getIntOrElse(_subscribeProps, "highWatermark", 20)
}

trait PageRollerProps extends SourceProps {
  private val NS_PAGE_ROLLER = "pageRoller"
  private val _pageRollerProp = get[Props](props, NS_PAGE_ROLLER)
  val page = getIntOrElse(_pageRollerProp, "page", 1)
  val pageSize = getIntOpt(_pageRollerProp, "pageSize")
  val maxPage = getIntOrElse(_pageRollerProp, "maxPage", Int.MaxValue)
  val offset = getIntOrElse(_pageRollerProp, "offset", Int.MaxValue)

  def page(page: Int): Props = updateProps(NS_PAGE_ROLLER, "page" → page.toString, "offset" → (page * pageSize.get).toString)

  def pageSize(pageSize: Int): Props = updateProp(NS_PAGE_ROLLER, "pageSize" → pageSize.toString)

  def maxPage(maxPage: Int): Props = updateProp(NS_PAGE_ROLLER, "maxPage" → maxPage.toString)
}

class PageRollerPropsImpl(override val props: Props) extends PageRollerProps

object PageRollerProps extends ConfProps[PageRollerProps] {
  implicit def toProps(props: Props): PageRollerProps = new PageRollerPropsImpl(props: Props)
}

/**
 * deadline support:
 * now
 * fromNow
 * 1.minutes.fromNow
 * 2011-12-03T10:15:30Z
 */
trait TimeRollerProps extends SourceProps {
  val NS_TIME_ROLLER = "timeRoller"
  val _timerRollerPage = get[Props](props, NS_TIME_ROLLER)
  val deadline = getStringOpt(_timerRollerPage, "deadline")
  val start = getStringOpt(_timerRollerPage, "start")
  val period = getStringOpt(_timerRollerPage, "period")
  val maxPeriod = getStringOpt(_timerRollerPage, "maxPeriod")

  def deadline(deadline: String): Props = updateProp(NS_TIME_ROLLER, "deadline" → deadline)

  def start(start: LocalDateTime): Props = updateProp(NS_TIME_ROLLER, "start" → start.format(DEFAULT_DATE_FORMATTER))

  def end(start: LocalDateTime): Props = updateProp(NS_TIME_ROLLER, "end" → start.format(DEFAULT_DATE_FORMATTER))

  def period(period: Duration): Props = updateProp(NS_TIME_ROLLER, "period" → period.toString)

  def maxPeriod(maxPeriod: Duration): Props = updateProp(NS_TIME_ROLLER, "maxPeriod" → maxPeriod.toString)
}

class TimePollerPropsImpl(override val props: Props) extends TimeRollerProps

object TimePollerProps extends ConfProps[TimeRollerProps] {
  implicit def toProps(props: Props): TimeRollerProps = new TimePollerPropsImpl(props)
}

trait TransactionProps extends SourceProps {
  val NS_TRANSACTION = "transaction"
  val _transaction = get[Props](props, NS_TRANSACTION)
  val channelSize = getIntOrElse(_transaction, "channelSize", 1)
  val batchSize = getIntOrElse(_transaction, "batchSize", 500)
  val maxAge = Duration(getStringOrElse(_transaction, "maxAge", "1.minutes"))
  val maxCacheSize = getIntOrElse(_transaction, "maxCacheSize", 100000)
}

class TransactionPropsImpl(override val props: Props) extends TransactionProps

object TransactionProps extends ConfProps[TransactionProps] {
  implicit def toProps(props: Props): TransactionProps = new TransactionPropsImpl(props)
}

trait KafkaPublisherProps extends PublisherProps {
  val topic = getStringOpt(cmpProps, "topic").get
}

trait KafkaSubscriberProps extends SubscriberProps

trait DataSourceSourceProps extends PageRollerProps with TimeRollerProps with TransactionProps with PublisherProps {
  def sql = getStringOpt(cmpProps, "sql").get
}

trait DataSourceSinkProps extends SubscriberProps

class DataSourceSourcePropsConversionsImpl(override val props: Props) extends DataSourceSourceProps

class DataSourceSinkPropsConversionsImpl(override val props: Props) extends DataSourceSinkProps

object DataSourceSourcePropsConversions extends ConfProps[DataSourceSourceProps] {
  implicit def toProps(props: Props): DataSourceSourceProps = new DataSourceSourcePropsConversionsImpl(props)
}

object DataSourceSinkPropsConversions extends ConfProps[DataSourceSinkProps] {
  implicit def toProps(props: Props): DataSourceSinkProps = new DataSourceSinkPropsConversionsImpl(props)
}

class KafkaPublisherPropsImpl(override val props: Props) extends KafkaPublisherProps

class KafkaSubscriberPropsImpl(override val props: Props) extends KafkaSubscriberProps

object KafkaPublisherPropsConversions extends ConfProps[KafkaPublisherProps] {
  implicit def toProps(props: Props): KafkaPublisherProps = new KafkaPublisherPropsImpl(props)
}

object KafkaSubscriberPropsConversions extends ConfProps[KafkaSubscriberProps] {
  implicit def toProps(props: Props): KafkaSubscriberProps = new KafkaSubscriberPropsImpl(props)
}

