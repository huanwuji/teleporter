package teleporter.integration.conf

import java.time.LocalDateTime

import teleporter.integration.conf.Conf.Props
import teleporter.integration.utils.{CronFixed, Dates}

import scala.collection.immutable.Map
import scala.concurrent.duration._

/**
 * Author: kui.dai
 * Date: 2015/11/26.
 */
trait BasicConversions {
  def asInt(v: Any): Option[Int] = v match {
    case s: String if s.isEmpty ⇒ None
    case s: String ⇒ Some(s.toInt)
    case i: Int ⇒ Some(i)
    case d: Double => Some(d.toInt)
    case bi: BigInt ⇒ Some(bi.toInt)
    case _ ⇒ throw new IllegalArgumentException(s"No match int type convert for type:${v.getClass}, value:$v")
  }

  def asLong(v: Any): Option[Long] = v match {
    case s: String if s.isEmpty ⇒ None
    case s: String ⇒ Some(s.toLong)
    case bi: BigInt ⇒ Some(bi.toLong)
    case d: Double => Some(d.toLong)
    case l: Long ⇒ Some(l)
    case _ ⇒ throw new IllegalArgumentException(s"No match long type convert for type:${v.getClass}, value:$v")
  }

  def asString(v: Any): String = v.toString

  def asNonEmptyString(v: Any): Option[String] = v match {
    case s: String if s.isEmpty ⇒ None
    case x ⇒ Some(x.toString)
  }

  def asDuration(v: Any): Option[Duration] = v match {
    case s: String if s.isEmpty ⇒ None
    case s: String ⇒ Some(Duration(s))
    case _ ⇒ throw new IllegalArgumentException(s"No match duration type convert for type:${v.getClass}, value:$v")
  }

  def asBoolean(v: Any): Option[Boolean] = v match {
    case s: String if s.isEmpty ⇒ None
    case s: String ⇒ Some(s.toBoolean)
    case b: Boolean ⇒ Some(b)
    case jb: java.lang.Boolean ⇒ Some(jb)
    case _ ⇒ throw new IllegalArgumentException(s"No match boolean type convert for type:${v.getClass}, value:$v")
  }
}

trait PropsSupport extends BasicConversions {
  val COMPONENT_NS = "cmp"

  def get[T](props: Props, key: String): T = props(key).asInstanceOf[T]

  def getOrElse[T](props: Props, key: String, default: T): T = props.getOrElse(key, default).asInstanceOf[T]

  def getStringOpt(props: Props, key: String): Option[String] = props.get(key).map(asString)

  def getStringOrElse(props: Props, key: String, default: String): String = getStringOpt(props, key).getOrElse(default)

  def getNonEmptyStringOpt(props: Props, key: String): Option[String] = props.get(key).flatMap(asNonEmptyString)

  def getNonEmptyStringOrElse(props: Props, key: String, default: String): String = getStringOpt(props, key).getOrElse(default)

  def getDuration(props: Props, key: String): Option[Duration] = props.get(key).flatMap(asDuration)

  def getDurationOrElse(props: Props, key: String, default: Duration): Duration = getDuration(props, key).getOrElse(default)

  def getIntOpt(props: Props, key: String): Option[Int] = props.get(key).flatMap(asInt)

  def getIntOrElse(props: Props, key: String, default: Int): Int = getIntOpt(props, key).getOrElse(default)

  def getLongOpt(props: Props, key: String): Option[Long] = props.get(key).flatMap(asLong)

  def getLongOrElse(props: Props, key: String, default: Long): Long = getLongOpt(props, key).getOrElse(default)

  def getBooleanOpt(props: Props, key: String): Option[Boolean] = props.get(key).flatMap(asBoolean)

  def getBooleanOrElse(props: Props, key: String, default: Boolean): Boolean = getBooleanOpt(props, key).getOrElse(default)

  def ns(props: Props, namespace: String): Props = props.filter(_._1.startsWith(namespace))

  def cmpProps(props: Props): Props = get[Props](props, COMPONENT_NS)

  private val _upperCaseReg = """[A-Z]""".r

  def camel2Point(props: Props): Props = props.foldLeft(Map[String,Any]()) {
    (m, entry) ⇒ m + (_upperCaseReg.replaceAllIn(entry._1, "." + _.toString().toLowerCase) → entry._2)
  }
}

object PropsSupport extends PropsSupport

trait ConfProps[T]

trait PropsAdapter extends PropsSupport {
  val NS_CMP = "cmp"
  val props: Props
  val cmpProps = getOrElse[Props](props, NS_CMP, Conf.PROPS_EMPTY)

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
        _props + (head → traversalUpdate(path.tail, getOrElse[Props](_props, head, Conf.props()), kv: _*))
    }
  }
}

trait SourceProps extends PropsAdapter {
  val errorRules = getNonEmptyStringOpt(cmpProps, "errorRules")
}

class SourcePropsImpl(override val props: Props) extends SourceProps

object SourceProps extends ConfProps[SourceProps] {
  implicit def toProps(props: Props): SourceProps = new SourcePropsImpl(props)
}

trait SinkProps extends PropsAdapter {
  val errorRules = getNonEmptyStringOpt(cmpProps, "errorRules")
}

class SinkPropsImpl(override val props: Props) extends SinkProps

object SinkProps extends ConfProps[SinkProps] {
  implicit def toProps(props: Props): SinkProps = new SinkPropsImpl(props)
}

trait PublisherProps extends SourceProps

trait CronProps extends PropsAdapter {
  val cron = getNonEmptyStringOpt(cmpProps, "cron").map(CronFixed.fixed)
}

trait SubscriberProps extends SinkProps {
  val NS_SUBSCRIBE = "subscriber"
  private val _subscribeProps: Props = getOrElse[Props](props, NS_SUBSCRIBE, Conf.PROPS_EMPTY)
  val highWatermark = getIntOrElse(_subscribeProps, "highWatermark", 20)
  val parallelism = getIntOrElse(_subscribeProps, "parallelism", 1)
}

class SubscriberPropsImpl(override val props: Props) extends SubscriberProps

object SubscriberProps extends ConfProps[SubscriberProps] {
  implicit def toProps(props: Props): SubscriberProps = new SubscriberPropsImpl(props)
}

trait PageRollerProps extends PublisherProps {
  val page = getIntOrElse(cmpProps, "page", 1)
  val pageSize = getIntOpt(cmpProps, "pageSize")
  val maxPage = getIntOrElse(cmpProps, "maxPage", Int.MaxValue)
  val offset = getIntOrElse(cmpProps, "offset", 0)

  def page(page: Int): Props = updateProps(NS_CMP, "page" → page.toString, "offset" → (page * pageSize.get).toString)

  def pageSize(pageSize: Int): Props = updateProp(NS_CMP, "pageSize" → pageSize.toString)

  def maxPage(maxPage: Int): Props = updateProp(NS_CMP, "maxPage" → maxPage.toString)

  def isPageRoller = pageSize.isDefined
}

class PageRollerPropsImpl(override val props: Props) extends PageRollerProps

object PageRollerProps extends ConfProps[PageRollerProps] {
  implicit def toProps(props: Props): PageRollerProps = {
    val pageRollerProps = new PageRollerPropsImpl(props: Props)
    //make offset is consistent with pageNo
    if (pageRollerProps.offset == (pageRollerProps.page * pageRollerProps.pageSize.get)) {
      pageRollerProps
    } else {
      new PageRollerPropsImpl(pageRollerProps.page(pageRollerProps.page))
    }
  }
}

/**
 * deadline support: now, fromNow, 1.minutes.fromNow, 2011-12-03T10:15:30Z
 */
trait TimeRollerProps extends PublisherProps {
  val deadline = getStringOpt(cmpProps, "deadline")
  val start = getStringOpt(cmpProps, "start")
  val end = getStringOpt(cmpProps, "end")
  val period = getStringOpt(cmpProps, "period")
  val maxPeriod = getStringOpt(cmpProps, "maxPeriod")

  def deadline(deadline: String): Props = updateProp(NS_CMP, "deadline" → deadline)

  def start(start: LocalDateTime): Props = updateProp(NS_CMP, "start" → start.format(Dates.DEFAULT_DATE_FORMATTER))

  def end(start: LocalDateTime): Props = updateProp(NS_CMP, "end" → start.format(Dates.DEFAULT_DATE_FORMATTER))

  def period(period: Duration): Props = updateProp(NS_CMP, "period" → period.toString)

  def maxPeriod(maxPeriod: Duration): Props = updateProp(NS_CMP, "maxPeriod" → maxPeriod.toString)

  def isContinuous: Boolean = deadline.exists(_.contains("fromNow"))

  def isTimerRoller = period.isDefined
}

class TimePollerPropsImpl(override val props: Props) extends TimeRollerProps

object TimePollerProps extends ConfProps[TimeRollerProps] {
  implicit def toProps(props: Props): TimeRollerProps = new TimePollerPropsImpl(props)
}

trait RollerProps extends PageRollerProps with TimeRollerProps

class RollerPropsImpl(override val props: Props) extends RollerProps

object RollerProps extends ConfProps[RollerProps] {
  implicit def toProps(props: Props): RollerProps = new RollerPropsImpl(props)
}

trait ScheduleProps extends CronProps with RollerProps

class SchedulePropsImpl(override val props: Props) extends ScheduleProps

object ScheduleProps extends ConfProps[ScheduleProps] {
  implicit def toProps(props: Props): ScheduleProps = new SchedulePropsImpl(props)
}

trait TransactionProps extends SourceProps {
  val NS_TRANSACTION = "transaction"
  val _transaction = getOrElse[Props](props, NS_TRANSACTION, Conf.PROPS_EMPTY)
  val recoveryPointEnabled = getBooleanOrElse(props, "recoveryPointEnabled", default = true)
  val channelSize = getIntOrElse(_transaction, "channelSize", 1)
  val batchSize = getIntOrElse(_transaction, "batchSize", 10000)
  val commitDelay: Option[Duration] = getDuration(_transaction, "commitDelay")
  val maxAge = getDurationOrElse(_transaction, "maxAge", 2.minutes)
  val timeoutRetry = getBooleanOrElse(_transaction, "timeoutRetry", default = true)
  val maxCacheSize = getIntOrElse(_transaction, "maxCacheSize", 100000)
}

class TransactionPropsImpl(override val props: Props) extends TransactionProps

object TransactionProps extends ConfProps[TransactionProps] {
  implicit def toProps(props: Props): TransactionProps = new TransactionPropsImpl(props)
}

trait DataSourceSourceProps extends PageRollerProps with TimeRollerProps with TransactionProps with PublisherProps {
  def sql = getStringOpt(cmpProps, "sql").get
}

class DataSourceSourcePropsConversionsImpl(override val props: Props) extends DataSourceSourceProps

object DataSourceSourcePropsConversions extends ConfProps[DataSourceSourceProps] {
  implicit def toProps(props: Props): DataSourceSourceProps = new DataSourceSourcePropsConversionsImpl(props)
}

trait DataSourceSinkProps extends SubscriberProps

class DataSourceSinkPropsConversionsImpl(override val props: Props) extends DataSourceSinkProps

object DataSourceSinkPropsConversions extends ConfProps[DataSourceSinkProps] {
  implicit def toProps(props: Props): DataSourceSinkProps = new DataSourceSinkPropsConversionsImpl(props)
}

trait KafkaPublisherProps extends PublisherProps {
  val topics = getStringOpt(cmpProps, "topics")
}

class KafkaPublisherPropsImpl(override val props: Props) extends KafkaPublisherProps

object KafkaPublisherPropsConversions extends ConfProps[KafkaPublisherProps] {
  implicit def toProps(props: Props): KafkaPublisherProps = new KafkaPublisherPropsImpl(props)
}

trait KafkaSubscriberProps extends SubscriberProps

class KafkaSubscriberPropsImpl(override val props: Props) extends KafkaSubscriberProps

object KafkaSubscriberPropsConversions extends ConfProps[KafkaSubscriberProps] {
  implicit def toProps(props: Props): KafkaSubscriberProps = new KafkaSubscriberPropsImpl(props)
}

trait MongoSourceProps extends PageRollerProps with TimeRollerProps with TransactionProps with PublisherProps {
  val database = getStringOpt(cmpProps, "database").get
  val collection = getStringOpt(cmpProps, "collection").get
  val query = getStringOpt(cmpProps, "query")
}

class MongoSourcePropsImpl(override val props: Props) extends MongoSourceProps

object MongoSourcePropsConversions extends ConfProps[MongoSourceProps] {
  implicit def toProps(props: Props): MongoSourceProps = new MongoSourcePropsImpl(props)
}

trait StreamProps extends PropsAdapter

class StreamPropsImpl(override val props: Props) extends StreamProps

object StreamPropsConversions extends ConfProps[StreamProps] {
  implicit def toProps(props: Props): StreamProps = new StreamPropsImpl(props)
}