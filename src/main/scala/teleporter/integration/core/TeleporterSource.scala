package teleporter.integration.core

import akka.actor.{ActorRef, Props}
import akka.stream.scaladsl.Source
import com.taobao.api.domain.Trade
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.component.jdbc.JdbcPublisher
import teleporter.integration.component.mongo.MongoPublisher
import teleporter.integration.component.taobao.TaobaoApiPublisher
import teleporter.integration.component.taobao.api.TradeApi
import teleporter.integration.component.{KafkaPublisher, ShadowPublisher}

import scala.collection.concurrent.TrieMap

/**
  * date 2015/8/3.
  *
  * @author daikui
  */
trait TeleporterSource extends LazyLogging {
  var sourceApplies: TrieMap[String, (String, TeleporterCenter) ⇒ Props] = TrieMap[String, (String, TeleporterCenter) ⇒ Props](
    "kafka" → { (key, center) ⇒ Props(classOf[KafkaPublisher], key, center) },
    "jdbc" → { (key, center) ⇒ Props(classOf[JdbcPublisher], key, center).withDispatcher("akka.teleporter.blocking-io-dispatcher") },
    "mongo" → { (key, center) ⇒ Props(classOf[MongoPublisher], key, center) },
    "taobao.trades.sold.get" → { (key, center) ⇒ Props(classOf[TaobaoApiPublisher[Trade]], key, TradeApi.`taobao.trades.sold.get`, center).withDispatcher("akka.teleporter.blocking-io-dispatcher") }
  )

  def apply[T](id: Long)(implicit center: TeleporterCenter): Source[T, ActorRef] = apply[T](center.context.getContext[SourceContext](id).key)

  def apply[T](key: String)(implicit center: TeleporterCenter): Source[T, ActorRef] = {
    val system = center.system
    val context = center.context.getContext[SourceContext](key)
    val config = context.config
    val sourceProps = sourceApplies(config.category)(key, center)
    val props = if (config.shadow) {
      Props(classOf[ShadowPublisher], key, system.actorOf(sourceProps), center)
    } else {
      sourceProps
    }
    Source.actorPublisher[T](props).mapMaterializedValue { ref ⇒
      center.context.indexes.modifyByKey2(key, _.asInstanceOf[SourceContext].copy(actorRef = ref)); ref
    }
  }

  def registerType(category: String, apply: (String, TeleporterCenter) ⇒ Props): Unit = {
    sourceApplies += category → apply
  }
}

class TeleporterSourceImpl extends TeleporterSource

object TeleporterSource {
  def apply(): TeleporterSource = new TeleporterSourceImpl
}