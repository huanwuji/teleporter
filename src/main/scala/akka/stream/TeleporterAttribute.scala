package akka.stream

import akka.stream.Attributes.Attribute
import teleporter.integration.core.{ConfigMetaBean, TeleporterCenter}
import teleporter.integration.supervision.DecideRule

/**
  * Created by huanwuji 
  * date 2016/12/30.
  */
object TeleporterAttribute {

  val emptySupervisionStrategy: SupervisionStrategy = SupervisionStrategy(Seq.empty)

  trait SupervisionStrategy extends Attribute {
    def rules: Seq[DecideRule]
  }

  class SupervisionStrategyImpl(val rules: Seq[DecideRule]) extends SupervisionStrategy

  case class TeleporterSupervisionStrategy(key: String, rules: Seq[DecideRule], center: TeleporterCenter) extends SupervisionStrategy

  object SupervisionStrategy {
    def apply(rules: Seq[DecideRule]): SupervisionStrategy = {
      new SupervisionStrategyImpl(rules)
    }

    def apply(key: String, config: ConfigMetaBean)(implicit center: TeleporterCenter): SupervisionStrategy = {
      TeleporterSupervisionStrategy(key, DecideRule(key, config), center)
    }
  }

  case class Dispatcher(dispatcher: String) extends Attribute

  val CacheDispatcher: Dispatcher = Dispatcher("akka.teleporter.cache-dispatcher")

  val BlockingDispatcher: Dispatcher = Dispatcher("akka.teleporter.blocking-dispatcher")

  val IODispatcher: Dispatcher = Dispatcher("akka.teleporter.io-dispatcher")
}