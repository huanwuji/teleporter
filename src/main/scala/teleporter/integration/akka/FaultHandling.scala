package teleporter.integration.akka

import akka.actor.SupervisorStrategy.{Decider, Restart, Stop}
import akka.actor._
import org.apache.logging.log4j.scala.Logging

/**
  * Author: kui.dai
  * Date: 2016/3/30.
  */
trait FaultHandling {

}

object SupervisorStrategy extends Logging {
  final val defaultDecider: Decider = {
    case e ⇒
      logger.info(e.getLocalizedMessage, e)
      e match {
        case _: ActorInitializationException ⇒ Stop
        case _: ActorKilledException ⇒ Stop
        case _: DeathPactException ⇒ Stop
        case _: Exception ⇒ Restart
      }
  }

  final val defaultStrategy: SupervisorStrategy = {
    OneForOneStrategy()(defaultDecider)
  }
}

final class DefaultSupervisorStrategy extends SupervisorStrategyConfigurator {
  override def create(): SupervisorStrategy = SupervisorStrategy.defaultStrategy
}
