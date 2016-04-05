package teleporter.integration.core

import akka.actor.ActorRef

import scala.collection.concurrent.TrieMap

/**
 * date 2015/8/3.
 * @author daikui
 */
case class ActorAddress(persistenceId: Int, actorRef: ActorRef)

trait ActorAddresses {
  val persistenceIdMapper = TrieMap[Int, ActorAddress]()

  def register(id: Int, actorRef: ActorRef): ActorAddresses = {
    val address = ActorAddress(id, actorRef)
    persistenceIdMapper += (id → address)
    this
  }

  def apply(persistenceId: Int): ActorAddress = {
    persistenceIdMapper(persistenceId)
  }
}

object ActorAddresses extends ActorAddresses {
  def apply(): ActorAddresses = new ActorAddresses {}
}