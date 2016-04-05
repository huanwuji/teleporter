package teleporter.integration.core

import akka.actor.ActorRef
import teleporter.integration.CmpType

import scala.collection.concurrent.TrieMap

/**
 * date 2015/8/3.
 * @author daikui
 */
object ActorAddress

trait ActorAddresses {
  val idMapper = TrieMap[(Int,CmpType), ActorRef]()

  def register(id: Int, actorRef: ActorRef, cmpType: CmpType): ActorAddresses = {
    idMapper += ((id, cmpType) → actorRef)
    this
  }

  def exists(id: Int, cmpType: CmpType): Boolean = idMapper.contains((id, cmpType))

  def apply(id: Int, cmpType: CmpType): ActorRef = {
    idMapper((id, cmpType))
  }
}

object ActorAddresses extends ActorAddresses {
  def apply(): ActorAddresses = new ActorAddresses {}
}