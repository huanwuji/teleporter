package teleporter.stream.integration.transaction

import akka.actor.ActorSystem
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.Attributes.Attribute
import akka.stream._
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.util.control.NonFatal

/**
  * Created by huanwuji on 2016/11/30.
  */
final case class Map[In, Out](f: In ⇒ Out) extends GraphStage[FlowShape[In, Out]] {
  val in = Inlet[In]("Map.in")
  val out = Outlet[Out]("Map.out")
  override val shape = FlowShape(in, out)

  override def initialAttributes: Attributes = DefaultAttributes.map

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private def decider =
        inheritedAttributes.get[SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)

      override def onPush(): Unit = {
        try {
          push(out, f(grab(in)))
        } catch {
          case NonFatal(ex) ⇒ decider(ex) match {
            case Supervision.Stop ⇒ failStage(ex)
            case _ ⇒ pull(in)
          }
        }
      }

      override def onPull(): Unit = pull(in)

      setHandlers(in, out, this)
    }
}

object KillSwitchTest extends App {
  implicit val system = ActorSystem()
  implicit val mater = ActorMaterializer()

  case class UserAttribute(value: String) extends Attribute

  Source.single(1).via(Map(_ + 1)).addAttributes(Attributes(UserAttribute("aaa"))).via(Map(_ + 1)).runForeach(println)
}