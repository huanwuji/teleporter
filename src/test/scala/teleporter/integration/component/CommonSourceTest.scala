package teleporter.integration.component

import java.time.LocalDateTime
import java.util.concurrent.locks.LockSupport

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Attributes, TeleporterAttributes}
import teleporter.integration.component.Roller._
import teleporter.integration.component.jdbc.SqlSupport
import teleporter.integration.supervision.DecideRule

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

/**
  * Created by huanwuji 
  * date 2017/1/18.
  */
class TestCommonSource extends CommonSource[Int, Int]("CommonSource") {
  override protected def initialAttributes: Attributes = super.initialAttributes and TeleporterAttributes.CacheDispatcher

  override def create(): Int = {
    println("create client")
    1
  }

  override def readData(client: Int): Option[Int] = {
    throw new RuntimeException
    Some(1)
  }

  override def close(client: Int): Unit = {
    println("close client")
  }
}

class TestCommonRollerSource(rollerContext: RollerContext)
  extends RollerSource[Int, Int]("test.common.source", rollerContext) with SqlSupport {

  override protected def initialAttributes: Attributes = super.initialAttributes and TeleporterAttributes.BlockingDispatcher

  override def readData(client: Int, rollerContext: RollerContext): Option[Int] = {
    println(rollerContext, rollerContext.timeline.map(_.deadline()).getOrElse(""))
    LockSupport.parkNanos(1e8.toLong)
    Some(Random.nextInt())
  }

  override def create(): Int = {
    println("create client")
    1
  }

  override def close(client: Int): Unit = {
    println("close client")
  }
}

class TestSourceAsync extends CommonSourceAsync[Int, Int]("TestSourceAsync") with SqlSupport {

  override def readData(client: Int, executionContext: ExecutionContext): Future[Option[Int]] = {
    implicit val ec = executionContext
    Future.successful(Some(Random.nextInt()))
  }

  override def create(executionContext: ExecutionContext): Future[Int] = {
    println("create client")
    Future.successful(1)
  }

  override def close(client: Int, executionContext: ExecutionContext): Future[Done] = {
    println("close client")
    Future.successful(Done)
  }
}

class TestRollerSourceAsync(rollerContext: RollerContext)
  extends RollerSourceAsync[Int, Int]("TestRollerSourceAsync", rollerContext) {

  override def readData(client: Int, rollerContext: RollerContext, executionContext: ExecutionContext): Future[Option[Int]] = {
    implicit val ec = executionContext
    println(rollerContext, rollerContext.timeline.map(_.deadline()).getOrElse(""))
    LockSupport.parkNanos(1e8.toLong)
    Future.successful(Some(Random.nextInt()))
  }

  override def create(executionContext: ExecutionContext): Future[Int] = {
    println("create client")
    Future.successful(1)
  }

  override def close(client: Int, executionContext: ExecutionContext): Future[Done] = {
    println("close client")
    Future.successful(Done)
  }
}

object CommonSourceTest extends App {
  implicit val system = ActorSystem()
  implicit val mater = ActorMaterializer()

  testCommonRollerSource()

  def testCommonSource()(implicit system: ActorSystem, mater: ActorMaterializer): Unit = {
    Source.fromGraph(new TestCommonSource)
      .addAttributes(Attributes(TeleporterAttributes
        .SupervisionStrategy(Seq(DecideRule(".*", "INFO", "reload(delay=5000.millis, retries=5, next=stop)")))))
      .to(Sink.foreach(println)).run()
  }

  def testCommonSourceAsync()(implicit system: ActorSystem, mater: ActorMaterializer): Unit = {
    Source.fromGraph(new TestSourceAsync)
      .addAttributes(Attributes(TeleporterAttributes
        .SupervisionStrategy(Seq(DecideRule(".*", "INFO", "reload(delay=5000.millis, retries=5, next=stop)")))))
      .to(Sink.foreach(println)).run()
  }

  def testCommonRollerSource()(implicit system: ActorSystem, mater: ActorMaterializer): Unit = {
    val now = LocalDateTime.now()
    val rollerContext = RollerContext(
      pagination =
        //        None,
        Some(Pagination(1, 10, 2)),
      timeline =
        //        None,
        Some(Timeline(
          start = LocalDateTime.now().minusSeconds(75),
          end = null,
          period = 10.seconds,
          maxPeriod = 20.seconds,
          deadline = () ⇒ now
        )),
      forever = false,
      state = Roller.Paging
    )
    Source.fromGraph(new TestCommonRollerSource(rollerContext.nextTimeline().copy(state = Paging)))
      .addAttributes(Attributes(TeleporterAttributes
        .SupervisionStrategy(Seq(DecideRule(".*", "INFO", "reload(delay=5000.millis, retries=5, next=stop)")))))
      .to(Sink.foreach(println)).run()
  }

  def testRollerSourceAsync()(implicit system: ActorSystem, mater: ActorMaterializer): Unit = {
    val now = LocalDateTime.now()
    val rollerContext = RollerContext(
      pagination =
        None,
      //      Some(Pagination(1, 10, 2)),
      timeline =
        //        None,
        Some(Timeline(
          start = LocalDateTime.now().minusSeconds(75),
          end = null,
          period = 10.seconds,
          maxPeriod = 20.seconds,
          deadline = () ⇒ LocalDateTime.now()
        )),
      forever = true,
      state = Roller.Timing
    )
    Source.fromGraph(new TestRollerSourceAsync(rollerContext.nextTimeline().copy(state = Roller.Timing)))
      .addAttributes(Attributes(TeleporterAttributes
        .SupervisionStrategy(Seq(DecideRule(".*", "INFO", "reload(delay=5000.millis, retries=5, next=stop)")))))
      .to(Sink.foreach(println)).run()
  }
}