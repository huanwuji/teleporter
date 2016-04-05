package teleporter.task.tests

import java.sql.ResultSet
import javax.sql.DataSource

import akka.actor.{Actor, ActorSystem, Props}
import akka.event.LoggingAdapter
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor.{ActorPublisher, ActorSubscriber, OneByOneRequestStrategy, RequestStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.logging.LogFactory

import scala.annotation.tailrec

/**
  * Author: kui.dai
  * Date: 2016/1/18.
  */
trait Resources {
  def using[A <: AutoCloseable, B](resource: A)(handler: A ⇒ B): B = {
    try {
      handler(resource)
    } finally {
      resource.close()
    }
  }
}

trait IdEntity {
  val id: Long
}

case class User(id: Long, name: String, sex: Int, age: Int, mobile: String) extends IdEntity

object User {
  def apply(rs: ResultSet): User = {
    User(
      id = rs.getLong("id"),
      name = rs.getString("name"),
      sex = rs.getInt("sex"),
      age = rs.getInt("age"),
      mobile = rs.getString("mobile")
    )
  }
}

trait JdbcTemplate[T <: IdEntity] extends Resources {
  type BeanMapper = ResultSet ⇒ T
  val log: LoggingAdapter
  val tableName: String
  val beanMapper: BeanMapper

  def find(id: String)(implicit dataSource: DataSource): Seq[T] = query(s"select * from $tableName where id=$id", beanMapper)

  def query[A](sql: String, mapper: ResultSet ⇒ A)(implicit dataSource: DataSource): Seq[A] =
    using(dataSource.getConnection) {
      conn ⇒ using(conn.prepareStatement(sql)) {
        ps ⇒ using(ps.executeQuery()) {
          rs ⇒ new Iterator[A] {
            override def hasNext: Boolean = rs.next()

            override def next(): A = mapper(rs)
          }.toIndexedSeq
        }
      }
    }
}

trait Pager[A <: IdEntity] {
  template: JdbcTemplate[A] ⇒
  def pageQuery(start: Int, pageSize: Int, pageSql: (Int, Int) ⇒ String)(implicit dataSource: DataSource): Seq[A] =
    query(pageSql(start, pageSize), beanMapper)
}

trait UserService extends JdbcTemplate[User] with Pager[User] {
  override val tableName: String = "user"
  override val beanMapper: BeanMapper = User(_)
}

class UserServiceImpl()(implicit val log: LoggingAdapter) extends UserService

object UserService {
  def apply()(implicit log: LoggingAdapter) = new UserServiceImpl()
}

trait JsonProtocol {
  val objectMapper = new ObjectMapper() with ScalaObjectMapper registerModule DefaultScalaModule

  implicit def userToJson(user: User): String = objectMapper.writeValueAsString(user)
}

object Main extends App with JsonProtocol {
  implicit val system = ActorSystem()
  implicit val mater = ActorMaterializer()
  implicit val logAdapter = system.log
  implicit val dataSource = new HikariDataSource()
  dataSource.setJdbcUrl("jdbc:mysql://172.18.2.154:3306/test")
  dataSource.setUsername("admin")
  dataSource.setPassword("admin123")
  val userService = UserService()
  val data = Iterator.from(0, 10).flatMap(userService.pageQuery(_, 100, (pageNum, pageSize) ⇒ s"select * from user limit $pageNum,$pageSize"))
  val toJson = Flow[User].map(user => user.toUpperCase)
  Source(1 to 10).runWith(Sink.foreachParallel(4){
    x => println(x)
  })
  Source.actorPublisher(Props(classOf[Publisher], data)) via toJson runWith Sink.actorSubscriber(Props[Subscriber])
}

class Publisher(data: Iterator[String]) extends ActorPublisher[String] {
  override def receive: Receive = {
    case Request(n) ⇒ delivery(n)
  }

  @tailrec
  final def delivery(n: Long): Unit = {
    if (n > 0) {
      if (data.hasNext) {
        onNext(data.next())
        delivery(n - 1)
      } else {
        onCompleteThenStop()
      }
    }
  }
}

class Subscriber extends ActorSubscriber {
  override protected def requestStrategy: RequestStrategy = OneByOneRequestStrategy

  override def receive: Actor.Receive = {
    case OnNext(ele) ⇒ println(ele)
  }
}