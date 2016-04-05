package teleporter.web

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.{`Access-Control-Allow-Methods`, `Access-Control-Allow-Origin`}
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization.write
import org.mongodb.scala._
import org.mongodb.scala.model.Filters._

import scala.concurrent.Future

/**
 * date 2015/8/3.
 * @author daikui
 */
object Fields {
  val id = "id"
  val name = "name"
}

object Main extends App with LazyLogging {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val dispatcher = system.dispatcher

  val config = ConfigFactory.load().getConfig("teleporter.web")
  val mongoConfig = config.getConfig("mongo")
  val bindConfig = config.getConfig("bind")
  val client: MongoClient = MongoClient(mongoConfig.getString("url"))
  implicit val db: MongoDatabase = client.getDatabase(mongoConfig.getString("database"))

  import MongoSupport._

  implicit val formats = DefaultFormats

  val route = logRequestResult("teleporter-web") {
    pathPrefix("static") {
      getFromResourceDirectory("")
    } ~
      pathPrefix("conf" / Segment) {
        module ⇒
          val col = db.getCollection(module)
          path(IntNumber) {
            id ⇒
              (get  & parameter('action ! "delete")) {
              complete(col.deleteOne(equal(Fields.id, id)).toFuture().map(_ ⇒ StatusCodes.OK))
            } ~
                get {
                  complete(col.find(equal(Fields.id, id)).map(_.toJson()).toFuture().map(_.head))
                } ~
                (post & entity(as[String])) {
               json ⇒ complete(col.replaceOne(equal(Fields.id, id),Document(json)).toFuture().map(_ ⇒ StatusCodes.OK))
            }
          } ~
            get {
              parameters('page.as[Int], 'pageSize.as[Int], 'search.as[String].?("{}")) {
                (page, pageSize, search) ⇒ complete(col.find(Document(search)).skip((page - 1) * pageSize).limit(pageSize).map(_.toJson()).toFuture().map(seqToJson))
              } ~ {
                complete(col.find().map(_.toJson()).toFuture().map(seqToJson))
              }
            } ~ (post & entity(as[String])) {
                json ⇒
                val bean = parse(json)
                val name = (bean \ "name").extract[String]
                complete {
                   generateId(col, name).map{
                    id ⇒ bean.asInstanceOf[JObject].values+ ("id" →id)
                   }.map {
                     map ⇒
                     val resultJson = write(map)
                     col.insertOne(Document(resultJson)).toFuture().map(_ ⇒ resultJson)
                   }
                }
              }
      }
  }

  private def generateId(col: MongoCollection[Document], name: String, factor: Int = 1): Future[Int] = {
    val hashCode = Math.abs(generateHashCode(name, factor))
    col.find(equal(Fields.id, hashCode)).toFuture().flatMap {
      case Nil ⇒ Future.successful(hashCode)
      case x ⇒
        if (name == x.head("name").asString().getValue) {
          Future.failed(new IllegalArgumentException(s"$name is exists"))
        } else {
          generateId(col, name, factor + 1)
        }
    }
  }

  private def generateHashCode(name: String, factor: Int = 1): Int = (1 to factor).map(_ ⇒ name).mkString("teleporter").hashCode

  Http().bindAndHandle(interface = bindConfig.getString("host"), port = bindConfig.getInt("port"), handler = route.map {
    resp ⇒ resp.copy(headers = resp.headers
      :+ `Access-Control-Allow-Origin`.*
      :+ `Access-Control-Allow-Methods`(GET, POST, PUT, DELETE, PATCH))
  })
}