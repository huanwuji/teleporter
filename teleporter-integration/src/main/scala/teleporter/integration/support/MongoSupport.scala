package teleporter.integration.support

import com.typesafe.scalalogging.LazyLogging
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization._
import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import teleporter.integration.component.mongo.Json2Bson
import teleporter.integration.component.{Entity, Repository}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

/**
 * Author: kui.dai
 * Date: 2015/11/18.
 */
trait MongoSupport[T <: Entity[I], I] extends Repository[T, I, MongoDatabase] with Json2Bson with LazyLogging {
  implicit val formats = Serialization.formats(NoTypeHints)
  val colName: String
  val primaryKey = "id"
  val (nameField, taskIdField) = ("name", "taskId")

  override def get(id: I)(implicit client: MongoDatabase, m: Manifest[T], ex: ExecutionContext): T = {
    getOption(id).get
  }

  override def getOption(id: I)(implicit client: MongoDatabase, m: Manifest[T], ex: ExecutionContext): Option[T] =
    Await.result(client.getCollection(colName).find(equal(primaryKey, id))
      .toFuture(), 1.minutes).map(_.toJson()).map { x ⇒ logger.info(x); x }.map(read[T]).headOption

  override def findByName(name: String)(implicit client: MongoDatabase, m: Manifest[T], ex: ExecutionContext): T = {
    findByNameOption(name).get
  }

  override def matchName(matches: String)(implicit client: MongoDatabase, m: Manifest[T], ex: ExecutionContext): Seq[T] =
    Await.result(client.getCollection(colName).find(regex(taskIdField, matches))
      .toFuture(), 1.minutes).map(_.toJson()).map { x ⇒ logger.info(x); x }.map(read[T])

  override def findByNameOption(name: String)(implicit client: MongoDatabase, m: Manifest[T], ex: ExecutionContext): Option[T] =
    Await.result(client.getCollection(colName).find(equal(nameField, name))
      .toFuture(), 1.minutes).map(_.toJson()).map { x ⇒ logger.info(x); x }.map(read[T]).headOption

  override def findByTask(taskId: Int)(implicit client: MongoDatabase, m: Manifest[T], ex: ExecutionContext): Seq[T] =
    Await.result(client.getCollection(colName).find(equal(taskIdField, taskId))
      .toFuture(), 1.minutes).map(_.toJson()).map { x ⇒ logger.info(x); x }.map(read[T])

  override def save(bean: T)(implicit client: MongoDatabase, m: Manifest[T], ex: ExecutionContext): Int =
    getOption(bean.id) match {
      case Some(x) ⇒ update(bean)
      case None ⇒ insert(bean)
    }

  override def modify(id: I, values: Map[_, _])(implicit client: MongoDatabase, ex: ExecutionContext): Int = {
    Await.result(client.getCollection(colName).updateOne(equal(primaryKey, id), Document("$set" → Document(write(values))))
      .toFuture(), 10.seconds).foreach(r ⇒ logger.info(r.toString))
    1
  }

  override def insert(bean: T)(implicit client: MongoDatabase, ex: ExecutionContext): Int = {
    Await.result(client.getCollection(colName).insertOne(Document(write(bean)))
      .toFuture(), 10.seconds).foreach(r ⇒ logger.info(r.toString()))
    1
  }

  override def update(bean: T)(implicit client: MongoDatabase, ex: ExecutionContext): Int = {
    Await.result(client.getCollection(colName).replaceOne(equal(primaryKey, bean.id), Document(write(bean)))
      .toFuture(), 10.seconds).foreach(r ⇒ logger.info(r.toString))
    1
  }
}