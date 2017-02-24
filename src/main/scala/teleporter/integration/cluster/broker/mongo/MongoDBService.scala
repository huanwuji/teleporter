package teleporter.integration.cluster.broker.mongo

import akka.Done
import org.apache.logging.log4j.scala.Logging
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model._
import org.mongodb.scala.{Document, MongoCollection, MongoWriteException}
import teleporter.integration.cluster.broker.PersistentProtocol.KeyValue
import teleporter.integration.cluster.broker.PersistentService

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

/**
  * Created by kui.dai on 2016/7/15.
  */
class MongoDBService(collection: MongoCollection[Document])(implicit ec: ExecutionContext) extends PersistentService with Logging {
  val timeout: FiniteDuration = 1.minutes
  val keyField = "_id"
  val valueField = "value"

  override def id(): Long =
    Await.result(collection.findOneAndUpdate(equal(keyField, "id"), inc(valueField, 1L), FindOneAndUpdateOptions().upsert(true)).map(_ (valueField).asInt64().longValue())
      .toFuture().map(_.head), timeout)

  override def range(key: String, start: Int, limit: Int): Seq[KeyValue] =
    Await.result(collection.find(regex(keyField, key)).skip(start).limit(limit)
      .map(doc ⇒ KeyValue(doc(keyField).asString().getValue, doc(valueField).asString().getValue)).toFuture(), timeout)

  override def apply(key: String): KeyValue = get(key) match {
    case None ⇒ throw new NoSuchElementException("key not found: " + key)
    case Some(v) ⇒ v
  }

  override def get(key: String): Option[KeyValue] =
    Await.result(collection.find(equal(keyField, key))
      .map(doc ⇒ KeyValue(doc(keyField).asString().getValue, doc(valueField).asString().getValue)).toFuture().map(_.headOption), timeout)

  override def unsafePut(key: String, value: String): Unit =
    Await.result(collection.updateOne(equal(keyField, key), set(valueField, value), UpdateOptions().upsert(true))
      .toFuture().map(_ ⇒ Done), timeout)

  override def delete(key: String): Unit =
    Await.result(collection.deleteOne(equal(keyField, key)).toFuture().map(_ ⇒ Done), timeout)

  override def unsafeAtomicPut(key: String, expect: String, update: String): Boolean =
    try {
      Await.result(collection.updateOne(Document(keyField → key, valueField → expect), set(valueField, update), UpdateOptions().upsert(true))
        .toFuture().map { results ⇒
        results.size == 1 && results.headOption.exists(r ⇒ r.getMatchedCount == 1 || r.getModifiedCount == 1 || r.getUpsertedId != null)
      }, timeout)
    } catch {
      case e: MongoWriteException ⇒
        logger.warn(s"AtomicPut expect value not match, ${e.getMessage}")
        false
    }
}

object MongoDBService {
  def apply(collection: MongoCollection[Document])(implicit ec: ExecutionContext): MongoDBService = new MongoDBService(collection)
}