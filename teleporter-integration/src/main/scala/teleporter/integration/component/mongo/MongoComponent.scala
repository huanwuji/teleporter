package teleporter.integration.component.mongo

import com.typesafe.scalalogging.LazyLogging
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.{Document, MongoClient}
import teleporter.integration.component.{MongoMessage, SchedulePublisher}
import teleporter.integration.conf.Conf.Props
import teleporter.integration.conf.{Conf, MongoSourceProps, PropsSupport}
import teleporter.integration.core.{Address, AddressBuilder, AutoCloseAddress, TeleporterCenter}
import teleporter.integration.script.Template

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Created by joker on 15/12/07
 */
class MongoAddressBuilder(override val conf: Conf.Address)(implicit override val center: TeleporterCenter)
  extends AddressBuilder[MongoClient] with PropsSupport with LazyLogging {
  override def build: Address[MongoClient] = {
    val cmpConfig = cmpProps(conf.props)
    val mongoClient = MongoClient(getStringOpt(cmpConfig, "url").get)
    new AutoCloseAddress[MongoClient](conf, mongoClient)
  }
}

class MongoPublisher(override val id: Int)(implicit override val center: TeleporterCenter)
  extends SchedulePublisher[MongoMessage, MongoClient] with PropsSupport {

  import teleporter.integration.conf.MongoSourcePropsConversions._

  val mongoConf: MongoSourceProps = currConf.props
  val database = address.getDatabase(mongoConf.database)
  val collection = database.getCollection(mongoConf.collection)

  override def handler(props: Props): Iterator[MongoMessage] = {
    val rollerProps: MongoSourceProps = props
    val filter = props.query.map { s ⇒
      if (s.nonEmpty) Document(Template(s, props)) else Document()
    }.getOrElse(BsonDocument())
    val query = collection.find(filter)
    if (scheduleProps.isPageRoller) {
      query.skip(rollerProps.offset).limit(rollerProps.pageSize.get)
    }
    Await.result(query.toFuture(), 5.minutes).toIterator
  }
}

object MongoComponent