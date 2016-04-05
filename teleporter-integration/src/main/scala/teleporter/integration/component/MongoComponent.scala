package teleporter.integration.component


import com.typesafe.scalalogging.LazyLogging
import org.mongodb.scala.{MongoClient, MongoDatabase}
import teleporter.integration.conf.Conf
import teleporter.integration.core.{Address, AddressBuilder, TeleporterCenter}

/**
 * Created by joker on 15/12/07.
 */
class MongoAddressBuilder(override val conf: Conf.Address)(implicit override val teleporterCenter: TeleporterCenter) extends AddressBuilder[MongoDatabase] with LazyLogging {
  override def build: Address[MongoDatabase] = {
    new Address[MongoDatabase] {
      val mongoClient = MongoClient(conf.props.get("url").toString)
      override val client: MongoDatabase = mongoClient.getDatabase(conf.props.get("database").toString)
      override val _conf: Conf.Address = conf

      override def close(): Unit = mongoClient.close()
    }
  }
}

class MongoComponent

//object MongoComponentFactory{
//
//  def build():MongoClient={
//    val config = ConfigFactory.load()
//    val mongoConfig = config.getConfig("teleporter.mongo")
//    MongoClient(mongoConfig.getString("url"))
//  }
//  def close (mongoClient: MongoClient)={
//   if(null!=mongoClient)
//     {
//       mongoClient.close()
//     }
//    def getDataBase(mongoClient: MongoClient,database: String):MongoDatabase={
//      if(null!=mongoClient)
//        {
//          mongoClient.getDatabase(database)
//        }
//      null
//
//    }
//
//  }
//}


