//package teleporter.stream.integration.transaction
//
//import com.github.mauricio.async.db.mysql.MySQLConnection
//import com.github.mauricio.async.db.{Configuration, Connection}
//
//import scala.concurrent.ExecutionContext.Implicits.global
//import scala.concurrent.duration._
//import scala.concurrent.{Await, Future}
//
///**
//  * Created by huanwuji on 2016/10/31.
//  */
//object AsyncMysql extends App {
//  val connection: Connection = new MySQLConnection(Configuration(port = 3306, username = "root", password = Some("root"), database = Some("ct")))
//
//  Await.result(connection.connect, 5 seconds)
//
//  val start = System.currentTimeMillis()
//  for (i ← 1 to 100000) {
//    val future = connection.sendPreparedStatement("insert into shop_info values(?,?)", Array(Math.random(), "test"))
//    val mapResult: Future[Any] = future.map(queryResult => queryResult.rows match {
//      case Some(resultSet) => {
////        println(resultSet)
//      }
//      case None => -1
//    }
//    )
//    val result = Await.result(mapResult, 5 seconds)
////    println(result)
//  }
//  println(System.currentTimeMillis() - start)
////  connection.disconnect
//}