package teleporter.integration.core

import java.time.LocalDateTime

import org.scalatest.FunSuite
import teleporter.integration.component.Rollers
import teleporter.integration.conf.DataSourceSourcePropsConversions
import teleporter.integration.utils.Dates

import scala.concurrent.duration.Duration

/**
 * Author: kui.dai
 * Date: 2015/12/4.
 */
class RollersTest extends FunSuite {
  test("pageSize") {
    import DataSourceSourcePropsConversions._
    var i = 1
    val props = Map[String,Any]().page(1).pageSize(10).maxPage(10)
    val roller = Rollers(props, {
      source ⇒
        i += 1
        println(source.props)
        if (i < 5) {
          Iterator(1, 2)
        } else {
          Iterator.empty
        }
    })
    roller.iterator.foreach(println)
  }
  test("maxPageSize") {
    import DataSourceSourcePropsConversions._
    val props = Map[String,Any]().page(1).pageSize(10).maxPage(10)
    val roller = Rollers(props, {
      source ⇒
        println(source.props)
        Iterator(1, 2)
    })
    roller.iterator.foreach(println)
  }
  test("RollerTime now") {
    import DataSourceSourcePropsConversions._
    val props = Map[String,Any]().start(LocalDateTime.now().minusMinutes(1)).deadline("now").period(Duration("10s"))
    val roller = Rollers(props, {
      source ⇒
        println(source.props)
        Iterator(1, 2)
    })
    roller.iterator.foreach(println)
  }
  test("RollerTime fromNow") {
    import DataSourceSourcePropsConversions._
    val period = Duration("10s")
    val props = Map[String,Any]().start(LocalDateTime.now().minusMinutes(1)).deadline("fromNow").period(period)
    val roller = Rollers(props, {
      source ⇒
        println(source.props)
        Iterator(1, 2)
    })
    val iterator = roller.iterator
    iterator.foreach(println)
    println("--------------")
    iterator.foreach(println)
    println("--------------")
    Thread.sleep(period.toMillis)
    iterator.foreach(println)
  }
  test("RollerTime 1.min.fromNow") {
    import DataSourceSourcePropsConversions._
    val period = Duration("10s")
    val props = Map[String,Any]().start(LocalDateTime.now().minusMinutes(2)).deadline("1.min.fromNow").period(period)
    val roller = Rollers(props, {
      source ⇒
        println(source.props)
        Iterator(1, 2)
    })
    val iterator = roller.iterator
    iterator.foreach(println)
    println("--------------")
    iterator.foreach(println)
    println("--------------")
    Thread.sleep(period.toMillis)
    iterator.foreach(println)
  }
  test("RollerTime fixed time") {
    import DataSourceSourcePropsConversions._
    val period = Duration("10s")
    val deadline = Dates.DEFAULT_DATE_FORMATTER.format(LocalDateTime.now())
    println(deadline)
    val props = Map[String,Any]().start(LocalDateTime.now().minusMinutes(2)).deadline(deadline).period(period)
    val roller = Rollers(props, {
      source ⇒
        println(source.props)
        Iterator(1, 2)
    })
    val iterator = roller.iterator
    iterator.foreach(println)
    println("--------------")
    iterator.foreach(println)
    println("--------------")
    Thread.sleep(period.toMillis)
    iterator.foreach(println)
  }
  test("rollers page and time") {
    import DataSourceSourcePropsConversions._
    val period = Duration("10s")
    val props = Map[String,Any]().start(LocalDateTime.now().minusSeconds(20)).deadline("fromNow").period(period).page(1).maxPage(5).pageSize(10)
    val roller = Rollers(props, {
      source ⇒
        println(source.props)
        Iterator(1, 2)
    })
    val iterator = roller.iterator
    iterator.foreach(println)
    println("--------------")
    iterator.foreach(println)
    println("--------------")
    Thread.sleep(period.toMillis)
    iterator.foreach(println)
  }
}