package teleporter.integration.utils

import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{ExecutionContext, Future}

/**
  * Author: kui.dai
  * Date: 2015/11/26.
  */
trait Use extends Logging {

  type Closable = AutoCloseable

  def using[R <: Closable, A](resource: R)(f: R ⇒ A): A = {
    try {
      f(resource)
    } finally {
      quietClose(resource)
    }
  }

  def quietClose[R <: Closable](resource: R): Unit = {
    try {
      resource.close()
    } catch {
      case e: Exception ⇒ logger.error(e.getLocalizedMessage, e)
    }
  }

  def futureUsing[R <: Closable, A](resource: R)(f: R => Future[A])(implicit ec: ExecutionContext): Future[A] = {
    f(resource) andThen { case _ => resource.close() }
  }
}