package teleporter.integration.component

import scala.concurrent.{ExecutionContext, Future}

/**
 * Author: kui.dai
 * Date: 2015/12/10.
 */
trait Entity[I] {
  def id: I

  def name: String
}

trait Repository[T, I, C] {
  def get(id: I)(implicit client: C, m: Manifest[T], ex: ExecutionContext): T = throw new UnsupportedOperationException("Not support this method")

  def getOption(id: I)(implicit client: C, m: Manifest[T], ex: ExecutionContext): Option[T] = throw new UnsupportedOperationException("Not support this method")

  def findByName(id: String)(implicit client: C, m: Manifest[T], ex: ExecutionContext): T = throw new UnsupportedOperationException("Not support this method")

  def findByNameOption(id: String)(implicit client: C, m: Manifest[T], ex: ExecutionContext): Option[T] = throw new UnsupportedOperationException("Not support this method")

  def matchName(name: String)(implicit client: C, m: Manifest[T], ex: ExecutionContext): Seq[T] = throw new UnsupportedOperationException("Not support this method")

  def findByTask(taskId: Int)(implicit client: C, m: Manifest[T], ex: ExecutionContext): Seq[T] = throw new UnsupportedOperationException("Not support this method")

  def save(dto: T)(implicit client: C, m: Manifest[T], ex: ExecutionContext): Int = throw new UnsupportedOperationException("Not support this method")

  def modify(id: I, dto: Map[_, _])(implicit client: C, ex: ExecutionContext): Int = throw new UnsupportedOperationException("Not support this method")

  def insert(dto: T)(implicit client: C, ex: ExecutionContext): Int = throw new UnsupportedOperationException("Not support this method")

  def update(dto: T)(implicit client: C, ex: ExecutionContext): Int = throw new UnsupportedOperationException("Not support this method")
}

trait AsyncRepository[T, I, C] {
  def get(id: I)(implicit client: C, m: Manifest[T], ex: ExecutionContext): Future[T] = throw new UnsupportedOperationException("Not support this method")

  def getOption(id: I)(implicit client: C, m: Manifest[T], ex: ExecutionContext): Option[Future[T]] = throw new UnsupportedOperationException("Not support this method")

  def findByName(id: String)(implicit client: C, m: Manifest[T], ex: ExecutionContext): Future[T] = throw new UnsupportedOperationException("Not support this method")

  def findByNameOption(id: String)(implicit client: C, m: Manifest[T], ex: ExecutionContext): Option[Future[T]] = throw new UnsupportedOperationException("Not support this method")

  def save(dto: T)(implicit client: C, ex: ExecutionContext): Future[Int]

  def modify(id: I, dto: Map[_, _])(implicit client: C, ex: ExecutionContext): Future[Int] = throw new UnsupportedOperationException("Not support this method")

  def insert(dto: T)(implicit client: C, ex: ExecutionContext): Future[Int] = throw new UnsupportedOperationException("Not support this method")
}