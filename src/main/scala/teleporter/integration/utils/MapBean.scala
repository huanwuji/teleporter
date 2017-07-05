package teleporter.integration.utils

import scala.reflect.{ClassTag, classTag}

/**
  * Author: kui.dai
  * Date: 2016/6/27.
  */
trait MapBean extends Converters with Convert {
  val underlying: Map[String, Any]

  def get[A](paths: String*)(implicit converter: Converter[A]): Option[A] = {
    val v = paths match {
      case head :: Nil ⇒ getValues(underlying, head)
      case _ ⇒ getValues(underlying, paths)
    }
    v.flatMap(toOption[A])
  }

  def gets[A](paths: String*)(implicit converter: Converter[A]): Seq[A] = {
    val v = paths match {
      case head :: Nil ⇒ getValues(underlying, head)
      case _ ⇒ getValues(underlying, paths)
    }
    v.flatMap(x ⇒ toOption[Seq[Any]](x).map(_.map(toOption[A])).map(_.flatten)).getOrElse(Seq.empty)
  }

  def apply[A](paths: String*)(implicit converter: Converter[A]): A = {
    this.get[A](paths: _*) match {
      case None ⇒ throw new NoSuchElementException(s"Path not found: $paths")
      case Some(value) ⇒ value
    }
  }

  def ++(kv: (String, Any)*): Map[String, Any] = underlying ++ kv

  def ++(path: Seq[String], kv: (String, Any)*): Map[String, Any] = traversalUpdate(underlying, path, kv: _*)

  def ++(paths: String, kv: (String, Any)*): Map[String, Any] = traversalUpdate(underlying, paths.split("\\."), kv: _*)

  def toMap: Map[String, Any] = underlying

  protected def traversalUpdate(bean: Map[String, Any], path: Seq[String], kv: (String, Any)*): Map[String, Any] = {
    path match {
      case Nil ⇒ bean ++ kv
      case _ ⇒
        val head = path.head
        bean + (head → traversalUpdate(bean.getOrElse(head, Map[String, Any]()).asInstanceOf[Map[String, Any]], path.tail, kv: _*))
    }
  }

  private def getValues(data: Map[String, Any], paths: String): Option[Any] = {
    if (paths.contains(".")) {
      getValues(data, paths.split("\\."))
    } else {
      data.get(paths)
    }
  }

  private def getValues(data: Map[String, Any], paths: Seq[String]): Option[Any] = {
    paths match {
      case ps if ps.length == 1 ⇒ data.get(paths.head)
      case ps ⇒
        data.get(paths.head).flatMap {
          v ⇒ getValues(v.asInstanceOf[Map[String, Any]], ps.drop(1))
        }
    }
  }

  def mapTo[T <: MapMetaBean : ClassTag]: T = {
    classTag[T].runtimeClass.getConstructor(classOf[Map[String, Any]]).newInstance(underlying).asInstanceOf[T]
  }

  override def toString: String = underlying.toString()
}

class MapBeanImpl(val underlying: Map[String, Any]) extends MapBean

trait MapMetaBean extends MapBean {

  def get[A](paths: this.type ⇒ Seq[String])(implicit converter: Converter[A]): Option[A] = {
    get[A](paths(this): _*)
  }

  def gets[A](paths: this.type ⇒ Seq[String])(implicit converter: Converter[A]): Seq[A] = {
    gets[A](paths(this): _*)
  }

  def apply[A](paths: this.type ⇒ Seq[String])(implicit converter: Converter[A]): A = {
    this.get[A](paths) match {
      case None ⇒ throw new NoSuchElementException(s"Path not found: $paths")
      case Some(value) ⇒ value
    }
  }

  def ++(kv: this.type ⇒ Seq[(String, Any)]): this.type = newSelf(underlying ++ kv(this))

  def ++(path: this.type ⇒ Seq[String], kv: this.type ⇒ Seq[(String, Any)]): this.type = {
    newSelf(traversalUpdate(underlying, path(this), kv(this): _*))
  }

  private def newSelf(map: Map[String, Any]): this.type = {
    this.getClass.getConstructor(classOf[Map[String, Any]]).newInstance(map).asInstanceOf[this.type]
  }
}

object MapMetaBean {
  def apply[T <: MapBean : ClassTag](map: Map[String, Any]): T = {
    classTag[T].runtimeClass.getConstructor(classOf[Map[String, Any]]).newInstance(map)
      .asInstanceOf[T]
  }

  def apply[T <: MapBean : ClassTag](bean: MapBean): T = apply(bean.toMap)
}

object MapBean extends Converters {
  val empty = MapBean(Map())

  implicit def apply(bean: Map[String, Any]): MapBean = new MapBeanImpl(bean)
}