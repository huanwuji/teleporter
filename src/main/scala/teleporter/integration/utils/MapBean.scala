package teleporter.integration.utils

/**
  * Author: kui.dai
  * Date: 2016/6/27.
  */
trait MapBean extends Converters with Convert {
  val underlying: Map[String, Any]

  def __dict__[A](paths: String*)(implicit converter: Converter[A]): Option[A] = {
    val v = paths match {
      case head :: Nil ⇒ getValues(underlying, head)
      case _ ⇒ getValues(underlying, paths)
    }
    v.flatMap(toOption[A])
  }

  def __dicts__[A](paths: String*)(implicit converter: Converter[A]): Seq[A] = {
    val v = paths match {
      case head :: Nil ⇒ getValues(underlying, head)
      case _ ⇒ getValues(underlying, paths)
    }
    v.flatMap(x ⇒ toOption[Seq[Any]](x).map(_.map(toOption[A])).map(_.flatten)).getOrElse(Seq.empty)
  }

  def apply[A](paths: String*)(implicit converter: Converter[A]): A = {
    this.__dict__[A](paths: _*).get
  }

  def ++(kv: (String, Any)*): Map[String, Any] = underlying ++ kv

  def ++(path: Seq[String], kv: (String, Any)*): Map[String, Any] = traversalUpdate(underlying, path, kv: _*)

  def ++(paths: String, kv: (String, Any)*): Map[String, Any] = traversalUpdate(underlying, paths.split("\\."), kv: _*)

  private val _upperCaseReg = """[A-Z]""".r

  def camel2Point: Map[String, Any] = underlying.foldLeft(Map[String, Any]()) {
    (m, entry) ⇒ m + (_upperCaseReg.replaceAllIn(entry._1, "." + _.toString().toLowerCase) → entry._2)
  }

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

  override def toString: String = underlying.toString()
}

class MapBeanImpl(val underlying: Map[String, Any]) extends MapBean

trait MapMetadata

object MapBean extends Converters {
  val empty = MapBean(Map())

  implicit def apply(bean: Map[String, Any]): MapBean = new MapBeanImpl(bean)
}