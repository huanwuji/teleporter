package teleporter.integration.component.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.util.concurrent.TimeUnit
import javax.sql.DataSource

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import teleporter.integration.utils.{CaseFormats, Use}

import scala.util.matching.Regex

/**
  * Author: kui.dai
  * Date: 2015/11/25.
  */
sealed trait Action

case class Upsert(up: Sql, sert: Sql) extends Action

case class Update(sql: Sql) extends Action

sealed trait Sql

case class NameSql(sql: String, binds: Map[String, Any] = Map.empty) extends Sql {
  def toPreparedSql: PreparedSql = PreparedSql(this)
}

case class PreparedSql(sql: String, params: Seq[Any] = Seq.empty) extends Sql

object PreparedSql {
  val paramRegex: Regex = "#\\{.+?\\}".r
  val paramGroupRegex: Regex = "#\\{(.+?)\\}".r

  case class PredefinedSql(sql: String, paramNames: Seq[String])

  lazy val preparedSqlCache: LoadingCache[String, PredefinedSql] = CacheBuilder.newBuilder()
    .maximumSize(1000).expireAfterWrite(1, TimeUnit.HOURS).build(new CacheLoader[String, PredefinedSql]() {
    override def load(nameSql: String): PredefinedSql = {
      val preparedParams = paramGroupRegex.findAllMatchIn(nameSql).map(_.group(1)).toIndexedSeq
      val preparedSql = paramRegex.replaceAllIn(nameSql, "?")
      PredefinedSql(preparedSql, preparedParams)
    }
  })

  /**
    * #{} replace ? for prepareStatement, {} only for text replace
    * inert into table (id,name) values(#{id},#{name}) limit {offset},{pageSize}
    */
  def apply(nameSql: NameSql): PreparedSql = {
    val predefinedSql = preparedSqlCache.get(nameSql.sql)
    val params = predefinedSql.paramNames.map {
      name ⇒
        nameSql.binds.applyOrElse(name, null) match {
          case opt: Option[Any] ⇒ opt.orNull
          case v ⇒ v
        }
    }
    val sql = Template(predefinedSql.sql, nameSql.binds)
    PreparedSql(sql, params)
  }
}

case class SqlResult[T](conn: Connection, ps: Statement, rs: ResultSet, result: T) {
  def close(): Unit = {
  }
}

trait SqlSupport extends Use {
  def paramsDefined(col: String): String = s"#{$col}"

  def nameColumns(traversableOnce: TraversableOnce[String]): String = traversableOnce.map(paramsDefined).mkString(",")

  def nameColumnsSet(traversableOnce: TraversableOnce[String]): String = traversableOnce.map(col ⇒ s"$col=${paramsDefined(col)}").mkString(",")

  def doAction(ds: DataSource, action: Action): Unit = action match {
    case Update(sql) ⇒ update(ds.getConnection, sql)
    case Upsert(up, sert) ⇒ if (update(ds.getConnection, up) == 0) update(ds.getConnection, sert)
  }

  def insertSql(tableName: String, data: Map[String, Any]): Sql = {
    val keys = data.keys
    NameSql( s"""insert into $tableName (${keys.mkString(",")}) values (${nameColumns(keys)})""", data)
  }

  def insertIgnoreSql(tableName: String, data: Map[String, Any]): Sql = {
    val keys = data.keys
    NameSql( s"""insert ignore into $tableName (${keys.mkString(",")}) values (${nameColumns(keys)})""", data)
  }

  def updateSql(tableName: String, primaryKey: String, data: Map[String, Any]): Sql = {
    val keys = data.keys.filterNot(_ == primaryKey)
    NameSql( s"""update $tableName set ${nameColumnsSet(keys)} where $primaryKey=${paramsDefined(primaryKey)}""", data)
  }

  def updateSql(tableName: String, primaryKey: String, version: String, data: Map[String, Any]): Sql = {
    val keys = data.keys.filterNot(_ == primaryKey)
    NameSql( s"""update $tableName set ${nameColumnsSet(keys)} where $primaryKey=${paramsDefined(primaryKey)} and $version > ${paramsDefined(version)}""", data)
  }

  def updateSql(tableName: String, primaryKeys: Seq[String], version: Option[String] = None, data: Map[String, Any]): Sql = {
    val keys = data.keySet -- primaryKeys
    val keysFilter = primaryKeys.map(keys ⇒ s"$keys=${paramsDefined(keys)}").mkString(" and ")
    val versionSql = version.map(v ⇒ s"and $version<${paramsDefined(v)}").getOrElse("")
    NameSql( s"""update $tableName set ${nameColumnsSet(keys)} where $keysFilter $versionSql""", data)
  }

  def update(conn: Connection, sql: Sql): Int =
    sql match {
      case nameSql: NameSql ⇒ update(conn, nameSql)
      case preparedSql: PreparedSql ⇒ update(conn, preparedSql)
    }

  def update(conn: Connection, nameSql: NameSql): Int = {
    update(conn, PreparedSql(nameSql))
  }

  def update(conn: Connection, preparedSql: PreparedSql): Int =
    using(conn) {
      _conn ⇒
        using(_conn.prepareStatement(preparedSql.sql)) {
          ps ⇒
            conn.prepareStatement(preparedSql.sql)
            val params = preparedSql.params
            var i = 1
            for (param ← params) {
              ps.setObject(i, param)
              i += 1
            }
            ps.executeUpdate()
        }
    }

  def bulkQueryToMap(conn: Connection, preparedSql: PreparedSql): SqlResult[Iterator[Map[String, Any]]] = {
    bulkQuery(conn, preparedSql)(ResultSetMapper.MapResultSetMapper)
  }

  def bulkQuery[T](conn: Connection, preparedSql: PreparedSql)(implicit resultSetMapper: ResultSetMapper[T]): SqlResult[Iterator[T]] = {
    var ps: PreparedStatement = null
    var rs: ResultSet = null
    try {
      ps = conn.prepareStatement(preparedSql.sql)
      val params = preparedSql.params
      var i = 1
      for (param ← params) {
        ps.setObject(i, param)
        i += 1
      }
      logger.info(s"bulk query sql: $preparedSql")
      rs = ps.executeQuery()
      SqlResult[Iterator[T]](
        conn = conn,
        ps = ps,
        rs = rs,
        result = new Iterator[T] {
          var isTakeOut = true
          var _next = false

          override def hasNext: Boolean =
            if (isTakeOut) {
              _next = rs.next()
              isTakeOut = false
              if (!_next) SqlSupport.closeQuietly(conn, ps, rs)
              _next
            } else {
              _next
            }

          override def next(): T = {
            isTakeOut = true
            resultSetMapper.to(rs)
          }
        })
    } catch {
      case e: Exception ⇒
        logger.error(e.getLocalizedMessage, e)
        SqlSupport.closeQuietly(conn, ps, rs)
        throw e
    }
  }

  def one[T](conn: Connection, preparedSql: PreparedSql)(implicit resultSetMapper: ResultSetMapper[T]): Option[T] = query(conn, preparedSql).headOption

  def query[T](conn: Connection, preparedSql: PreparedSql)(implicit resultSetMapper: ResultSetMapper[T]): Iterable[T] = {
    using(conn) {
      _ ⇒
        using(conn.prepareStatement(preparedSql.sql)) {
          ps ⇒
            conn.prepareStatement(preparedSql.sql)
            val params = preparedSql.params
            var i = 1
            for (param ← params) {
              ps.setObject(i, param)
              i += 1
            }
            using(ps.executeQuery()) {
              rs ⇒
                new Iterator[T] {
                  override def hasNext: Boolean = rs.next()

                  override def next(): T = resultSetMapper.to(rs)
                }.toIndexedSeq
            }
        }
    }
  }

  def queryToMap(conn: Connection, preparedSql: PreparedSql): Iterable[Map[String, Any]] = {
    query(conn, preparedSql)(ResultSetMapper.MapResultSetMapper)
  }
}

trait ResultSetMapper[T] {
  def to(rs: ResultSet): T
}

object ResultSetMapper {

  implicit object MapResultSetMapper extends ResultSetMapper[Map[String, Any]] {
    override def to(rs: ResultSet): Map[String, Any] = {
      val metaData = rs.getMetaData
      (1 to rs.getMetaData.getColumnCount).foldLeft(Map.newBuilder[String, Any]) { (b, i) ⇒
        val label = metaData.getColumnLabel(i)
        b += (label → rs.getObject(i))
      }.result()
    }
  }

  implicit object SnakeMapResultSetMapper extends ResultSetMapper[Map[String, Any]] {
    override def to(rs: ResultSet): Map[String, Any] = {
      val metaData = rs.getMetaData
      (1 to rs.getMetaData.getColumnCount).foldLeft(Map.newBuilder[String, Any]) { (b, i) ⇒
        val label = metaData.getColumnLabel(i)
        b += (CaseFormats.UNDERSCORE_SNAKE.convert(label) → rs.getObject(i))
      }.result()
    }
  }

}

object SqlSupport extends SqlSupport {
  def closeQuietly(conn: Connection, stat: Statement, rs: ResultSet): Unit = {
    try {
      if (rs != null) rs.close()
    } finally {
      try {
        if (stat != null) stat.close()
      } finally {
        try {
          if (conn != null) conn.close()
        } finally {
          conn.close()
        }
      }
    }
  }
}

object Template {
  val expressionParamGroupRegex: Regex = "\\{(.+?)\\}".r

  def apply(template: String, params: Map[String, Any]): String = {
    expressionParamGroupRegex.replaceAllIn(template, mh ⇒ params(mh.group(1)).toString)
  }
}