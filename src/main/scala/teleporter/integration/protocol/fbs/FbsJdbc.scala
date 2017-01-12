package teleporter.integration.protocol.fbs

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp, Types}

import com.google.flatbuffers.FlatBufferBuilder
import teleporter.integration.component.jdbc._
import teleporter.integration.core.{TId, TransferMessage}
import teleporter.integration.protocol.fbs.generate._

/**
  * Author: kui.dai
  * Date: 2016/4/8.
  */
object FbsJdbcParam {

  import teleporter.integration.utils.Bytes._

  def apply(builder: FlatBufferBuilder, value: Any): Int = {
    value match {
      case Some(v) ⇒ build(builder, v)
      case None ⇒ JdbcParam.createJdbcParam(builder, Types.NULL, JdbcParam.createValueVector(builder, Array.emptyByteArray))
      case x ⇒ build(builder, x)
    }
  }

  private def build(builder: FlatBufferBuilder, value: Any): Int = {
    value match {
      case v: Boolean ⇒ JdbcParam.createJdbcParam(builder, Types.BOOLEAN, JdbcParam.createValueVector(builder, v))
      case v: Char ⇒ JdbcParam.createJdbcParam(builder, Types.CHAR, JdbcParam.createValueVector(builder, v))
      case v: Short ⇒ JdbcParam.createJdbcParam(builder, Types.SMALLINT, JdbcParam.createValueVector(builder, v))
      case v: Int ⇒ JdbcParam.createJdbcParam(builder, Types.INTEGER, JdbcParam.createValueVector(builder, v))
      case v: Long ⇒ JdbcParam.createJdbcParam(builder, Types.BIGINT, JdbcParam.createValueVector(builder, v))
      case v: Float ⇒ JdbcParam.createJdbcParam(builder, Types.FLOAT, JdbcParam.createValueVector(builder, v))
      case v: Double ⇒ JdbcParam.createJdbcParam(builder, Types.DOUBLE, JdbcParam.createValueVector(builder, v))
      case v: BigDecimal ⇒ JdbcParam.createJdbcParam(builder, Types.DOUBLE, JdbcParam.createValueVector(builder, v))
      case v: java.math.BigDecimal ⇒ JdbcParam.createJdbcParam(builder, Types.DOUBLE, JdbcParam.createValueVector(builder, toBytes(v)))
      case v: Date ⇒ JdbcParam.createJdbcParam(builder, Types.DATE, JdbcParam.createValueVector(builder, v.toString))
      case v: Timestamp ⇒ JdbcParam.createJdbcParam(builder, Types.TIMESTAMP, JdbcParam.createValueVector(builder, v.toString))
      case v: String ⇒ JdbcParam.createJdbcParam(builder, Types.VARCHAR, JdbcParam.createValueVector(builder, v))
      case v: Array[Byte] ⇒ JdbcParam.createJdbcParam(builder, Types.BLOB, JdbcParam.createValueVector(builder, v))
      case null ⇒ JdbcParam.createJdbcParam(builder, Types.NULL, JdbcParam.createValueVector(builder, Array.emptyByteArray))
      case v ⇒ throw new IllegalArgumentException(s"Unsupport data type $v, ${v.getClass.getSimpleName}")
    }
  }

  def unapply(statement: JdbcStatement): Seq[Any] = scala.collection.Seq.tabulate(statement.paramsLength())(statement.params).map(FbsJdbcParam.unapply)

  def unapply(field: JdbcParam): Any = {
    val b = Array.tabulate(field.valueLength())(field.value)
    field.`type`() match {
      case Types.NULL ⇒ null
      case Types.BOOLEAN ⇒ toBoolean(b)
      case Types.CHAR ⇒ toChar(b)
      case Types.SMALLINT ⇒ toShort(b)
      case Types.INTEGER ⇒ toInt(b)
      case Types.BIGINT ⇒ toLong(b)
      case Types.FLOAT ⇒ toFloat(b)
      case Types.DOUBLE ⇒ toDouble(b)
      case Types.DATE ⇒ Date.valueOf(b)
      case Types.TIMESTAMP ⇒ Timestamp.valueOf(b)
      case Types.VARCHAR ⇒ toStr(b)
      case Types.BLOB ⇒ b
      case _ ⇒ throw new IllegalArgumentException(s"Unsupport type:${field.`type`()}")
    }
  }
}

object FbsJdbc {
  implicit def asPreparedSql(statement: JdbcStatement): PreparedSql = PreparedSql(statement.sql(), FbsJdbcParam.unapply(statement))

  private def apply(action: JdbcAction): Action = {
    action.`type`() match {
      case ActionType.Update ⇒
        Update(action.statements(0))
      case ActionType.Upsert ⇒
        Upsert(action.statements(0), action.statements(1))
    }
  }

  def apply(message: generate.JdbcMessage): TransferMessage[Seq[Action]] = {
    val tId = TId.keyFromBytes(Array.tabulate(message.tidLength())(message.tid))
    val jdbcRecord = scala.collection.immutable.Seq.tabulate(message.actionsLength())(message.actions).map(apply)
    TransferMessage[Seq[Action]](id = tId, data = jdbcRecord)
  }

  def apply(byteBuffer: ByteBuffer): Seq[TransferMessage[Seq[Action]]] = {
    val messages = JdbcMessages.getRootAsJdbcMessages(byteBuffer)
    scala.collection.immutable.Seq.tabulate(messages.messagesLength())(messages.messages).map(apply)
  }

  def apply(bytes: Array[Byte]): Seq[TransferMessage[Seq[Action]]] = {
    val messages = JdbcMessages.getRootAsJdbcMessages(ByteBuffer.wrap(bytes))
    scala.collection.immutable.Seq.tabulate(messages.messagesLength())(messages.messages).map(apply)
  }

  private def unapply(builder: FlatBufferBuilder, sql: Sql): Int = {
    val preparedSql = sql match {
      case nameSql: NameSql ⇒ nameSql.toPreparedSql
      case preparedSql: PreparedSql ⇒ preparedSql
    }
    val params = JdbcStatement.createParamsVector(builder, preparedSql.params.map(param ⇒ FbsJdbcParam(builder, param)).toArray)
    JdbcStatement.createJdbcStatement(builder, builder.createString(preparedSql.sql), params)
  }

  def unapply(record: TransferMessage[Seq[Action]], builder: FlatBufferBuilder): Int = {
    val tId = JdbcMessage.createTidVector(builder, record.id.toBytes)
    val jdbcActions = record.data.map {
      case update: Update ⇒
        val statements = JdbcAction.createStatementsVector(builder, Array(unapply(builder, update.sql)))
        JdbcAction.createJdbcAction(builder, ActionType.Update, statements)
      case upsert: Upsert ⇒
        val statements = JdbcAction.createStatementsVector(builder, Array(unapply(builder, upsert.up), unapply(builder, upsert.sert)))
        JdbcAction.createJdbcAction(builder, ActionType.Upsert, statements)
    }
    val actions = JdbcMessage.createActionsVector(builder, jdbcActions.toArray)
    JdbcMessage.createJdbcMessage(builder, tId, actions)
  }

  def unapply(records: Seq[TransferMessage[Seq[Action]]], initialCapacity: Int): FlatBufferBuilder = {
    val builder = new FlatBufferBuilder(initialCapacity)
    val messages = JdbcMessages.createMessagesVector(builder, records.map(unapply(_, builder)).toArray)
    val root = JdbcMessages.createJdbcMessages(builder, messages)
    builder.finish(root)
    builder
  }
}