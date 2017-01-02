package teleporter.integration.component.taobao

import java.time.ZoneId
import java.util.Date

import com.taobao.api._
import teleporter.integration._
import teleporter.integration.component.ScheduleActorPublisher
import teleporter.integration.component.ScheduleActorPublisherMessage.ScheduleSetting
import teleporter.integration.component.taobao.TaobaoApi.PageTimeApi
import teleporter.integration.core._
import teleporter.integration.utils.Converters._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by huanwuji on 2016/11/18.
  */
trait TaobaoApi {
  def responseCalls[P <: TaobaoResponse, E](req: TaobaoRequest[P], result: P ⇒ Seq[E], token: String)(implicit client: TaobaoClient): Either[Throwable, Seq[E]] = {
    responseCall[P, Seq[E]](req, result, token)
  }

  def responseCall[P <: TaobaoResponse, T](req: TaobaoRequest[P], result: P ⇒ T, token: String)(implicit client: TaobaoClient): Either[Throwable, T] = {
    try {
      val resp = client.execute(req, token)
      if (resp.isSuccess) {
        Right(result(resp))
      } else {
        Left(TaobaoApiException(resp))
      }
    } catch {
      case e: ApiException ⇒ Left(e)
    }
  }
}

object TaobaoApi {
  type PageTimeApi[T] = (String, Date, Date, Long, Long, String) ⇒ TaobaoClient ⇒ Either[Throwable, Seq[T]]
}

class TaobaoApiException(msg: String) extends RuntimeException(msg)

object TaobaoApiException {
  def apply(resp: TaobaoResponse): TaobaoApiException = new TaobaoApiException(s"code:${resp.getErrorCode}, errorMsg:${resp.getMsg}, subCode:${resp.getSubCode}, subMsg:${resp.getSubMsg}")
}

object TaobaoClientMetaBean {
  val FServerUrl = "serverUrl"
  val FAppKey = "appKey"
  val FAppSecret = "appSecret"
  val FFormat = "format"
  val FConnectTimeout = "connectTimeout"
  val FReadTimeout = "readTimeout"
}

class TaobaoClientMetaBean(override val underlying: Map[String, Any]) extends AddressMetaBean(underlying) {

  import TaobaoClientMetaBean._

  def serverUrl: String = client[String](FServerUrl)

  def appKey: String = client[String](FAppKey)

  def appSecret: String = client[String](FAppSecret)

  def format: String = client[String](FFormat)

  def connectTimeout: Int = client[Int](FConnectTimeout)

  def readTimeout: Int = client[Int](FReadTimeout)
}

class TaobaoApiPublisher[T](override val key: String, pageTimeApi: PageTimeApi[T])(implicit override val center: TeleporterCenter)
  extends ScheduleActorPublisher[T, TaobaoClient] {
  override implicit val executionContext: ExecutionContext = context.dispatcher

  override protected def grab(scheduleSetting: ScheduleSetting): Future[Iterator[T]] = {
    val shopContext = sourceContext.extraKeys[VariableContext]("token")
    val apiContext = sourceContext.extraKeys[VariableContext]("api")
    val token = shopContext.config[String]("token")
    val fields = apiContext.config[String]("fields")
    val timeAttrs = scheduleSetting.timeAttrs.get
    val pageAttrs = scheduleSetting.pageAttrs.get
    Future {
      pageTimeApi(fields,
        Date.from(timeAttrs.start.atZone(ZoneId.systemDefault()).toInstant),
        Date.from(timeAttrs.end.atZone(ZoneId.systemDefault()).toInstant),
        pageAttrs.page,
        pageAttrs.pageSize,
        token)(client)
    }.map {
      case Left(e) ⇒ throw e
      case Right(seq) ⇒ seq.toIterator
    }
  }
}

object TaobaoComponent {
  def taobaoClientApply: ClientApply = (key, center) ⇒ {
    val config = center.context.getContext[AddressContext](key).config
    val clientMetaBean = config.mapTo[TaobaoClientMetaBean]
    val client = new DefaultTaobaoClient(
      clientMetaBean.serverUrl,
      clientMetaBean.appKey,
      clientMetaBean.appSecret,
      clientMetaBean.format,
      clientMetaBean.connectTimeout,
      clientMetaBean.readTimeout
    )
    new CloseClientRef[TaobaoClient](key, client, _.key)
  }
}