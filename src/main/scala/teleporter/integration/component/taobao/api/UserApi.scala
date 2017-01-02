package teleporter.integration.component.taobao.api

import com.taobao.api.TaobaoClient
import com.taobao.api.domain.User
import com.taobao.api.request.UserGetRequest
import com.taobao.api.response.UserGetResponse
import teleporter.integration.component.taobao.TaobaoApi

/**
  * Author: kui.dai
  * Date: 2016/3/8.
  */
trait UserApi extends TaobaoApi {
  def `taobao.user.get`(fields: String, nick: String)(implicit client: TaobaoClient): Either[Throwable, User] = {
    val request = new UserGetRequest
    request.setFields(fields)
    request.setNick(nick)
    responseCall[UserGetResponse, User](request, resp ⇒ resp.getUser, null)
  }
}