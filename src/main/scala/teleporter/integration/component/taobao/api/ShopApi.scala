package teleporter.integration.component.taobao.api

import com.taobao.api.TaobaoClient
import com.taobao.api.domain.Shop
import com.taobao.api.request.ShopGetRequest
import com.taobao.api.response.ShopGetResponse
import teleporter.integration.component.taobao.TaobaoApi

/**
  * Author: kui.dai
  * Date: 2016/3/7.
  */
trait ShopApi extends TaobaoApi {
  def `taobao.shop.get`(fields: String, sellerNick: String)(implicit client: TaobaoClient): Either[Throwable, Shop] = {
    val request = new ShopGetRequest()
    request.setFields(fields)
    request.setNick(sellerNick)
    responseCall[ShopGetResponse, Shop](request, resp ⇒ resp.getShop, null)
  }
}