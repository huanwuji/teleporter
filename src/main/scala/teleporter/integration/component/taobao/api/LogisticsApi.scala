package teleporter.integration.component.taobao.api

import com.taobao.api.TaobaoClient
import com.taobao.api.domain.AddressResult
import com.taobao.api.request.LogisticsAddressSearchRequest
import com.taobao.api.response.LogisticsAddressSearchResponse
import teleporter.integration.component.taobao.TaobaoApi

import scala.collection.JavaConverters._

/**
  * Author: kui.dai
  * Date: 2016/3/14.
  */
trait LogisticsApi extends TaobaoApi {
  def `taobao.logistics.address.search`(token: String, rDef: String)(implicit client: TaobaoClient): Either[Throwable, Seq[AddressResult]] = {
    val request = new LogisticsAddressSearchRequest()
    request.setRdef(rDef)
    client.execute(request, token)
    responseCalls[LogisticsAddressSearchResponse, AddressResult](request, resp ⇒ resp.getAddresses.asScala, null)
  }
}