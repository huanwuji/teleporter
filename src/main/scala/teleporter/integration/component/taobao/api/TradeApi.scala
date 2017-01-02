package teleporter.integration.component.taobao.api

import java.util.Date

import com.taobao.api.TaobaoClient
import com.taobao.api.domain.Trade
import com.taobao.api.request._
import com.taobao.api.response._
import teleporter.integration.component.taobao.TaobaoApi
import teleporter.integration.component.taobao.TaobaoApi.PageTimeApi

import scala.collection.JavaConverters._

/**
  * Author: kui.dai
  * Date: 2015/12/29.
  */
trait TradeApi extends TaobaoApi {
  def `taobao.trade.fullinfo.get`(fields: String, tid: Long, token: String)(implicit client: TaobaoClient): Either[Throwable, Trade] = {
    val request = new TradeFullinfoGetRequest
    request.setFields(fields)
    request.setTid(tid)
    responseCall[TradeFullinfoGetResponse, Trade](request, resp ⇒ resp.getTrade, token)
  }

  def `taobao.trades.sold.get`: PageTimeApi[Trade] = (fields: String, start: Date, end: Date, page: Long, pageSize: Long, token: String) ⇒ { implicit client: TaobaoClient ⇒
    val request = new TradesSoldGetRequest
    request.setFields(fields)
    request.setUseHasNext(true)
    request.setStartCreated(start)
    request.setEndCreated(start)
    request.setPageNo(page)
    request.setPageSize(pageSize)
    responseCalls[TradesSoldGetResponse, Trade](request, _.getTrades.asScala, token)
  }

  def `taobao.trades.sold.increment.get`: PageTimeApi[Trade] = (fields: String, start: Date, end: Date, page: Long, pageSize: Long, token: String) ⇒ { implicit client: TaobaoClient ⇒
    val request = new TradesSoldIncrementGetRequest
    request.setFields(fields)
    request.setUseHasNext(true)
    request.setStartModified(start)
    request.setEndModified(end)
    request.setPageNo(page)
    request.setPageSize(pageSize)
    responseCalls[TradesSoldIncrementGetResponse, Trade](request, _.getTrades.asScala, token)
  }

  def `taobao.trades.sold.incrementv.get`: PageTimeApi[Trade] = (fields: String, start: Date, end: Date, page: Long, pageSize: Long, token: String) ⇒ { implicit client: TaobaoClient ⇒
    val request = new TradesSoldIncrementvGetRequest
    request.setFields(fields)
    request.setUseHasNext(true)
    request.setStartCreate(start)
    request.setEndCreate(end)
    request.setPageNo(page)
    request.setPageSize(pageSize)
    responseCalls[TradesSoldIncrementvGetResponse, Trade](request, _.getTrades.asScala, token)
  }
}

object TradeApi extends TradeApi