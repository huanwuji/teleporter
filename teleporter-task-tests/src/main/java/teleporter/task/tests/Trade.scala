package teleporter.task.tests

/**
 * Created by joker on 15/10/20.
 */
case class Trade(tid:String,
                 status:String,
                 dp_id:String,
                 modified:String,
                 created:String,
                 adjustFee:Double,
                 alipayNo:String,
                 buyerAlipayNo:String,
                 buyerCodFee:Double,
                 buyerEmail:String,
                 buyerNick:String,
                 buyerObtainPointFee:Double,
                 codFee:Double,
                 codStatus:String,
                 commissionFee:Double,
                 consignTime:String,
                 discountFee:Double,
                 endTime:String,
                 expressAgencyFee:Double,
                 hasPostFee:String,
                 payTime:String,
                 payment:Double,
                 pointFee:Double,
                 postFee:Double,
                 realPointFee:Double,
                 receivedPayment:Double,
                 receiverAddress:String,
                 receiverCity:String,
                 receiverDistrict:String,
                 receiverMobile:String,
                 receiverName:String,
                 receiverPhone:String,
                 receiverState:String,
                 receiverZip:String,
                 sellerCodFee:Double,
                 sellerNick:String,
                 shippingType:String,
                 timeoutActionTime:String,
                 totalFee:Double,
                 tradefrom:String,
                 `type`:String,
                 num:Long,
                 stepTradeStatus:String,
                 stepPaidFee:Double
                  )  {
    val separator :String="\001"
   override  def toString():String={
     this.productIterator.map(msg=>{
          msg
     }).mkString(separator)

   }



}



