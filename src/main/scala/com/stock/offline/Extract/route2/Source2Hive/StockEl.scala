package com.stock.offline.Extract.route2.Source2Hive

import spray.json.DefaultJsonProtocol
/**
  * 股票数据类
  * @param stocktyp
  * @param stockcode 代码
  * @param date 日期
  * @param oppoint 开盘点数
  * @param clspoint 收盘点数
  * @param cgmoney 涨跌额
  * @param cgpoint 涨跌幅
  * @param minpoint 最低
  * @param maxpoint 最高
  * @param dealnum 成交量
  * @param dealmoney 成交金额
  * @param cgrate 换手率
  */
case class StockEl (stocktyp:String,stockcode:String,date:String,oppoint:String,clspoint:String,cgmoney:String,cgpoint:String,minpoint:String,maxpoint:String,dealnum:String,dealmoney:String,cgrate:String) extends Serializable{
}
object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val colorFormat = jsonFormat12(StockEl)
}