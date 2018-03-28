package com.stock.offline.Extract.Extract2Hive

import java.sql.DriverManager
import java.util
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.stock.offline.StoneBean
import com.util.HttpUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import spray.json.DefaultJsonProtocol


/**
  * 获取股票信息
  * Save as json file
  *
  * 示例URL:http://q.stock.sohu.com/hisHq?code=cn_%s&start=20171109&end=20180308&stat=1&order=D&period=d&callback=historySearchHandler&rt=jsonp&r=0.11224118720128917&0.238318188568853
  *
  */
object StockProducer {
  val filePath = "d://stock-message-json-v2.txt"
  var errcount:Int = 0

  val hql = "CREATE EXTERNAL TABLE IF NOT EXISTS t_stock_offline(date, oppoint,clspoint,cgmoney,cgpoint,minpoint,maxpoint,dealnum,dealmoney,cgrate) " +
    " COMMENT 'This is the staging page view table' PARTITIONED BY (stockcode)" +
    " ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'" +
    " STORED AS TEXTFILE" +
    " LOCATION '<hdfs_location>';"



  /**
    *
    * @param args
    *              (filePath,jdbcurl,total,index)
    */
  def main(args: Array[String]): Unit = {
    getStockDetail()
  }

  /**
    * 股票数据类
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
  case class stockel(stockcode:String,date:String,oppoint:String,clspoint:String,cgmoney:String,cgpoint:String,minpoint:String,maxpoint:String,dealnum:String,dealmoney:String,cgrate:String)
  object MyJsonProtocol extends DefaultJsonProtocol {
    implicit val colorFormat = jsonFormat11(stockel)
  }

  import MyJsonProtocol._
  import spray.json._

  def getStockDetail(): Unit ={
    val urlSample = "http://q.stock.sohu.com/hisHq?code=cn_%s&start=19901009&end=20180308&stat=1&order=D&period=d&callback=historySearchHandler&rt=jsonp&r=0.11224118720128917&0.238318188568853";
    val jdbcurl = "jdbc:mysql://127.0.0.1:3306/lifeblog?user=root&password=541325&serverTimezone=GMT"
    val sql = "select stock_id,name from t_stock_list where goted = 0"
    val updatesql = "update t_stock_list set goted=1 where stock_id =%s"
    val reg1 = """historySearchHandler\(\[(.*)\]\)""".r

    //kafkaconfig
    var kafkaParam =new Properties()
    kafkaParam.put("bootstrap.servers", "bogon:9092,bogon:9093,bogon:9094")
    kafkaParam.put("request.required.acks", "0")
    kafkaParam.put("producer.type", "sync")
    kafkaParam.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaParam.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    //init kafka
    var kafka = new KafkaProducer[String,String](kafkaParam)

    val conn = DriverManager.getConnection(jdbcurl)
    val rs = conn.prepareStatement(sql).executeQuery();
    conn.setAutoCommit(false)
    try {
      while (rs.next()) {
        Thread.sleep(1100)
        val code = rs.getString(1)
        val data = HttpUtil.get(urlSample.format(code), charset = "gb2312")
        //unapply source to json-array
        val jsonarr = reg1.unapplySeq(data).getOrElse(Array(""))(0)
        if(!"".equals(jsonarr)){
          //parse to json-object
          val bean = JSON.parseObject(jsonarr,classOf[StoneBean])
          val jsonobj = stockel(bean.code,bean(0),bean(1),bean(2),bean(3),bean(4),bean(5),bean(6),bean(7),bean(8),bean(9)).toJson.toString()
          val record = new ProducerRecord[String,String](bean.code,jsonobj)
          kafka.send(record)
        }
        println(code + "  finished")
        conn.prepareStatement(updatesql.format(code)).execute()
      }
    }catch{
      case ex:Exception => {
        println(ex)
        errcount +=1
        conn.commit()
        if(errcount <= 10){
          getStockDetail()
        }else{
          // do nothing
        }
      }
    }finally {
      conn.commit()
    }
  }




}
