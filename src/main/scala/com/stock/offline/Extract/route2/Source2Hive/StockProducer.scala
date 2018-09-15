package com.stock.offline.Extract.route2.Source2Hive

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

  import MyJsonProtocol._
  import spray.json._

  def getStockDetail(): Unit ={
    val urlSample = "http://q.stock.sohu.com/hisHq?code=cn_%s&start=19901009&end=20180308&stat=1&order=D&period=d&callback=historySearchHandler&rt=jsonp&r=0.11224118720128917&0.238318188568853";
    val jdbcurl = "jdbc:mysql://127.0.0.1:3306/lifeblog?user=root&password=541325&serverTimezone=GMT"
    val sql = "select stock_id,name,belong_typ from t_stock_list where goted = 0"
    val updatesql = "update t_stock_list set goted=1 where stock_id =%s"
    val reg1 = """historySearchHandler\(\[(.*)\]\)""".r

    //kafkaconfig
    var kafkaParam =new Properties()
    kafkaParam.put("bootstrap.servers", "bogon:9092,bogon:9093,bogon:9094")
    kafkaParam.put("request.required.acks", "0")
    kafkaParam.put("producer.type", "sync")
    kafkaParam.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaParam.put("value.serializer", "com.stock.offline.Extract.route2.Source2Hive.StockEncoder")

    //init kafka
    var kafka = new KafkaProducer[String,StockEl](kafkaParam)

    val conn = DriverManager.getConnection(jdbcurl)
    val rs = conn.prepareStatement(sql).executeQuery();
    conn.setAutoCommit(false)
    try {
      while (rs.next()) {
        Thread.sleep(1100)
        val code = rs.getString(1)
        val data = HttpUtil.get(urlSample.format(code), charset = "gb2312")
        //unapply source to json-array
        val jsonarr = reg1.unapplySeq(data).getOrElse(Array(""))
        println("jsonarr="+jsonarr)
        if(!"".equals(jsonarr.toString)){
          //parse to json-object
          val bean: StoneBean = JSON.parseObject(jsonarr.toString,classOf[StoneBean])

          for(el <- bean.hq){
            val jsonobj = StockEl(rs.getString(3),bean.code,el(0),el(1),el(2),el(3),el(4),el(5),el(6),el(7),el(8),el(9))
            kafka.send(new ProducerRecord[String,StockEl](bean.code,jsonobj))
          }
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
