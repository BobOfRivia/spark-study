package com.stock.offline.Extract

import java.io.FileWriter
import java.sql.DriverManager

import com.util.HttpUtil
import spray.json.DefaultJsonProtocol

/**
  * 获取股票信息
  * Save as json file
  *
  * 示例URL:http://q.stock.sohu.com/hisHq?code=cn_%s&start=20171109&end=20180308&stat=1&order=D&period=d&callback=historySearchHandler&rt=jsonp&r=0.11224118720128917&0.238318188568853
  *
  */
object Extract2JsonFile {
  val filePath = "d://stock-message-json-v2.txt"
  var errcount:Int = 0
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
    * @param code 代码
    * @param col1 日期
    * @param col2 开盘点数
    * @param col3 收盘点数
    * @param col4 涨跌额
    * @param col5 涨跌幅
    * @param col6 最低
    * @param col7 最高
    * @param col8 成交量
    * @param col9 成交金额
    * @param col10 换手率
    */
  case class stockel(code:String,col1:String,col2:String,col3:String,col4:String,col5:String,col6:String,col7:String,col8:String,col9:String,col10:String)
  object MyJsonProtocol extends DefaultJsonProtocol {
    implicit val colorFormat = jsonFormat11(stockel)
  }

  def getStockDetail(): Unit ={
    val urlSample = "http://q.stock.sohu.com/hisHq?code=cn_%s&start=19901009&end=20180308&stat=1&order=D&period=d&callback=historySearchHandler&rt=jsonp&r=0.11224118720128917&0.238318188568853";

    val jdbcurl = "jdbc:mysql://127.0.0.1:3306/lifeblog?user=root&password=541325&serverTimezone=GMT"
    val sql = "select stock_id,name from t_stock_list where goted = 0"
    val updatesql = "update t_stock_list set goted=1 where stock_id =%s"

    val reg1 = """historySearchHandler\(\[(.*)\]\)""".r

    val conn = DriverManager.getConnection(jdbcurl)
    val rs = conn.prepareStatement(sql).executeQuery();
    conn.setAutoCommit(false)
    val writer = new FileWriter(filePath,true);
    try {
      while (rs.next()) {
        Thread.sleep(1100)
        val code = rs.getString(1)
        val data = HttpUtil.get(urlSample.format(code), charset = "gb2312")
        val json = reg1.unapplySeq(data).getOrElse(Array("{}"))(0)
        writer.write(json)
        println(code + "  finished")
        conn.prepareStatement(updatesql.format(code)).execute()
      }
    }catch{
      case ex:Exception => {
        println(ex)
        errcount +=1
        writer.flush()
        writer.close()
        conn.commit()
        if(errcount <= 10){
          getStockDetail()
        }else{
          // do nothing
        }
      }
    }finally {
      writer.flush()
      writer.close()
      conn.commit()
    }
  }




}
