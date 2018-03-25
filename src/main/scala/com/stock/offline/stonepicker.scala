package com.stock.offline

import java.io.FileWriter
import java.sql.DriverManager

import com.util.HttpUtil

/**
  * 获取股票信息
  *
  * 示例URL:http://q.stock.sohu.com/hisHq?code=cn_%s&start=20171109&end=20180308&stat=1&order=D&period=d&callback=historySearchHandler&rt=jsonp&r=0.11224118720128917&0.238318188568853
  *
  */
object stonepicker {
  val filePath = "d://stock-message.txt"
  var errcount:Int = 0
  /**
    *
    * @param args
    *              (filePath,jdbcurl,total,index)
    */
  def main(args: Array[String]): Unit = {
    getStockDetail()
  }

  def getStockDetail(): Unit ={
    val urlSample = "http://q.stock.sohu.com/hisHq?code=cn_%s&start=19901009&end=20180308&stat=1&order=D&period=d&callback=historySearchHandler&rt=jsonp&r=0.11224118720128917&0.238318188568853";

    val jdbcurl = "jdbc:mysql://127.0.0.1:3306/lifeblog?user=root&password=541325&serverTimezone=GMT"
    val sql = "select stock_id,name from t_stock_list where goted = 0"
    val updatesql = "update t_stock_list set goted=1 where stock_id =%s"

    val conn = DriverManager.getConnection(jdbcurl)
    val rs = conn.prepareStatement(sql).executeQuery();
    conn.setAutoCommit(false)
    val writer = new FileWriter(filePath,true);
    try {
      while (rs.next()) {
        Thread.sleep(1100)
        val code = rs.getString(1)
        val data = HttpUtil.get(urlSample.format(code), charset = "gb2312")
        println(data)
        writer.write(data)
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
