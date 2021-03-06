package com.stock.offline.Extract.route1.aSource2file

import java.io.FileWriter
import java.sql.DriverManager

import com.util.HttpUtil
import spray.json.DefaultJsonProtocol

/**
  * 获取股票信息
  * Save as json file
  *
  * 示例URL:http://q.stock.sohu.com/hisHq?code=cn_%s&start=20171109&end=20180401&stat=1&order=D&period=d&callback=historySearchHandler&rt=jsonp&r=0.11224118720128917&0.238318188568853
  *
  */
object Extract2JsonFile {
  val filePath = "/Users/guhongjie/data/spider/stock/stock-message-json.txt"
  var errcount:Int = 0
  /**
    *
    * @param args
    *              (filePath,jdbcurl,total,index)
    */
  def main(args: Array[String]): Unit = {
    getStockDetail()
//    test()
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
  import util.control.Breaks._
  def getStockDetail(): Unit ={
    val urlSample = "http://q.stock.sohu.com/hisHq?code=cn_%s&start=19901009&end=20180805&stat=1&order=D&period=d&callback=historySearchHandler&rt=jsonp&r=0.11224118720128917&0.238318188568853";

    val jdbcurl = "jdbc:mysql://127.0.0.1:3306/spider?user=root&password=sxd5a5dwg&serverTimezone=GMT"
    val sql = "select stock_id,name,belong_typ from t_stock_list where goted = 0"
    val updatesql = "update t_stock_list set goted=1 where stock_id =%s"

    val reg1 = """historySearchHandler\(\[(.*)\]\)""".r

    val reg2 = """\{(.*)\}""".r

    val conn = DriverManager.getConnection(jdbcurl)
    val rs = conn.prepareStatement(sql).executeQuery()
    conn.setAutoCommit(false)
    val writer = new FileWriter(filePath,true);
    try {
      while (rs.next()) {
        breakable{
          Thread.sleep(1100)
          val code = rs.getString(1)
          var data = HttpUtil.get(urlSample.format(code), charset = "gb2312")
  //        val test = reg1.unapplySeq(data)
  //        val test1 = Array("{}")
  //        val test2 =  reg1.unapplySeq(data).getOrElse(Array("{}"))
  //        val test = reg1.unapplySeq(data).getOrElse(List[AnyRef])
          data = data.substring(22,data.length-3)

          if(data.equals("")){
            break
          }


          val json = reg2.unapplySeq(data).get
          println(json)
  //        println(json(0))
          writer.write(json(0))
          println(code + "  finished")
          conn.prepareStatement(updatesql.format(code)).execute()
        }
      }
    }catch{
      case ex:Exception => {
        ex.printStackTrace()
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

  def test(): Unit ={
    val jsonMsg = "historySearchHandler([{\"status\":2,\"msg\":\"cn_000003 non-existent\",\"code\":\"cn_000003\"}])"

    val reg1 = """historySearchHandler\(\[(.*)\]\)""".r
    val res1 = reg1.unapplySeq(jsonMsg).getOrElse()
    val res = reg1.unapplySeq(jsonMsg).getOrElse().asInstanceOf[List[AnyRef]]

    println(res(0))

    println()
  }




}
