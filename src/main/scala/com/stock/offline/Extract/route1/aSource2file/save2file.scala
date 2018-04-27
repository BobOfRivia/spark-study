package com.stock.offline.Extract.route1.aSource2file

import com.alibaba.fastjson.JSON
import com.stock.offline.StoneBean
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import spray.json.DefaultJsonProtocol

import scala.collection.mutable.ArrayBuffer

/**
  * Created by JACK on 2018/3/10.
  */
object save2file {
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


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("save2hive").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd0: RDD[String] = sc.textFile("file:///D://stock-message.txt")
    val reg1 = """historySearchHandler\(\[(.*)\]\)""".r
    val rdd1: RDD[StoneBean] = rdd0.map{ l =>
      l match {
        case reg1(value) => JSON.parseObject(value,classOf[StoneBean])
        case _ => new StoneBean()
      }
    }

    import MyJsonProtocol._
    import spray.json._

    val rdd2 = rdd1.filter(_.hq != null).flatMap{ stock =>
      var a = new ArrayBuffer[String]()
      for(el <- stock.hq){
        el(0)
        //produce  to  kafka   &  Consumer use the batch data to insert into hive
        a += stockel(stock.code,el(0),el(1),el(2),el(3),el(4),el(5),el(6),el(7),el(8),el(9)).toJson.toString()
      }
      a
    }
    rdd2.take(4).foreach(println(_))
    // TODO OR TODF & SAVE TO HIVE OR PQRQUET
    rdd2.saveAsTextFile("file:///D://stock-message-json.txt")
    rdd2
    //使用Spark 将json文件处理为业务需要的json格式  code data1 data2 data3 ...

    //然后使用spark sql 来读取文件，生成临时表，并进行计算或查询 将结果保存为一张表。

  }

}
