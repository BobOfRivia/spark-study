package com.jrb_access_monitor.Hive2Mysql

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

class hive2mysql {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hive2mysql").setMaster("local[2]")

    val sc =new SparkContext(conf)

    val spark: SparkSession = SparkSession.builder.appName("log2Hive_hiveMission").master("local[2]").getOrCreate()

    spark.udf.register("length0",(s:String)=>s.length())

    //groupby A,B  diff from mysql
    //val df =spark.sql("select substr(time,1,8) as day,ipaddr,count(1) from hive_log_access group by ipaddr,day")

    //so
    val df: DataFrame =spark.sql("select substr(time,1,8) as day,ipaddr from hive_log_access")
    import spark.implicits._
    val result1 =df.map{
      line=>
        val key=(line.getString(0),line.getString(1))
        (key,1)
    }
    result1.rdd.reduceByKey(_+_).sortBy(_._1._1)foreach{
      el=>
        println(el._1._1+" "+el._1._2+" "+el._2)
    }
//    def func(a:(String,Int),b:(String,Int)): (String,Int) ={
//      if(a._1==b._1)
//        (a._1,a._2+a._2)index.html
//      else
//        b
//    }


  }

}
