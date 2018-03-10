package com.spark1.scala.PVUV

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.tools.scalap.scalax.util.StringUtil

/**
  * Created by JACK on 2018/3/5.
  */
object pvuvCounter {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("pvuvCounter").setMaster("local[*]") //方括号中的数据代表启动指定线程数，* 等价于CPU核数
    val sc = new SparkContext(conf)

    var rdd1 = sc.textFile("G:\\Work-Space\\Hadoop\\spark-study\\src\\main\\scala\\com\\spark1\\scala\\PVUV\\page_views.data")
    //PV
    var rdd2: RDD[Array[String]] =  rdd1.map(_.split("\t")).filter(arr => arr.length ==7  )
    var starttime = System.currentTimeMillis()

//    rdd2.cache()
    var rddpv = rdd2.map{
      list =>
        var str = list(0).toString.split(" ")(0)
        (str.substring(0,str.length),1)
    }.reduceByKey(_+_)
    var midtime = System.currentTimeMillis()
    println("================PV,costtime="+(midtime-starttime))

    //UV
   var rdduv: RDD[(String, Int, Int)] = rdd2.filter(arr => arr(4).trim.length>0 ).map(l =>(l(0).substring(0,l(0).length),l(4))).groupByKey().map{ pair=>
      var date = pair._1
      //toset去重
      var unum = pair._2.toSet.size
      var unum1 = pair._2.size
      (date,unum,unum1)
    }
    var endtime = System.currentTimeMillis()
    println("================UV,costtime="+(endtime-midtime))
    var a = rdduv.map( d => (d._1,d._2))
    rddpv.leftOuterJoin(a).map{ m =>
      var data = m._1
      var pv = m._2._1
      var uv = m._2._2
      (data,pv,uv)
    }.foreach(println(_))

    println("wholetime===>"+(endtime-starttime))
  }

}
