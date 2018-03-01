package com.spark1.scala.secsort

import org.apache.spark.{SparkConf, SparkContext}

import scala.com.spark1.scala.sparkSenior.SecSort

/**
  * Spark的二次排序
  */
class secsort {

}

object secsort{

  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setAppName("secsort").setMaster("local")
    var sc = new SparkContext(conf)
    var file = sc.textFile("D:\\LearnWSpace\\Language\\scala\\scalaWkspace\\spark-study\\src\\main\\scala\\com\\spark1\\scala\\secsort\\secsort")
    file.map{ line =>
      var l = line.split(" ")
      (new SecSort(l(0).toInt,l(1).toInt),l(0).toInt,l(1).toInt)
    }.sortBy(_._1).map(m => (m._2,m._3)).foreach(pair => println(pair._1 + " "+pair._2 ))




  }

}
