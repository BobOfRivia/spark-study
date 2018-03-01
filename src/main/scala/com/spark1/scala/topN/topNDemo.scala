package com.spark1.scala.topN

import org.apache.spark.{SparkConf, SparkContext}
/**
  * TOPN算法
  */
class topNDemo {

}

object topNDemo{

  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setAppName("topNDemo").setMaster("local")

    var sc = new SparkContext(conf)
    var file = sc.textFile("D:\\LearnWSpace\\Language\\scala\\scalaWkspace\\spark-study\\src\\main\\scala\\com\\spark1\\scala\\topN\\topn")
    var rdd1 =  file.filter(!_.startsWith(" ")).filter(!_.startsWith("考号"))
    var rdd2 =  file.filter(_.startsWith(" ")).map(_.replace(" ",""))
    var filt = rdd1.union(rdd2)
    var data = filt.map( line => {
      var word = line.split("\t");
      (word(0),word(1),word(2),word(3),word(4))
    })
    println("====语文TOP5")
    var lanTop5 = data.map(line => (line._1,line._2,line._3)).sortBy(_._3,false).take(5).foreach(println(_))
    println("====数学TOP5")
    var mathTop5 = data.map(line => (line._1,line._2,line._4)).sortBy(_._3,false).take(5).foreach(println(_))
    println("====英语TOP5")
    var EnTop5 = data.map(line => (line._1,line._2,line._5)).sortBy(_._3,false).take(5).foreach(println(_))



  }

}



