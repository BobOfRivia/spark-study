package scala.com.spark1.scala

import org.apache.spark.{SparkConf, SparkContext}

import  org.apache.spark.rdd.RDD
/**
  * Created by JACK on 2018/2/13.
  */
object WordCountLocal {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ScalaWordCountLocal").setMaster("local")

    var sc = new SparkContext(conf)

    var lines = sc.textFile("D:\\my.cnf",1)
    var wordRdd = lines.flatMap(line => line.split(" "))

//    var mapRdd = wordRdd.map(_ => (_,1))
    var mapRdd = wordRdd.map((_,1) )

    for( i <- mapRdd.reduceByKey(_ + _)){
      println(i._1 + " "+ i._2)
    }
//    wordCount.for
  }

}

class WordCountLocal{

}
