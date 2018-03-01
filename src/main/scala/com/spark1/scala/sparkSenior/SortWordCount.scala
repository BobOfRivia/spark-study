package scala.com.spark1.scala.sparkSenior

import scala.com.spark1.scala.SparkComm

/**
  * 对wordcount 单词数进行倒序排序
  * SortBy || SortByKey
  */
object SortWordCount extends SparkComm{

  def main(args: Array[String]): Unit = {

    val sc = init("SortWordCount")
    var rdd = sc.parallelize(Array("Hello Jack","Hello bob","Hey Json","GOOD day my lady","Hey Jack","Hello Esan"))
    var rdd1 = rdd.flatMap(_.split(" ")).map(el => (el,1)).reduceByKey(_ + _)
    /**
      * 如果要针对结果进行二次排序，该怎么做呢
      *   ---1.进行key,value 的反转 将要排序的作为key值
      */
    rdd1.map(tpl =>(tpl._2,tpl._1)).sortByKey(false).foreach(printTuple(_))

    /**
      * 实际可以不需要反转
      * 直接sortBy
      */
    rdd1.sortBy(_._2,false).foreach(printTuple(_))


  }

}
