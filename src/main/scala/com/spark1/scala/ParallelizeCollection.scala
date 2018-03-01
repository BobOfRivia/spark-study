package scala.com.spark1.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * SortBy || SortByKey
  */
object ParallelizeCollection {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParallelizeCollection").setMaster("local")
    val sc = new SparkContext(conf)
    println(sc.parallelize(Array(("Jack",211),("App",112),("Lucy",23),("Zues",100)),1).sortByKey(false).collect().foreach(println(_)))
    println(sc.parallelize(Array(("Jack",211),("App",112),("Lucy",23),("Zues",100)),1).sortBy(_._2,false).collect().foreach(println(_)))
  }

}
