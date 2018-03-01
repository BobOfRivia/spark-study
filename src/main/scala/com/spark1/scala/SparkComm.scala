package scala.com.spark1.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by JACK on 2018/2/17.
  */
trait SparkComm {

  def init(appname:String): SparkContext ={
    new SparkContext(new SparkConf().setAppName(appname).setMaster("local"))
  }

  def printTuple(tp:Tuple2[Any,Any]): Unit ={
    println(tp._1+" "+tp._2)
  }

  def printMap(i:Any): Unit ={
    print(i+" ")
  }

}
