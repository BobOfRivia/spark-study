package com.spark1.scala.sparkSenior

import scala.collection.Iterator
import scala.com.spark1.scala.SparkComm

/**
  * 分组TOPN
  * 思路：先分组，然后在map内实现topn
  */
object Topn extends SparkComm{

  def main(args: Array[String]): Unit = {
    val sc = init("topn")
    var lines = sc.parallelize(Array(("c1",99),("c1",67),("c1",68),("c1",89),("c1",91),("c2",100),("c2",96),("c2",71),("c2",87),("c3",99),("c3",8)))
    lines.groupByKey().map{
      line =>
        var itr = sortTopn(line._2,3)
        (line._1,itr)
    }.foreach(printTuple(_))
  }
  def sortTopn(itr:Iterable[Int],topn:Int): Iterable[Int] ={
    var max = 0
    var max1 = 0
    var max2 =0
    while(itr.iterator.hasNext){
      var i = itr.iterator.next()
      if(i > max) max=i
      else if(i > max1) max1=i
      else if(i > max2) max2=i
    }
    Array(max,max1,max2).toIterable
  }

}
