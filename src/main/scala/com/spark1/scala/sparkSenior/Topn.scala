package com.spark1.scala.sparkSenior

import scala.Option
import scala.collection.Iterator
import scala.collection.mutable.ArrayBuffer
import scala.com.spark1.scala.SparkComm
import scala.util.Random

/**
  * 分组TOPN
  * 思路：先分组，然后在map内实现topn
  */
object Topn extends SparkComm{

  def main(args: Array[String]): Unit = {
    val sc = init("topn")
    var lines = sc.parallelize(Array(("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",99),("c1",67),("c1",68),("c1",89),("c1",91),("c2",100),("c2",96),("c2",71),("c2",87),("c3",99),("c3",8)))
    lines.cache()
    //方案一
    //此处groupByKey 可能会OOM。
    //另一方面，如果某个key包含的数据占总数据的99%，那会造成严重的数据倾斜
//    lines.groupByKey().map{
//      line =>
//        var itr = sortTopn(line._2,3)
//        (line._1,itr)
//    }.foreach(printTuple(_))

    //方案二
    //多段聚合
    /**
      * 将分区的key分为多段（key_1,key_2,key_3），先对该分区进行第一次聚合并求TOPN，再将（key_1,key_2,key_3）合并，对该分区进行第二次聚合求TOPN
      */
    lines.mapPartitions({ m =>
      m.map{ l =>((Random.nextInt(2),l._1),l._2)}
    }).groupByKey().flatMap{
      m=>
        sortTopn(m._2,2).map((m._1._2,_))
    }.groupByKey().map{
      m =>
        (m._1,sortTopn(m._2,2))
    }.foreach(println(_))

    //方案三
    /**
      * 思路：在shuffle之前，先对分区中的数据进行一次聚合操作，获取当前分区的分组之后的TOPN结果，然后再对局部聚合结果，做一次全局的聚合
      */
//    lines.aggregateByKey(0)(_+_,_+_) <=等价于=>
//    lines.reduceByKey(_+_)

    /**
      * 第二个参数为针对同一个key的分区内部执行函数
      * 第三个参数为对所有分区执行函数
      */
    lines.aggregateByKey(new ArrayBuffer[Int]())(
      (u,v) =>{
        //将元素v添加至集合u
        u += v
        //返回u中最大的三个值
        u.sorted.take(3)
      },
        (u1,v1) =>{
          //将集合v1添加至集合u1
          u1 ++= v1
          //返回u中最大的三个值
          u1.sorted.take(3)
        }
    ).foreach(println(_))


  }
  def sortTopn(itr:Iterable[Int],topn:Int): Iterable[Int] ={
    println("do sortTopn")
    var max = 0
    var max1 = 0
    var max2 =0
    var items = itr.iterator;
    while(items.hasNext){
      var i = items.next()
      if(i > max) max=i
      else if(i > max1) max1=i
      else if(i > max2) max2=i
    }
    Array(max,max1,max2).toIterable
  }

}
