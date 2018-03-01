package scala.com.spark1.scala

import scala.com.spark1.scala.TransformationDemo.init

/**
  * Created by JACK on 2018/2/17.
  */
object ActionDemo extends SparkComm{

  def main(args: Array[String]): Unit = {
    var sc = init("SparkComm")

    //1 to 10 累加
    var rdd = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10))

    println(rdd.reduce(_+_))
    //reduce来累加

    /**
      * collect 将 RDD中的元素获取到客户端
      * 这种方式不建议使用，若取样 可以使用take(n)，遍历建议使用foreach
      * 因为如果数据量大的话，会浪费大量网络IO，还可能发生oom异常
      *
      * 不用foreach action操作，在远程集群上遍历rdd中的元素
      * 而使用collect操作，将分布在远程集群上的RDD拉到本地
      */
    var list = rdd.map(_*2).collect()

    //count
    println(rdd.count())

    //take操作与collect 类似，也是获取远程数据到本地
    //因为返回类型不是RDD take可以选择数量
    /**
      * 重要：因为foreach 是在远程集群上执行的，因此效率要比聚合action高很多
      */
    var list1 = rdd.take(5).foreach(println(_))

    //saveAsTextFile
    //将RDD保存在本地 或者HDFS中
    //saveAsTextFile 只能保存在hdfs? 并且不能指定具体的文件名，只能指定文件夹
//    rdd.saveAsTextFile("D:\\saveAsTextFile.txt")

    var rdd1 = sc.parallelize(Array(("c1","Jack"),("c2","Rose"),("c3","bob"),("c1","Doge")))
    rdd1.countByKey().foreach(printTuple(_))
  }

}
