package scala.com.spark1.scala

/**
  * 共享变量 -- 广播变量
  *
  * 共享变量存在的意义：
  *   ---原始的变量，每一份都会拷贝一份至Task
  *   ---共享变量，相当于单例工厂，共用同一个变量，可以减少网络传输
  *   ---如果变量很庞大，定义共享变量可以节省很多网络IO
  *
  * 其中 Broadcast  属于只读
  * Accumulator 属于可写
  *
  *
  */
object BroadcastVarDemo extends SparkComm{

  def main(args: Array[String]): Unit = {

    var sc = init("broadCast")

    var rdd= sc.parallelize(Array(1,2,3,4,5,6))

    var num = 3
    rdd.map(_*num).foreach(printMap(_))

    /**
      * 使用广播变量
      */
    var bc = sc.broadcast(num)
    rdd.map(_*bc.value).foreach(printMap(_))



    //统计次数
    //在集群上操作修改共享变量可能会存在问题，建议使用Accumulator
    rdd.map{el=>
      num = num +1
    }.foreach(printMap(_))
    /**
      * 使用累加变量
      * 可以使集群操作一个共同变量
      */
    var ac =sc.longAccumulator
    rdd.map(_+1).foreach(printMap(_))
    println("累计总数="+ac.value)
  }




}
