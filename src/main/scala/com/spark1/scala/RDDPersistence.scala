package scala.com.spark1.scala

/**
  * RDD持久化
  */
object RDDPersistence extends SparkComm{

  def main(args: Array[String]): Unit = {
    /**
      * 为什么RDD需要持久化？
      *
      * for example
      */
    var sc = init("RDDPersistence")
    var rdd0 = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10))
    var rdd1 = rdd0.map(_*2)
    rdd1.foreach(printMap(_))

    /**
      * 在第二次调用之前的rdd0时，所有spark会重新去计算生成（或者从数据源获取）rdd0，因此耗时是成倍的
      * 为了解决这样的问题，我们需要把rdd数据进行持久化
      */
    rdd0.foreach(printMap(_))

    var lines = sc.textFile("D:\\yifsdb1-20180112.sql")
    var chars = lines.flatMap(_.toCharArray)

    /**
      * 5552097
        time1 = 336
        time2 = 177
      *
      * 5552097
        time1 = 808
        time2 = 82
      */
    //加了cache || persist 第二次真的快了 第一次慢了
    //因此要权衡考虑 缓存花去的时间
    //老版本cache需要连续调用？
    //优先
    chars.cache()

    /**
      * persist的参数包括三种持久化策略
      *
      * MEMORY_ONLY 仅持久化到内存，若超出则下次需要重新计算
      * MEMORY_ONLY_SER 同上，但是数据会先进行序列化，但是序列化和反序列化会消耗CPU 和 时间
      * MEMORY_AND_DISK 超出内存的部分，存储至磁盘，下次会联合读取
      * MEMORY_AND_DISK_SER 同上，数据会序列化存储
      * DISK_ONLY 进磁盘
      * xxxxxxxx_2 表示缓存的数据，会存储一个副本在其他节点，这样可以避免缓冲数据丢失，导致必须重新计算
      *
      * 一般来说，优先级
      *   MEMORY_ONLY > MEMORY_ONLY_SER > xxxxxxxx_2
      * 但一般会避免使用磁盘缓存，因为磁盘数据获取速度可能慢于重新计算
      */
        chars.persist()

    var time0 = System.currentTimeMillis()
    println(chars.count())
    var time1 = System.currentTimeMillis()
    println(chars.count())
    var time2 = System.currentTimeMillis()

    println("time1 = "+(time1-time0))
    println("time2 = "+(time2-time1))

  }


}
