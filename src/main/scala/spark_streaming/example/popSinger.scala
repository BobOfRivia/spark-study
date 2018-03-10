package spark_streaming.example

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, StreamingContext,Seconds}

/**
  * 实时统计最受欢迎的歌手
  */
object popSinger {

  def main(args: Array[String]): Unit = {

    def addFunc(cur:Seq[Int], prvNum:Option[Int]):Option[Int]={
      Some(cur.sum +  prvNum.getOrElse(0))
    }

    /**
      * 重要：Msater至少要启动两个线程
      * 一个用来接收数据
      * 一个用来执行算子
      */
    var conf = new SparkConf().setMaster("local[2]").setAppName("popSinger")

    // 批处理间隔：以此为一个单位创建一个DStream
    var ssc = new StreamingContext(conf,Seconds(1))
    ssc.checkpoint("G:\\bigData\\checkpoint")
    var ds1 = ssc.socketTextStream("localhost",9912,StorageLevel.MEMORY_AND_DISK_SER).map(l => (l,1))

      //10秒一次存档
      .checkpoint(Seconds(10))
    //第二个参数：窗口间隔，算子处理的数据时间单位 ； 第三个参数：滑动间隔，获取数据窗口的间隔
    //此处的逻辑为：每隔2秒钟统计前10秒的数据
//    var ds2 = ds1.reduceByKeyAndWindow((v1:Int,v2:Int) => v1+v2,Seconds(10),Seconds(2))
//    var ds3 = ds1.reduceByKeyAndWindow(_+_,Seconds(10)).print()
    ds1.updateStateByKey[Int](addFunc _).print()
//    ds2.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
