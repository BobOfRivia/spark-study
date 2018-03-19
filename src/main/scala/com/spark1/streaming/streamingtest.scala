package com.spark1.streaming

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object streamingtest {

  def main(args: Array[String]): Unit = {


    //1. spark context
    var sc = new SparkContext(new SparkConf().setAppName("streamingtest").setMaster("local[2]"))


    //2. streaming context
    var ssc =new StreamingContext(sc,Seconds(10))

    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)

    //3. compute
    val result = ds.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map(l => (l,1))
        .reduceByKey(_+_)

    result.print()
    result.saveAsTextFiles("/tmp/resutl",new java.util.Date().getTime.toString)

    //4.start sparkstreaming
    ssc.start()

    //5. close friendly
    new Thread(new Runnable {
      override def run(): Unit =
      {
        var isRunning = true
        while(isRunning){
          // to decide if need to close
          val judge = false
          if(!judge){
           Thread.sleep(60000)
          }else{
            isRunning=false
          }
        }
        ssc.stop()
      }

    }).start()

    println("prepare to stop streaming")
    ssc.awaitTermination()
    ssc.stop()

    println("already stop streaming")

  }
}
