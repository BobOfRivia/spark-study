package com.spark1.streaming.consumer

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Consumer {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("streaming-consumer").setMaster("local[3]"))

    val ssc = new StreamingContext(sc,Seconds(10))

    val topic = "kafka0"

    var kafkaParam = new java.util.HashMap[String,String]()

    //1. group id
    //2. topic
    //3. zk connect
    //4.auto.offset.reset
    //5.zk timeout
    kafkaParam.put("auto.offset.reset","latest")
    kafkaParam.put("group.id","kafkagp0")
    kafkaParam.put("bootstrap.servers","localhost:9092,localhost:9093")

    var topics = Map("kafka0" -> 3)

    KafkaUtils.createStream(ssc,kafkaParam,topics,StorageLevel.MEMORY_AND_DISK_2)


  }

}
