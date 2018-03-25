package com.stock.realtime.Consumer

import java.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010._

object StreamingConsumer {

  case class StockTimerBean()

  def main(args: Array[String]): Unit = {

    val checkpointPath = "hdfs://bogon:8020/stock/checkpoint"
    //初始化Kafka配置信息
    //http://kafka.apache.org/documentation.html#newconsumerconfigs
    val topics =Array[String]("stocktpc")
    val kafkaParam = collection.Map[String,String](("bootstrap.servers"->"bogon:9092,bogon:9093,bogon:9094"),("key.deserializer" ->"org.apache.kafka.common.serialization.StringDeserializer"),
      ("value.deserializer" ->"org.apache.kafka.common.serialization.StringDeserializer"),("group.id" ->"stockgp"))

    //使用KafkaUtils.createStream创建DStream
    val sc = new SparkContext(new SparkConf().setAppName("STConsumerSpark").setMaster("local[2]"))

    //LocationStrategies

    def getStreamingContext():StreamingContext = {
      val ssc = new StreamingContext(sc,Seconds(10))
      val consumers: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe[String,String](topics,kafkaParam)
      val dstream: InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,consumers)
      //there is /n at the tail ,so reg end with \n
      val reg=  "var hq_str_(.*)=\"(.*)\";\n".r

      //reg to unapply into elements
      //对DStream进行算子操作
      dstream.map{
        map =>
          val value =reg.unapplySeq(map.value()).getOrElse(List("",""))
          // save to hive
          (value(0),value(1).split(","))
      }

      ssc.checkpoint(checkpointPath)
      ssc

    }

    //Manage the "offset" by streaming self!!
   val ssc = StreamingContext.getActiveOrCreate(checkpointPath=checkpointPath,creatingFunc=getStreamingContext)


    ssc.start()
    ssc.awaitTermination()

    ssc.stop()

    def saveDataTohive(tp:(String,Array[String])): Unit ={

      var createsql = " create EXTERNAL TABLE IF NOT EXIST "


    }


  }
}
