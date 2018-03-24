package com.stock.realtime.Consumer

import java.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010._

object STConsumerSpark {

  case class StockTimerBean()

  def main(args: Array[String]): Unit = {

    //初始化Kafka配置信息
    //http://kafka.apache.org/documentation.html#newconsumerconfigs
    val topics =Array[String]("stocktpc")
    val kafkaParam = collection.Map[String,String](("bootstrap.servers"->"bogon:9092,bogon:9093,bogon:9094"),("key.deserializer" ->"org.apache.kafka.common.serialization.StringDeserializer"),
      ("value.deserializer" ->"org.apache.kafka.common.serialization.StringDeserializer"),("group.id" ->"stockgp"))

    //使用KafkaUtils.createStream创建DStream
    val sc = new SparkContext(new SparkConf().setAppName("STConsumerSpark").setMaster("local[2]"))
    val ssc = new StreamingContext(sc,Seconds(10))
    val consumers: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe[String,String](topics,kafkaParam)

    //LocationStrategies
    val dstream: InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,consumers)
    //there is /n at the tail ,so reg end with \n
    val reg=  "var hq_str_(.*)=\"(.*)\";\n".r

    //reg to unapply into elements
    //对DStream进行算子操作
    dstream.map{
      map =>
        val value =reg.unapplySeq(map.value()).getOrElse(List("",""))
        (value(0),value(1).split(","))
    }

    //对结果集进行处理
    ssc.start()
    ssc.awaitTermination()

  }
}
