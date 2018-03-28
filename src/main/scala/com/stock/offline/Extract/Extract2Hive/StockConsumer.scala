package com.stock.offline.Extract.Extract2Hive

import java.sql.DriverManager
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}
import org.joda.time.Minutes

class StockConsumer {

  def main(args: Array[String]): Unit = {

    //init Kafka
    var kafkaParam = collection.Map[String, Object]("bootstrap.servers"->"bogon:9092,bogon:9093,bogon:9094"),("key.deserializer" ->"org.apache.kafka.common.serialization.StringDeserializer"),
    ("value.deserializer" ->"org.apache.kafka.common.serialization.StringDeserializer"),("group.id" ->"stockgp"))
    //1. decide to use receiver or direct(direct means offset managed by spark then read data by spark)
      //i decide to use direct because i need to get whole-data safely-with-easily
    val consumerStategies: ConsumerStrategy[String, String] =  ConsumerStrategies.Subscribe(Array("offlinestock"),kafkaParam)

    val checkpointPath = "hdfs://127.0.0.1:8020/stock/offline/consumer"

    def doStreaming(): StreamingContext = {
      val ssc = new StreamingContext(new SparkConf().setMaster("local[2]").setAppName("StockOfflineConsumer"), Minutes(5))
      var consumer: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,consumerStategies)

      //connection to hive
      val hive = DriverManager.getConnection("jdbc:hive2://127.0.0.1:xxxx","root","541325")
      hive.setAutoCommit(true)
      consumer.foreachRDD{
        rdd =>
          var hql=""

          rdd.foreach{
            el=>
              hql += ""

          }
          hive.prepareStatement(hql).execute()
      }
      ssc.checkpoint(checkpointPath)
      ssc
    }
    val ssc = StreamingContext.getActiveOrCreate(checkpointPath=checkpointPath,creatingFunc=doStreaming)
    //2.start
    ssc.start()
    ssc.awaitTermination()

    //use spark-streaming to get batch-data from kafka

    //once get batch-data save them to hive,so the batch must be very hug and comfortable



  }

}
