package com.stock.offline.Extract.route2.Source2Hive

import com.stock.offline.Extract.route2.BeanUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SparkConf}
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}

class StockConsumer {

  def main(args: Array[String]): Unit = {

    //init Kafka
    var kafkaParam = collection.Map[String, Object](("bootstrap.servers"->"bogon:9092,bogon:9093,bogon:9094")
    ,("key.deserializer" ->"org.apache.kafka.common.serialization.StringDeserializer"),
    ("value.deserializer" ->"org.apache.kafka.common.serialization.StringDeserializer"),("group.id" ->"stockgp"))

    //1. decide to use receiver or direct(direct means offset managed by spark then read data by spark)
      //i decide to use direct because i need to get whole-data safely-with-easily

    val consumerStategies: ConsumerStrategy[String, Array[Byte]] =  ConsumerStrategies.Subscribe(Array("offlinestock"),kafkaParam)

    val checkpointPath = "hdfs://127.0.0.1:8020/stock/offline/consumer"

    val sparkconf  = new SparkConf().setMaster("local[2]").setAppName("StockOfflineConsumer")

    def doStreaming(): StreamingContext = {
      val ssc = new StreamingContext(sparkconf, Minutes(5))
      var consumer: InputDStream[ConsumerRecord[String, Array[Byte]]] = KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,consumerStategies)

      //connection to hbase
//      val hive = DriverManager.getConnection("jdbc:hive2://127.0.0.1:xxxx","root","541325")

        val sparkSession = SparkSession.builder().config(sparkconf).getOrCreate()

      val rdd: Unit = consumer.foreachRDD{
        rdd =>
          rdd.foreach{
            el=>
              BeanUtils.BytesToObject(el.value()).asInstanceOf[StockEl]
          }
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
