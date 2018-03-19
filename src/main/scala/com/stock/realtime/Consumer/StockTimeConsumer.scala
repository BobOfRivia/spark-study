package com.stock.realtime.Consumer

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

/**
  * TODO consumer need to be reconstruct
  */
class StockTimeConsumer {


  def main(args: Array[String]): Unit = {
    val groupId = "stockgp"
    val zkurl = ""
    val largest = true

    // 1. 给定Consumer连接的相关参数
    val props = new Properties
    // a. 给定group id
    props.put("group.id", groupId)
    // b. 给定zk连接url
    props.put("zookeeper.connect", zkurl)
    // c. 给定自动提交offset偏移量间隔时间修改为2s(默认60s)
    props.put("auto.commit.interval.ms", "2000")
    // d. 给定初始化consumer时候的offset值（该值只有在第一次consumer消费数据的时候有效 --> 只要zk中保存了该consumer的offset偏移量信息，那么该参数就无效了）
    if (largest) props.put("auto.offset.reset", "largest")
    else props.put("auto.offset.reset", "smallest")
    // 3. 创建Consumer连接器
    var connector: KafkaConsumer[String, String] = new KafkaConsumer[String,String](props)

    //KafkaConsumer
    var consumerRecords: ConsumerRecords[String, String] = connector.poll(10*1000)

    var itor = consumerRecords.iterator()

    while(itor.hasNext){
      var sb =  new StringBuilder

      var record = itor.next()

      //save metadata
      val offset = record.offset()
      val partitionId= record.partition()
      val topic = record.topic()
      val key = record.key()
      val value = record.value()

      println(offset,partitionId,topic,key,value)
      //todo consume this data
    }

  }

}
