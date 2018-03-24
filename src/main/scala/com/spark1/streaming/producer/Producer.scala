package com.spark1.streaming.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object Producer {

  def main(args: Array[String]): Unit = {

    var props = new Properties()

    // a. 给定kafka的服务路径信息
    props.put("metadata.broker.list", "localhost:9092,localhost:9093,localhost:9094")
    // b. 给定数据发送是否等待broker返回结果, 默认为0表示不等待
    props.put("request.required.acks", "0")
    // c. 给定数据发送方式，默认是sync==>同步发送数据，可以修改为异步发送数据(async)
    props.put("producer.type", "sync")
    // d. 给定消息序列化为byte数组的方式，默认为: kafka.serializer.DefaultEncoder, 默认情况下，要求Producer发送的数据类型是byte数组；如果发送string类型的数据，需要给定另外的Encoder编码器
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    // e. 数据发送默认采用hash的机制决定消息发送到那一个分区中，默认值为: kafka.producer.DefaultPartitioner, 参数为: partitioner.class
    // TODO: 有时候需要根据业务的需要自定义一个数据分区器
//    props.put("partitioner.class", "com.ibeifeng.kafka.producer.PartitionerDemo")


    //1.构建Producer对象
    var producer = new KafkaProducer[String,String](props)


    //2.topic: String, partition: Integer,  key: K, value: V
    val topic = "kafka0"
    val random = new Random()
    val namelist = ArrayBuffer("Gakki","Gakki","Gakki","Hello","World","Spark","Spark")

    for(i <- 0 to 3){
      new Thread(() =>{
//        var proRecord = new ProducerRecord[String,String]()
        val key = random.nextInt(100)

        val wordnum = random.nextInt(10)

        var str = new StringBuilder
        for(j <- 1 to wordnum){
          str.append(namelist(random.nextInt(namelist.length-1))).append(" ")
        }

        val value =str.toString()
        val proRd = new ProducerRecord[String,String](topic,key%3,key.toString,value)

        producer.send(proRd)

        try{
          Thread.sleep(random.nextInt(50)+10)
        }catch{
          case e:Exception => println(e)
        }
      })

      Runtime.getRuntime.addShutdownHook(new Thread(()=>{
        println("Producer  closed with jvm dead")
        producer.close()
      }))


    }



  }

}
