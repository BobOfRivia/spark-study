package com.stock.realtime.Producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scalaj.http.{Http, HttpRequest, HttpResponse}

/**
  * TODO Producer need to be reconstruct
  */
class StockTimeProducer {


  /**
    *
    *
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    var onestockUrl = "%s"

    val  stockList =Array[String]()

    val topic ="stocktpc"


    //1. construct KafkaProducer
    // 1. 创建一个Producer对象
    // 1.1 构建一个Properties以及给定连接kafka的相关producer的参数
    val props: Properties = new Properties
    // a. 给定kafka的服务路径信息
    props.put("metadata.broker.list", "hadoop-senior01.ibeifeng.com:9092,hadoop-senior01.ibeifeng.com:9093,hadoop-senior01.ibeifeng.com:9094,hadoop-senior01.ibeifeng.com:9095")
    // b. 给定数据发送是否等待broker返回结果, 默认为0表示不等待
    props.put("request.required.acks", "0")
    // c. 给定数据发送方式，默认是sync==>同步发送数据，可以修改为异步发送数据(async)
    props.put("producer.type", "sync")
    // d. 给定消息序列化为byte数组的方式，默认为: kafka.serializer.DefaultEncoder, 默认情况下，要求Producer发送的数据类型是byte数组；如果发送string类型的数据，需要给定另外的Encoder编码器
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    // e. 数据发送默认采用hash的机制决定消息发送到那一个分区中，默认值为: kafka.producer.DefaultPartitioner, 参数为: partitioner.class
    // TODO: 有时候需要根据业务的需要自定义一个数据分区器
    props.put("partitioner.class", "com.ibeifeng.kafka.producer.PartitionerDemo")

    var producer = new KafkaProducer[String,String](props)

    //new two thread to pick stock message
    new Thread(() => {
      for(stock <- stockList){

        //2.make ProducerRecord
        val resp: HttpResponse[String] = Http(onestockUrl.format(stock)).asString
        val pr = new ProducerRecord[String,String]("stocktpc",stock,resp.body)

        //3.send to kafka
        producer.send(pr)

        //4. have a rest
          Thread.sleep(10000)
      }
    }).start()

    //5. make a jvm hook,while jvm closed this guy dead
    Runtime.getRuntime.addShutdownHook(new Thread(()=>{
      println("JVM  CLOSED , SO PRODUCER WILL CLOSE")
      producer.close()
    }))

  }


}
