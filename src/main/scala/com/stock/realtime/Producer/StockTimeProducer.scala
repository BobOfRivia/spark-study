package com.stock.realtime.Producer

import java.sql.{DriverManager, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}
import java.util.concurrent.{ExecutorService, Executors}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import scalaj.http.{Http, HttpRequest, HttpResponse}

import scala.collection.mutable.ArrayBuffer

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

    var onestockUrl = "http://hq.sinajs.cn/list=sh%s"

//    var sc = SQLContext(new SparkContext(new SparkConf().setAppName("StockTimeProducer").setMaster("local")))

    var dm = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/lifeblog?user=root&password=541325&serverTimezone=GMT")
    var result: ResultSet =dm.prepareStatement(" select stock_id from t_stock_list ").executeQuery()

    val timeOfOncePick = 60*1000*10 //10min


    val  stockList =ArrayBuffer[String]()

    while(result.next()){
      stockList.+=(onestockUrl.format(result.getString(0)))
    }

    val topic ="stocktpc"


    //1. construct KafkaProducer
    // 1. 创建一个Producer对象
    // 1.1 构建一个Properties以及给定连接kafka的相关producer的参数
    val props: Properties = new Properties
    // a. 给定kafka的服务路径信息
    props.put("metadata.broker.list", "bogon:9092,bogon:9093,bogon:9094")
    // b. 给定数据发送是否等待broker返回结果, 默认为0表示不等待
    props.put("request.required.acks", "0")
    // c. 给定数据发送方式，默认是sync==>同步发送数据，可以修改为异步发送数据(async)
    props.put("producer.type", "sync")
    // d. 给定消息序列化为byte数组的方式，默认为: kafka.serializer.DefaultEncoder, 默认情况下，要求Producer发送的数据类型是byte数组；如果发送string类型的数据，需要给定另外的Encoder编码器
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    // e. 数据发送默认采用hash的机制决定消息发送到那一个分区中，默认值为: kafka.producer.DefaultPartitioner, 参数为: partitioner.class
    // TODO: 有时候需要根据业务的需要自定义一个数据分区器
//    props.put("partitioner.class", "com.ibeifeng.kafka.producer.PartitionerDemo")

    val producer = new KafkaProducer[String,String](props)

    //new two thread to pick stock message
    val threadpool: ExecutorService = Executors.newFixedThreadPool(4)

    val calendar = Calendar.getInstance()
    calendar.set(Calendar.HOUR_OF_DAY,9)
    calendar.set(Calendar.MINUTE,15)
    val start1 = calendar.getTimeInMillis
    calendar.set(Calendar.HOUR_OF_DAY,11)
    calendar.set(Calendar.MINUTE,30)
    val end1 = calendar.getTimeInMillis

    calendar.set(Calendar.HOUR_OF_DAY,13)
    calendar.set(Calendar.MINUTE,0)
    val start2 = calendar.getTimeInMillis
    calendar.set(Calendar.HOUR_OF_DAY,15)
    val end2 = calendar.getTimeInMillis

    //judge sleep time
    def timeBlockJudge(now :Long): Long ={
      if(now < start1){
        println("today's stock haven't start!")
        start1 -now
      }else if((now >start1 && now <=end1) || (now >start2 && now <=end2)){
        timeOfOncePick
      }else if(now >end1 && now < start2){
        println("today's morning stock is over ,wait to afternoon")
        start2 - now
      }else{
        println("today's  stock is over ")
        -1
      }
    }


    //main function of producer
    def runStockProducer(): Unit ={
      for(stock <- stockList){
        threadpool.execute(() =>{
          //2.make ProducerRecord
          val resp: HttpResponse[String] = Http(onestockUrl.format(stock)).asString
          val pr = new ProducerRecord[String,String](topic,stock,resp.body)

          //3.send to kafka
          producer.send(pr)

          Thread.sleep(500)

        })
      }
    }

    while(true){
      runStockProducer();

      var now: Long =  Calendar.getInstance().getTimeInMillis

      var waittime = timeBlockJudge(now)
      if(waittime == -1){
        println("stock time over")
        //TODO: how about other thread haven't finished this time??
        System.exit(-1)
      }
      //4. have a rest
      Thread.sleep(waittime)

    }





    //5. make a jvm hook,while jvm closed this guy dead
    Runtime.getRuntime.addShutdownHook(new Thread(()=>{
      println("JVM  CLOSED , SO PRODUCER WILL CLOSE")
      producer.close()
    }))

  }


}
