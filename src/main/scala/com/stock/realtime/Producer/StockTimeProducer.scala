package com.stock.realtime.Producer

import java.sql.{DriverManager, ResultSet}
import java.util.{Calendar, Properties}
import java.util.concurrent.{ExecutorService, Executors}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scalaj.http._

import scala.collection.mutable.ArrayBuffer

/**
  * TODO Producer need to be reconstruct
  */
object StockTimeProducer {


  /**
    *
    *
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    var onestockUrl = "http://hq.sinajs.cn/list=%s%s"

//    var sc = SQLContext(new SparkContext(new SparkConf().setAppName("StockTimeProducer").setMaster("local")))

    var dm = DriverManager.getConnection("jdbc:mysql://192.168.20.1:3306/lifeblog?user=root&password=541325&serverTimezone=GMT")
    var result: ResultSet =dm.prepareStatement(" select stock_id,belong_typ from t_stock_list ").executeQuery()

    val timeOfOncePick = 60*1000*10 //10min


    val  stockList =ArrayBuffer[String]()

    while(result.next()){
      stockList.+=(onestockUrl.format(result.getString(2),result.getString(1)))
    }

    val topic ="stocktpc"

//http://kafka.apache.org/documentation.html#producerconfigs
    //1. construct KafkaProducer
    // 1. 创建一个Producer对象
    // 1.1 构建一个Properties以及给定连接kafka的相关producer的参数
    val props: Properties = new Properties
    // a. 给定kafka的服务路径信息
    props.put("bootstrap.servers", "bogon:9092,bogon:9093,bogon:9094")
    // b. 给定数据发送是否等待broker返回结果, 默认为0表示不等待
    props.put("request.required.acks", "0")
    // c. 给定数据发送方式，默认是sync==>同步发送数据，可以修改为异步发送数据(async)
    props.put("producer.type", "sync")
    // d. 给定消息序列化为byte数组的方式，默认为: kafka.serializer.DefaultEncoder, 默认情况下，要求Producer发送的数据类型是byte数组；如果发送string类型的数据，需要给定另外的Encoder编码器
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // e. 数据发送默认采用hash的机制决定消息发送到那一个分区中，默认值为: kafka.producer.DefaultPartitioner, 参数为: partitioner.class
    // TODO: 有时候需要根据业务的需要自定义一个数据分区器
//    props.put("partitioner.class", "com.ibeifeng.kafka.producer.PartitionerDemo")

    val producer = new KafkaProducer[String,String](props)

    //new two thread to pick stock message
    val threadpool: ExecutorService = Executors.newFixedThreadPool(1)

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
    calendar.set(Calendar.HOUR_OF_DAY,20)
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
        threadpool.execute(new Runnable {
          override def run(): Unit = {
            //2.make ProducerRecord (rewrite for charset)
            val resp: HttpResponse[String] = new HttpRequest( url = stock,
              method = "GET",
              connectFunc = (req, conn) => conn.connect,
              params = Nil,
              headers = Seq("User-Agent" -> "scalaj-http/1.0"),
              options = HttpConstants.defaultOptions,
              proxyConfig = None,
              charset = "GB2312",
              sendBufferSize = 4096,
              urlBuilder = (req) => HttpConstants.appendQs(req.url, req.params, req.charset),
              compress = true).asString
            println("url:"+stock+" sending msg:"+resp.body+" to topic"+topic)
            val pr = new ProducerRecord[String,String](topic,stock,resp.body)

            //3.send to kafka
            producer.send(pr)

            Thread.sleep(5000)

          }
        } )
      }
    }

    //main Task Controller  with time-step
    while(true){
      runStockProducer();

      val now: Long =  Calendar.getInstance().getTimeInMillis

      val waittime = timeBlockJudge(now)
      if(waittime == -1){
        println("stock time over")
        //TODO: how about other thread haven't finished this time??
        System.exit(-1)
      }
      //4. have a rest
      Thread.sleep(waittime)

    }





    //5. make a jvm hook,while jvm closed this guy dead
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
        println("JVM  CLOSED , SO PRODUCER WILL CLOSE")
        producer.close()
      }
    }))

  }


}
