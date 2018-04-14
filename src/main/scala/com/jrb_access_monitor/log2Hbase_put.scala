package com.jrb_access_monitor

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 采用hbase put 将RDD导入Hbase
  */
class log2Hbase_put {

  def main(args: Array[String]): Unit = {
    //1配置
    //1.1 Spark配置
    val conf = new SparkConf().setAppName("accessCleaner").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //1.2 Hbase配置
    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.property.clientPort","2181")
    hconf.set("hbase.zookeeper.quorum", "master")
    val tableName = "log_aceess";

    //2连接
    //2.1 Hbase连接！！！
    val conn = ConnectionFactory.createConnection(hconf)
    var admin = conn.getAdmin;
    val userTable = TableName.valueOf(tableName)

    //2.2 创建 user 表
    val tableDescr = new HTableDescriptor(userTable)
    tableDescr.addFamily(new HColumnDescriptor("log_parser".getBytes))
    if (admin.tableExists(userTable)) {
      //删除表并重新导入
      admin.disableTable(userTable)
      admin.deleteTable(userTable)
    }
    admin.createTable(tableDescr)
    println("Done!")

    //2.3 获取表连接
    val table = conn.getTable(TableName.valueOf(tableName));

    //3 RDD 处理
    val fileRDD: RDD[String] = sc.textFile("/srcdata/yifs-apache-log/20180411/logs/access_log")
    val regmatch = "(.*) (.*) (.*) \\[(.*)\\] \\\"(.*) (.*) (.*)\\\" (.*) (.*)".r
    val rdd1: RDD[List[String]] = fileRDD.map(regmatch.unapplySeq(_).getOrElse(List("--")))
    //采用HBase的API 将RDD导入HBase
    rdd1.foreachPartition{
      per =>
        val puts= ArrayBuffer[Put]()
        for(el <- per){
          // 时间作为RowKey
          val put: Put = new Put(el(3).getBytes)
          //public KeyValue(row: Array[Byte], family: Array[Byte], qualifier: Array[Byte], value: Array[Byte]) {
          val IpAddr =new KeyValue(el(3).getBytes,"client".getBytes(),"IpAddr".getBytes(),el(0).getBytes())
          val Method =new KeyValue(el(3).getBytes,"client".getBytes(),"Method".getBytes(),el(4).getBytes())
          val Page =new KeyValue(el(3).getBytes,"client".getBytes(),"Page".getBytes(),el(5).getBytes())
          val Prot =new KeyValue(el(3).getBytes,"client".getBytes(),"Prot".getBytes(),el(6).getBytes())

          val respCode =new KeyValue(el(3).getBytes,"server".getBytes(),"respCode".getBytes(),el(7).getBytes())
          val ThreadCode =new KeyValue(el(3).getBytes,"server".getBytes(),"ThreadCode".getBytes(),el(8).getBytes())

          put.add(IpAddr).add(Method).add(Page).add(Prot).add(respCode).add(ThreadCode)
          puts += put
        }

        //scala 集合隐式转换为JAVA 集合  。
        // scala和java类型之间的转换，并不会发生拷贝
        //https://www.jianshu.com/p/740cce989f27
        import scala.collection.JavaConversions._
        //如Hbase
        table.put(puts)
    }



    //==================================OVER============================================



    //计划使用Spark API 将DataFrame导入HBase。发现没有现成的API（弃）
//    val rdd2: RDD[Row] = rdd1.map{
//      list=>
//        Row(list(0),list(1),list(2),list(3),list(4),list(5),list(6),list(7),list(8))
//    }
//
//    //filter localhost
//    var rdd3: RDD[Row] = rdd2.filter(_(0).equals("127.0.0.1")).filter(_(0).equals("localhost"))
//
//    //save to hbase
//    var sqlsession = SparkSession.builder().config(conf).getOrCreate()
//    val st =  StructType.apply(Array(StructField("IpAddr",StringType),StructField("bak1",StringType),StructField("bak2",StringType),StructField("Time",DateType),StructField("Method",StringType),StructField("page",StringType),StructField("Method",StringType),StructField("Prot",StringType),StructField("code1",StringType)))
//    val df = sqlsession.createDataFrame(rdd3,st)


  }

}
