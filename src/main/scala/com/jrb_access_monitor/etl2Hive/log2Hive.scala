package com.jrb_access_monitor.etl2Hive



import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{ StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by JACK on 2018/4/14.
  */
object log2Hive {

  def main(args: Array[String]): Unit = {
    val prs = new java.text.SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss")
    val fmt = new java.text.SimpleDateFormat("yyyyMMddHHmmss")

    val conf = new SparkConf().setAppName("log2Hive").setMaster("local[2]")
    val sc = new SparkContext()
    val fileRDD: RDD[String] = sc.textFile("hdfs://bogon:8020/srcdata/yifs-apache-log/20180411/logs/access_log")
    val regmatch = "(.*) (.*) (.*) \\[(.*)\\] \\\"(.*) (.*) (.*)\\\" (.*) (.*)".r
    val recordRdd: RDD[List[String]] = fileRDD.map(line => regmatch.unapplySeq(line).getOrElse(List(line)))

    val filtered = recordRdd.filter(_.length < 9)
    //记录被过滤的记录,用于调试
    filtered.saveAsTextFile("hdfs://bogon:8020/srcdata/yifs-apache-log/20180411/parse_result/access_log_filted")


    //alived to hive
    val alived = recordRdd.filter(_.length==9)
    val rdd2: RDD[Row] = alived.map{
      list=>
        Row(list(0),list(1),list(2),fmt.format(prs.parse(list(3))),list(4),list(5),list(6),list(7),list(8))
    }

    //save to hive
    val sqlsession: SparkSession = SparkSession.builder.appName("log2Hive_hiveMission").master("local[2]").getOrCreate()
    val st =  StructType.apply(Array(StructField("IpAddr",StringType),StructField("bak1",StringType),StructField("bak2",StringType),StructField("Time",StringType),StructField("Method",StringType),StructField("page",StringType),StructField("Prot",StringType),StructField("threadCode",StringType)))
    val df: DataFrame = sqlsession.createDataFrame(rdd2,st)
    df.write.mode(org.apache.spark.sql.SaveMode.Overwrite).saveAsTable("hive_log_access")


  }


}
