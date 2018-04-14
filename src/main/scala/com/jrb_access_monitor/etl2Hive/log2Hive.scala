package com.jrb_access_monitor.etl2Hive

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by JACK on 2018/4/14.
  */
class log2Hive {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("log2Hive").setMaster("local[2]")

    val sc = new SparkContext()

    val fileRDD: RDD[String] = sc.textFile("/srcdata/yifs-apache-log/20180411/logs/access_log")

    val regmatch = "(.*) (.*) (.*) \\[(.*)\\] \\\"(.*) (.*) (.*)\\\" (.*) (.*)".r

    val recordRdd = fileRDD.map(regmatch.unapplySeq(_).getOrElse(List("--")))

    //记录被过滤的记录,用于调试
    val filtered = recordRdd.filter(_.length==1)
//    filtered.foreach{
//      record =>
//
//    }
    val rdd2: RDD[Row] = filtered.map{
      list=>
        Row(list(0),list(1),list(2),list(3),list(4),list(5),list(6),list(7),list(8))
    }


    //save to hbase
    var sqlsession: SparkSession = SparkSession.builder.appName("log2Hive_hiveMission").master("local[2]").getOrCreate()
    val st =  StructType.apply(Array(StructField("IpAddr",StringType),StructField("bak1",StringType),StructField("bak2",StringType),StructField("Time",DateType),StructField("Method",StringType),StructField("page",StringType),StructField("Prot",StringType),StructField("threadCode",StringType)))
    val df: DataFrame = sqlsession.createDataFrame(rdd2,st)

    df.write.saveAsTable("hive_log_access")

      //排除未匹配上的记录
//      .filter(_.length==1)
      //


  }


}
