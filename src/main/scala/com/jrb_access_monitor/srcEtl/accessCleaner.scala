package com.jrb_access_monitor.srcEtl

import java.io

import org.apache.hadoop.fs.shell.CopyCommands.Put
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

class accessCleaner {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("accessCleaner").setMaster("local[2]")

    val sc = new SparkContext(conf)

    var fileRDD: RDD[String] = sc.textFile("/srcdata/yifs-apache-log/20180411/logs/access_log")

    println("getAllFileCount ==== "+fileRDD.count())

    val regmatch = "(.*) (.*) (.*) \\[(.*)\\] \\\"(.*) (.*) (.*)\\\" (.*) (.*)".r

    val rdd1: RDD[List[String]] = fileRDD.map{
      line=>
        val a: List[String] = regmatch.unapplySeq(line).getOrElse(List("--"))
        a
    }



    //
    val rdd2: RDD[Row] = rdd1.map{
      list=>
        Row(list(0),list(1),list(2),list(3),list(4),list(5),list(6),list(7),list(8))
    }

    //filter localhost
    var rdd3: RDD[Row] = rdd2.filter(_(0).equals("127.0.0.1")).filter(_(0).equals("localhost"))

    //save to hbase
    var sqlsession = SparkSession.builder().config(conf).getOrCreate()
    val st =  StructType.apply(Array(StructField("IpAddr",StringType),StructField("bak1",StringType),StructField("bak2",StringType),StructField("Time",DateType),StructField("Method",StringType),StructField("page",StringType),StructField("Method",StringType),StructField("Prot",StringType),StructField("code1",StringType)))
    val df = sqlsession.createDataFrame(rdd3,st)


  }

}
