package com.stock.offline

import org.apache.spark.sql.SparkSession

/**
  * 股票数据提取
  */
object datashower {

  /**
    *
    * @param args {0-代码}
    */
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("datapaser").master("local[2]").getOrCreate()
    val df1 = spark.read.json("file:///D://stock-message-json.txt")
    df1.createGlobalTempView("t_stock")
    df1.cache()
    spark.sql("select col1,col3,col8,col9  from global_temp.t_stock where code='%s'".format(args(0))).toJSON
  }


}
