package scala.com.spark1.scala

/**
  * Created by JACK on 2018/2/13.
  */
class wc1 {

}

/**
  * 使用Java的方式开发进行本地测试Spark的WordCount程序
  * @author DT大数据梦工厂
  * http://weibo.com/ilovepains
  */
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]){
    val conf = new SparkConf()
    conf.setAppName("Wow, My First Spark App!")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("D://tmp//helloSpark.txt", 1)
    val words = lines.flatMap { line => line.split(" ") }
    val pairs = words.map { word => (word,1) }
    val wordCounts = pairs.reduceByKey(_+_)
    wordCounts.foreach(wordNumberPair => println(wordNumberPair._1 + " : " + wordNumberPair._2))
    sc.stop()
  }
}
