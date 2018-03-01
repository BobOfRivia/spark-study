package scala.com.spark1.scala.sparkSenior

import scala.com.spark1.scala.SparkComm

/**
  * Created by JACK on 2018/2/17.
  */
object SecSortMain extends SparkComm{

    def main(args: Array[String]): Unit = {
      val sc = init("SecSortMain")
      val lines = sc.textFile("D:\\LearnWSpace\\Language\\scala\\scalaWkspace\\spark-study\\src\\main\\scala\\com\\spark1\\scala\\sparkSenior\\SecSort.txt")
      lines.map { line =>
        (new SecSort(line.split(" ")(0).toInt, line.split(" ")(1).toInt), line)
        //很显然在sortByKey的时候，会调用Ordered 的 compare方法 通过这点来实现二次排序
      }.sortByKey().map(s => s._2).foreach(printMap(_))
    }
}
