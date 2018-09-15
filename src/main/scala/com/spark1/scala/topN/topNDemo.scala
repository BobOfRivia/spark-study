package com.spark1.scala.topN

import com.spark1.scala.secsort.secbean
import org.apache.spark.{SparkConf, SparkContext}
/**
  * TOPN算法
  */
class topNDemo {

}

object topNDemo{

  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setAppName("topNDemo").setMaster("local")

    var sc = new SparkContext(conf)
    var file = sc.textFile("D:\\LearnWSpace\\Language\\scala\\scalaWkspace\\spark-study\\src\\main\\scala\\com\\spark1\\scala\\topN\\topn")
    var rdd1 =  file.filter(!_.startsWith(" ")).filter(!_.startsWith("考号"))
    var rdd2 =  file.filter(_.startsWith(" ")).map(_.replace(" ",""))
    var filt = rdd1.union(rdd2)
    var data = filt.map( line => {
      var word = line.split("\t");
      (word(0),word(1),word(2),word(3),word(4))
    })
    println("====语文TOP5")
    var lanTop5 = data.map(line => (line._1,line._2,line._3)).sortBy(_._3,false).take(5).foreach(println(_))
    println("====数学TOP5")
    var mathTop5 = data.map(line => (line._1,line._2,line._4)).sortBy(_._3,false).take(5).foreach(println(_))
    println("====英语TOP5")
    var EnTop5 = data.map(line => (line._1,line._2,line._5)).sortBy(_._3,false).take(5).foreach(println(_))

    val rdd10 = sc.textFile("file:///Users/guhongjie/IdeaProjects/spark-study/src/main/scala/com/spark1/scala/topN/topn")

    class scorebean(val no:Int,val name:String,val chinese:Int,val math:Int,val en:Int,val total:Int,val rank:Int,val delta:String)  extends Ordered[scorebean] with Serializable{

      override def compare(o: scorebean): Int = {
        val deltatotal = this.total - o.total
        if(deltatotal == 0){
          this.math-o.math
        }else{
          deltatotal
        }
      }

      override def toString: String = {

        this.name +"  "+this.total+"  "+this.math
      }
    }


    val sortedrdd = rdd10.filter(!_.startsWith("考号")).mapPartitions{
      itor =>
        itor.map{
          line=>
            val bean = line.split("\t")
            val scorebean = new scorebean(bean(0).toInt,bean(1),bean(2).toInt,bean(3).toInt,bean(4).toInt,bean(5).toInt,bean(6).toInt,"")
            (scorebean,1)
        }
    }.sortBy(_._1).foreach(println(_))


  }

}



