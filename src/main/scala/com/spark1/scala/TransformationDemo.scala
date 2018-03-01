package scala.com.spark1.scala;

/**
 * Created by JACK on 2018/2/17.
 */
object TransformationDemo extends SparkComm{

    def main(args: Array[String]): Unit = {

        var sc = init("SparkComm")

        var rdd = sc.parallelize(Array(1,2,3,4,5,6,7))
        //map 几个元素乘以2
        /**
          * map对任何类型的RDD都可以使用
          */
        rdd.map(_*2).collect.foreach(printMap _)

        //filter 过滤出集合中的偶数
        rdd.filter(_%2 ==0).foreach(printMap _)

        //flatMap 将行拆分为单词
        var rdd1 = sc.parallelize(Array("hello world","hello jack","hello me and you"))
        rdd1.flatMap(_.split(" ")).foreach(printMap _)

        //groupByKey || groupBy 将每个班级的成绩进行分组
        /**
          * 聚合，多个合并为一个集合
          */
        var rdd2 = sc.parallelize(Array(("CLASS1","A"),("CLASS2","A"),("CLASS3","D"),("CLASS4","C"),("CLASS5","C"))).groupBy(_._2).foreach(println(_))

        //reduceByKey
        /**
          * 以每个key为组，组内进行reduce
          */
        var rdd3 = sc.parallelize(Array(("CLASS1",1),("CLASS2",1),("CLASS3",1),("CLASS4",1),("CLASS1",1)))
        rdd3.reduceByKey(_+_).foreach(println(_))

        //join
        /**
          * Java需要使用parallelizePair
          * 类似于Sql中的Join
          */
        var student  = sc.parallelize(Array((1,"Jack"),(2,"Bob"),(7,"Lily"),(10,"Link")))
        var score  = sc.parallelize(Array((1,98),(2,23),(7,100),(6,67),(1,888)))
        student.join(score).foreach(printTuple(_))

        /**
          * leftOuterJoin就相当于Mysql的 leftJoin
          */
        student.leftOuterJoin(score).foreach(printTuple(_))

        /**
          * cogroup 与 Join的区别在于 cogroup join出多个，会放在一个Iterable中
          * 从效果看，cogroup更类似与 leftOuterJoin
          */
        student.cogroup(score).foreach(printTuple(_))
    }



}
