package scala.com.spark1.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by JACK on 2018/2/13.
 */
public class WordCountLocal {

    public static void main(String[] args) {
        // 编写Spark应用程序
        // 本地可以直接在main方法中运行

        // 第一步：创建SparkConf对象，设置Spark应用程序
        //setMaster() 可以设置Spark集群中 Master节点的url，但如果设置为local则代表，本地运行
        SparkConf conf = new SparkConf()
                .setAppName("WordCountLocal")
                .setMaster("local");

        // 第二步：创建一个JavaSparkContext对象
            // 在spark中，sparkContext是Spark所有功能的一个入口，无论java、scala,甚至是python编写.都必须要一个SparkContext
            // 主要作用：包括初始化Spark应用程序所需的一些核心组件，包括调度器（DAGSchedule,TaskScheduler），还会去到Spark Master节点上进行注册
            // 一句话，SparkContext,是Spark应用中，可以说是最最重要的一个对象
        // Scala -> SparkContext || Java -> JavaSparkContext || Spark SQL -> SQLContext、HiveContext || Spark Streaming -> SparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // 第三步：要针对输入源（hdfs文件，本地文件，等等），创建一个初始的RDD。
        // 输入源中的数据会打散，分配到RDD中的各个Partition中，从而形成一个初始的分布式数据集
        //SparkContext中，用于根据文件类型的输入源创建RDD的方法，叫做textFile()方法
        //在Java中，创建普通的RDD，叫做JavaRDD

        JavaRDD<String> lines = jsc.textFile("D:\\my.cnf");

        // 第四步：对初始的RDD进行transformation操作，也就是一些计算操作
        // 通常操作会通过创建function,并配合RDD的map、flagMap等算子来执行
        // 如果function通常比较简单，则创建指定Function的匿名内部类
        // 如果function比较复杂，则单独创建一个类实现function接口

        //FlatMapFunction，有两个泛型参数，分别代表了输入和输出类型
        // flagMap算子的作用是把RDD一个元素，拆分成一个或多个元素
        //Spark的优势之一，flagMap处理过后，依旧在同一个节点，不存在网络IO
         JavaRDD<String> words =  lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID=1L;
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        //接着，需要将每一个单词，映射为（单词，1）的这种格式
        //只有这样才能以单词作为Key 来进行累加
        JavaPairRDD<String,Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s,1);
                    }
                });

        //接着,需要以单词作为Key,统计每个单词出现的次数
        //这里要使用reduceByKey这个算子,对每个key对应的value,都进行reduce操作
        //与scala中的reduce操作类似,挨个执行
        JavaPairRDD<String,Integer> wordcounts = pairs.reduceByKey((v1, v2) ->  {return v1+v2;});

        //通过三个Spark算子操作,实现了wordcount
        wordcounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tp) throws Exception {
                System.out.println(tp._1+"  "+tp._2);
            }
        });


    }

}
