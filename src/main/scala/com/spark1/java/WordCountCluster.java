package scala.com.spark1.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Spark 脚本集群配置
 *      --修改文件路径
 *      --打jar包
 *      --jar包提交至可连接Spark集群的服务器
 *
 * shell执行脚本配置
 *      /usr/local/spark/bin/spark-submit \
 *      --class com.spark.WordCountCluster \
 *      --num-executor 3 \
 *      --dirver-memory 100m \
 *      --executor-memmory 100m \
 *      --executor-core 3 \
 *      /usr/xxxxx[jar包路径] \
 */
public class WordCountCluster {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("WordCountLocal");
//                .setMaster

        JavaSparkContext jsc = new JavaSparkContext(conf);

//        JavaRDD<String> lines = jsc.textFile("D:\\my.cnf");
        JavaRDD<String> lines = jsc.textFile("hdfs://hadoop02:9000/xxx.txt");

        JavaRDD<String> words =  lines.flatMap((s) ->{return Arrays.asList(s.split(" ")).iterator();});

        JavaPairRDD<String,Integer> pairs = words.mapToPair((s) ->{  return new Tuple2<String, Integer>(s,1);});

        JavaPairRDD<String,Integer> wordcounts = pairs.reduceByKey((v1, v2) ->  {return v1+v2;});

        wordcounts.foreach((tp)->{System.out.println(tp._1+"  "+tp._2);});

    }

}
