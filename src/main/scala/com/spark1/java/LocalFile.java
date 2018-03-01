package scala.com.spark1.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Iterator;

/**
 * Created by JACK on 2018/2/15.
 */
public class LocalFile {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LocalFile").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("D:\\LearnWSpace\\Language\\scala\\scalaWkspace\\spark-study\\src\\main\\scala\\com\\spark1\\java\\LocalFiletest");
        System.out.println(lines.map((s) -> s.length()).reduce((v1,v2) -> v1+v2));
    }
}
