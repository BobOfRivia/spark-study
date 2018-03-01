package scala.com.spark1.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * 统计每行出现的次数
 */
public class LineCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("LineCount").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        //创建初始RDD
        JavaRDD<String> lines = sc.textFile("D:\\LearnWSpace\\Language\\scala\\scalaWkspace\\spark-study\\src\\main\\scala\\com\\spark1\\java\\LocalFiletest");

        //将每一行映射为（line,1）的这种key-value
        //然后才能统计每行出现的次数
       JavaPairRDD<String,Integer>  pair =  lines.mapToPair((s) -> new Tuple2<String,Integer>(s,1));

       pair.reduceByKey(((v1, v2) -> v1+v2)).foreach((tp)-> System.out.println(tp._1+" "+tp._2));

        sc.close();
    }

}
