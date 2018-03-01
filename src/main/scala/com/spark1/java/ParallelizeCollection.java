package scala.com.spark1.java;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by JACK on 2018/2/15.
 */
public class ParallelizeCollection {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("ParallelizeCollection").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> nums = Arrays.asList(new Integer[]{1,2,3,4,5,6,7,8,9,10});

        JavaRDD<Integer> rdd = sc.parallelize(nums);
        Integer sum = rdd.reduce(((v1, v2) -> v1+v2));
        System.out.println(sum);

    }

}
