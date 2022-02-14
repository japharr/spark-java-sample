package com.japharr.sample.pair;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Monday 4 September, 2005");
        inputData.add("INFO: Tuesday 5  September, 2005");
        inputData.add("WARN: Tuesday 5 September, 2005");
        inputData.add("FATAL: Wednesday 6 September, 2005");
        inputData.add("ERROR: Thursday 4 September, 2005");

        // logger
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Spark Configuration
        SparkConf conf = new SparkConf()
            .setAppName("startingSpark") // app name
            .setMaster("local[*]"); // use all the available cores on the machine

        // A connection to Spark Cluster
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the inputData to RDD
        JavaRDD<String> source = sc.parallelize(inputData);

        // Create a square of the data using reduce()
        JavaPairRDD<String, Long> pairRDD = source.mapToPair(a -> {
            String[] splits = a.split(":");
            return new Tuple2<>(splits[9], 1L);
        });

        JavaPairRDD<String, Long>  sumRdd = pairRDD.reduceByKey((a, b) -> a + b);
    }
}
