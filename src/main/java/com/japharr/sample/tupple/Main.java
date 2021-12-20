package com.japharr.sample.tupple;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple1$;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(4);
        inputData.add(16);
        inputData.add(9);
        inputData.add(100);

        // logger
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Spark Configuration
        SparkConf conf = new SparkConf()
            .setAppName("startingSpark") // app name
            .setMaster("local[*]"); // use all the available cores on the machine

        // A connection to Spark Cluster
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the inputData to RDD
        JavaRDD<Integer> source = sc.parallelize(inputData);

        // Create a square of the data using reduce()
        JavaRDD<Tuple2<Integer, Double>> squareRdd = source.map(a -> new Tuple2(a, Math.sqrt(a)));
    }
}
