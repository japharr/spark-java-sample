package com.japharr.sample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

// Reading a file from HDFS line by line
public class Main3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("readingFileContent")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the content of a file
        JavaRDD<String> fileContentRdd = sc.textFile("hdfs://localhost:9000/ducks/ducks.csv");

        // Print the content of the file line by line
        fileContentRdd.collect().forEach(System.out::println);

        sc.close();
    }
}
