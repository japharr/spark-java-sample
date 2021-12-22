package com.japharr.sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.FileNotFoundException;
import java.io.IOException;

// Reading a whole file from HDFS
public class Main2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("readingHDFS").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // read a whole file
        JavaPairRDD<String, String> myRdd = sc.wholeTextFiles("hdfs://localhost:9000/ducks/ducks.csv");

        // print the content of the file
        myRdd.foreach(r -> System.out.println("value: " + r._2));

        // print the file path
        myRdd.foreach(r -> System.out.println("value: " + r._1));

        sc.close();
    }

    // get hdfs file size
    public static long getFileSize(String filePath) throws IOException, FileNotFoundException {
        Configuration config = new Configuration();
        Path path = new Path(filePath);
        FileSystem hdfs = path.getFileSystem(config);
        ContentSummary cSummary = hdfs.getContentSummary(path);
        long length = cSummary.getLength();
        return length;
    }
}
