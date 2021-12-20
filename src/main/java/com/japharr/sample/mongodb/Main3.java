package com.japharr.sample.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

// Reading a file from HDFS line by line
public class Main3 {
    public static void main(String[] args) {
        String connectionString = System.getProperty("mongodb.uri");
        System.out.println("uri: " + connectionString);
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            List<String> inputData = new ArrayList<>();
            inputData.add("John");
            inputData.add("Thomas");
            inputData.add("Justice");
            inputData.add("Bridget");

            SparkConf conf = new SparkConf()
                .setAppName("readingFileContent")
                .setMaster("local");

            JavaSparkContext sc = new JavaSparkContext(conf);

            // Read the content of a file
            // JavaRDD<String> fileContentRdd = sc.textFile("hdfs://localhost:9000/ducks/ducks.csv");
            JavaRDD<String> fileContentRdd = sc.parallelize(inputData);

            // Print the content of the file line by line
            fileContentRdd.collect().forEach(System.out::println);

            sc.close();
        }
    }
}
