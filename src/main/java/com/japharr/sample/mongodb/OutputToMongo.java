package com.japharr.sample.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

// Reading a file from HDFS line by line
public class OutputToMongo {
    public static void main(String[] args) {
        String connectionString = System.getProperty("mongodb.uri");
        System.out.println("uri: " + connectionString);
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            // connecting to a specific database
            MongoDatabase sparkDb = mongoClient.getDatabase("sparkDb");
            MongoCollection<Document> outputSource = sparkDb.getCollection("inputData");

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
            fileContentRdd.collect().forEach(r -> insertData(r, outputSource));

            sc.close();
        }
    }

    public static void insertData(String data, MongoCollection<Document> outputSource) {
        System.out.println("inserting into db: " + data);
        Document student = new Document("_id", new ObjectId());
        student.append("name", data);
        outputSource.insertOne(student);
    }
}
