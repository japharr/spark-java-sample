package com.japharr.sample.mongodb;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class ReadFromMongoDb {
  public static void main(String[] args) {
    String connectionString = System.getProperty("mongodb.uri");
    System.out.println("uri: " + connectionString);
    SparkSession spark = SparkSession.builder()
        .master("local")
        .appName("MongoSparkConnectorIntro")
        .config("spark.mongodb.input.uri", connectionString)
        .config("spark.mongodb.output.uri", connectionString)
        .master("local[2]")
        .getOrCreate();

    // Create a JavaSparkContext using the SparkSession's SparkContext object
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    // Create a custom ReadConfig
    Map<String, String> readOverrides = new HashMap<String, String>();
    readOverrides.put("collection", "posting_record");
    readOverrides.put("readPreference.name", "secondaryPreferred");
    ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

    Dataset<Row> result = MongoSpark.loadAndInferSchema(spark, readConfig);

    Dataset<Row> output = result.filter(result.col("tag").isin("NOSTRO CONTROL", "NOSTRO ITEM"));

    output.show();

    spark.close();
  }
}
