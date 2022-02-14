package com.japharr.sample.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MainSql {
  public static void main(String[] args) {
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    SparkSession spark = SparkSession
        .builder()
        .appName("testingSql")
        .master("local[2]")
        .getOrCreate();

    Dataset<Row> dataset = spark.read()
        .option("header", true)
        .csv("/students.csv");

    dataset.createOrReplaceTempView("my_students_table");

    Dataset<Row> result = spark.sql("select distinct(year) from my_students_table order by year");

    result.show();

    spark.close();
  }
}
