package com.japharr.sample.sql;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Function1;
import static org.apache.spark.sql.functions.*;

public class Main {
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

    //dataset.show();

    //long count = dataset.count();
    //System.out.println("csv count: " + count);

    //Row first = dataset.first();

    //Object column3Value = first.get(3); // column index
    //String subject = first.getAs("subject"); // column name

    // Using filter
    // using sql
    // Dataset<Row> modernArt = dataset.filter("subject = 'Modern Art AND year > 2000");

    // using lambda
    // Dataset<Row> modernArt = dataset.filter((FilterFunction<Row>) item -> item.getAs("subject").equals("Modern Art") && Integer.parseInt(item.getAs("year")) > 2007);

    // using dataset column
    // Column subjectCol = dataset.col("subject");
    // Column yearCol = dataset.col("year");
    // Dataset<Row> modernArt = dataset.filter(subjectCol.equalTo("Modern Art").and(yearCol.geq(2007)));

    // Using function col (Domain Specific Language)
    Dataset<Row> modernArt = dataset.filter(col("subject").equalTo("Modern Art")
        .and(col("year").geq(2007)));

    modernArt.show();

    spark.close();
  }
}
