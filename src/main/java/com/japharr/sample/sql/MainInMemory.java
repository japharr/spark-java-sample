package com.japharr.sample.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class MainInMemory {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
        .builder().appName("testingSql").master("local[2]")
        .getOrCreate();

    Dataset<Row> dataSet = spark.read().option("header", true).format("csv").csv("/Users/datamatics/Data/biglog.txt");

    dataSet.createOrReplaceTempView("logging_table");

    // using group by
    // Dataset<Row> result = spark.sql("select level, count(datetime) from logging_table group by level order by level desc");
    // Dataset<Row> result = spark.sql(
    //  "select level, date_format(datetime, 'MMMM') as month, cast(first(date_format(datetime, 'M')) as int) as monthnum, count(1) as total " +
    //    "from logging_table group by level, month order by monthnum");
    // result = result.drop("monthnum");

    Dataset<Row> result = spark.sql(
      "select level, date_format(datetime, 'MMMM') as month, count(1) as total " +
        "from logging_table group by level, month order by cast(first(date_format(datetime, 'M')) as int)");

    // result.createOrReplaceTempView("aggregate_table");

    // Dataset<Row> summation = spark.sql("select sum(total) from aggregate_table");

    result.show();

    spark.close();
  }
}
