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

    List<Row> inMemory = new ArrayList<>();

    inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
    inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
    inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
    inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
    inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));

    StructField[] fields = new StructField[] {
        new StructField("level", DataTypes.StringType, false, Metadata.empty()),
        new StructField("datetime", DataTypes.StringType, false, Metadata.empty()),
    };

    StructType structType = new StructType(fields);
    Dataset<Row> dataSet = spark.createDataFrame(inMemory, structType);

    dataSet.createOrReplaceTempView("logging_table");

    // using group by
    // Dataset<Row> result = spark.sql("select level, count(datetime) from logging_table group by level order by level desc");
    Dataset<Row> result = spark.sql("select level, date_format(datetime, 'MMMM') as month, count(1) from logging_table group by level, month");

    result.show();

    spark.close();
  }
}
