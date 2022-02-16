package com.japharr.sample.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;

public class MainPracticalSql {
  public static void main(String[] args) {
    Logger.getLogger("org.apache").setLevel(Level.WARN);
    if(args.length == 0) {
      System.out.println("at least, one argument is required");
      return;
    }

    SparkSession spark = SparkSession
        .builder()
        .appName("testingSql")
        .master("local[2]")
        .getOrCreate();

    Dataset<Row> dataset = spark.read()
        .option("header", true)
        .csv(args[0]);

    dataset.createOrReplaceTempView("atm_transfer");

    // Dataset<Row> result = spark.sql(sql_01);
    // Dataset<Row> result = spark.sql(sql_02);
    Dataset<Row> result = spark.sql("sql_03");

    result.createOrReplaceTempView("atm_transfer_vat");

    Dataset<Row> result01 = spark.sql("sql_04");

//    List<Report> reportList = result01.javaRDD().map(row -> {
//      return new Report(row.getString(0), row.getString(1), row.getDouble(2), row.getDouble(3));
//    }).collect();


    result01.show(false);
    //reportList.forEach(System.out::println);



    spark.close();
  }
}

class Report implements Serializable {
  private String cat;
  private String desc;
  private BigDecimal total;
  private BigDecimal vat;

  public Report() {}

  public Report(String cat, String desc, Double total, Double vat) {
    this.cat = cat;
    this.desc = desc;
    this.total = BigDecimal.valueOf(total);
    this.vat = BigDecimal.valueOf(vat);
  }

  public String getCat() {
    return cat;
  }

  public String getDesc() {
    return desc;
  }

  public BigDecimal getTotal() {
    return total;
  }

  public BigDecimal getVat() {
    return vat;
  }

  @Override
  public String toString() {
    return "Report{" +
      "cat='" + cat + '\'' +
      ", desc='" + desc + '\'' +
      ", total=" + total +
      ", vat=" + vat +
      '}';
  }
}
