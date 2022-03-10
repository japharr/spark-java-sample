package com.japharr.sample.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MainPracticalSql {
  private static final String SQL_QUERY_CALC_VAT =
      "SELECT %1$s, %2$s, double(%2$s) * %3$f AS vat FROM atm_transfer";
  private static final String SQL_QUERY_CALC_SUM =
      "select %1$s, sum(%2$s) AS total, sum(vat) AS total_vat from atm_transfer_vat GROUP BY %1$s";

  public static void main(String[] args) {
    Logger.getLogger("org.apache").setLevel(Level.WARN);
    if(args.length == 0) {
      System.out.println("at least, one argument is required");
      return;
    }

    System.out.println("path: " + args[0]);

    SparkSession spark = SparkSession
        .builder()
        .appName("testingSql")
        .master("local[2]")
        .getOrCreate();

    Dataset<Row> dataset = spark.read()
        .option("header", true)
        .csv(args[0]);

    dataset.createOrReplaceTempView("atm_transfer");

    String amountColumn = "Settlement_Impact";
    String groupByColumns = "Trxn_Category, Settlement_Impact_Desc";
    Set<String> groupByColumnsSet = new HashSet<>();
    groupByColumnsSet.add("Trxn_Category");groupByColumnsSet.add("Settlement_Impact_Desc");

    double vat = 0.075;

    String sql1 = String.format(SQL_QUERY_CALC_VAT, groupByColumns, amountColumn, vat);
    System.out.println(sql1);

    //String sql1 = "select Trxn_Category, Settlement_Impact_Desc, Settlement_Impact, double(Settlement_Impact) * 0.075 as vat from atm_transfer";

//    String sql2 =
//        "select Trxn_Category, Settlement_Impact_Desc, sum(Settlement_Impact) AS total, sum(vat) AS total_vat " +
//            "from atm_transfer_vat GROUP BY Trxn_Category, Settlement_Impact_Desc";

    String sql2 = String.format(SQL_QUERY_CALC_SUM, groupByColumns, amountColumn);
    System.out.println(sql2);

    /* */
    Dataset<Row> vatDataset = spark.sql(sql1);

    vatDataset.createOrReplaceTempView("atm_transfer_vat");

    Dataset<Row> sumDataset = spark.sql(sql2);

    List<Report2> reportList = sumDataset.javaRDD().map(row -> {
      Set<String> values = new HashSet<>();
      groupByColumnsSet.forEach(by -> values.add(row.getAs(by)));
      return new Report2(values, BigDecimal.valueOf((Double) row.getAs("total")),
          BigDecimal.valueOf((Double) row.getAs("total_vat")));
    }).collect();

    reportList.forEach(System.out::println);
    /* */

    /*

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

    */

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

class Report2 implements Serializable {
  private Set<String> groupBy;
  private BigDecimal total;
  private BigDecimal vat;

  public Report2() {}

  public Report2(Set<String> groupBy, BigDecimal total, BigDecimal vat) {
    this.groupBy = groupBy;
    this.total = total; //BigDecimal.valueOf(total);
    this.vat = vat; //BigDecimal.valueOf(vat);
  }

  public Set<String> getGroupBy() {
    return groupBy;
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
        "cat='" + groupBy + '\'' +
        ", total=" + total +
        ", vat=" + vat +
        '}';
  }
}
