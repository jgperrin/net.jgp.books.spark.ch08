package net.jgp.books.sparkWithJava.ch08.lab_900.es_ingestion;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

public class ElasticSearchToDatasetApp {

  public static void main(String[] args) {
    ElasticSearchToDatasetApp app =
        new ElasticSearchToDatasetApp();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("ElasticSearch to Dataframe")
        .master("local")
        .getOrCreate();

    SQLContext sql = spark.sqlContext();
    // JavaEsSparkSQL.esDF(spark, arg1, arg2);

    // Dataset<Row> df = spark.read().jdbc(
    // "jdbc:mysql://localhost:3306/sakila?serverTimezone=EST",
    // "actor", props);
    // df = df.orderBy(df.col("last_name"));
    // df.show();
  }
}
