package net.jgp.books.spark.ch08.lab320_ingestion_partitioning;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Partitioning the film table in 10 for a MySQL injection to Spark, using
 * the Sakila sample database.
 * 
 * @author jgp
 */
public class MySQLToDatasetWithPartitionApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    MySQLToDatasetWithPartitionApp app =
        new MySQLToDatasetWithPartitionApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName(
            "MySQL to Dataframe using JDBC with partitioning")
        .master("local")
        .getOrCreate();

    // Using properties
    Properties props = new Properties();
    props.put("user", "root");
    props.put("password", "Spark<3Java");
    props.put("useSSL", "false");
    props.put("serverTimezone", "EST");

    // Used for partitioning
    props.put("partitionColumn", "film_id");
    props.put("lowerBound", "1");
    props.put("upperBound", "1000");
    props.put("numPartitions", "10");

    // Let's look for all movies matching the query
    Dataset<Row> df = spark.read().jdbc(
        "jdbc:mysql://localhost:3306/sakila",
        "film",
        props);

    // Displays the dataframe and some of its metadata
    df.show(5);
    df.printSchema();
    System.out.println("The dataframe contains " + df
        .count() + " record(s).");
    System.out.println("The dataframe is split over " + df.rdd()
        .getPartitions().length + " partition(s).");
  }
}
