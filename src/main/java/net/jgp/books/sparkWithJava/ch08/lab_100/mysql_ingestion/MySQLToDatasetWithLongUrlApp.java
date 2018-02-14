package net.jgp.books.sparkWithJava.ch08.lab_100.mysql_ingestion;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * MySQL injection to Spark, using the Sakila sample database, without using
 * properties
 * 
 * @author jgp
 */
public class MySQLToDatasetWithLongUrlApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    MySQLToDatasetWithLongUrlApp app = new MySQLToDatasetWithLongUrlApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("MySQL to Dataframe using a JDBC Connection")
        .master("local")
        .getOrCreate();

    // Using a JDBC URL
    String jdbcUrl = "jdbc:mysql://localhost:3306/sakila"
        + "?user=root"
        + "&password=Spark<3Java"
        + "&useSSL=false"
        + "&serverTimezone=EST";

    // And read in one shot
    Dataset<Row> df = spark.read()
        .jdbc(jdbcUrl, "actor", new Properties());
    
    // Displays the dataframe and some of its metadata
    df.show();
    df.printSchema();
    System.out.println("The dataframe contains "
        + df.count()
        + " record(s).");
  }
}
