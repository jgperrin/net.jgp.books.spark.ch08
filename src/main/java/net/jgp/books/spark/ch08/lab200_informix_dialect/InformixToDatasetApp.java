package net.jgp.books.spark.ch08.lab200_informix_dialect;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;

/**
 * MySQL injection to Spark, using the Sakila sample database.
 * 
 * @author jgp
 */
public class InformixToDatasetApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    InformixToDatasetApp app = new InformixToDatasetApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName(
            "Informix to Dataframe using a JDBC Connection")
        .master("local")
        .getOrCreate();

    // Specific Informix dialect
    JdbcDialect dialect = new InformixJdbcDialect();
    JdbcDialects.registerDialect(dialect);

    // Using properties
    Dataset<Row> df = spark
        .read()
        .format("jdbc")
        .option(
            "url",
            "jdbc:informix-sqli://[::1]:33378/stores_demo:IFXHOST=lo_informix1210;DELIMIDENT=Y")
        .option("dbtable", "customer")
        .option("user", "informix")
        .option("password", "in4mix")
        .load();

    // Displays the dataframe and some of its metadata
    df.show(5);
    df.printSchema();
    System.out.println("The dataframe contains " + df
        .count() + " record(s).");
  }
}
