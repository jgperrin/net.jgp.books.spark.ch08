package net.jgp.books.spark.ch08.lab300_advanced_queries;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * MySQL injection to Spark, using the Sakila sample database.
 * 
 * @author jgp
 */
public class MySQLWithWhereClauseToDatasetApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    MySQLWithWhereClauseToDatasetApp app =
        new MySQLWithWhereClauseToDatasetApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName(
            "MySQL with where clause to Dataframe using a JDBC Connection")
        .master("local")
        .getOrCreate();

    // Using properties
    Properties props = new Properties();
    props.put("user", "root");
    props.put("password", "Spark<3Java");
    props.put("useSSL", "false");
    props.put("serverTimezone", "EST");

    String sqlQuery = "select * from film where "
        + "(title like \"%ALIEN%\" or title like \"%victory%\" "
        + "or title like \"%agent%\" or description like \"%action%\") "
        + "and rental_rate>1 "
        + "and (rating=\"G\" or rating=\"PG\")";

    // Let's look for all movies matching the query
    Dataset<Row> df = spark.read().jdbc(
        "jdbc:mysql://localhost:3306/sakila",
        "(" + sqlQuery + ") film_alias",
        props);

    // Displays the dataframe and some of its metadata
    df.show(5);
    df.printSchema();
    System.out.println("The dataframe contains " + df
        .count() + " record(s).");
  }
}
