package net.jgp.books.spark.ch08.lab100_mysql_ingestion

import java.util.Properties
import org.apache.spark.sql.SparkSession

/**
 * MySQL injection to Spark, using the Sakila sample database, without using
 * properties
 *
 * @author rambabu.posa
 */
object MySQLToDatasetWithLongUrlScalaApp {

  /**
   * main() is your entry point to the application.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    /**
     * The processing code.
     */
    val spark: SparkSession = SparkSession.builder
      .appName("MySQL to Dataframe using a JDBC Connection")
      .master("local[*]")
      .getOrCreate

    // Using a JDBC URL
    val jdbcUrl = "jdbc:mysql://localhost:3306/sakila" +
      "?user=root" +
      "&password=Spark<3Java" +
      "&useSSL=false" +
      "&serverTimezone=EST"

    // And read in one shot
    var df = spark.read.jdbc(jdbcUrl, "actor", new Properties)
    df = df.orderBy(df.col("last_name"))

    // Displays the dataframe and some of its metadata
    df.show()
    df.printSchema()
    println(s"The dataframe contains ${df.count} record(s).")

    spark.stop
  }

}
