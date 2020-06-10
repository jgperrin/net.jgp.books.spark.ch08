package net.jgp.books.spark.ch08.lab100_mysql_ingestion

import java.util.Properties
import org.apache.spark.sql.SparkSession

/**
 * MySQL injection to Spark, using the Sakila sample database.
 *
 * @author rambabu.posa
 */
object MySQLToDatasetScalaApp {

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

    // Using properties
    val props = new Properties
    props.put("user", "root")
    props.put("password", "Spark<3Java")
    props.put("useSSL", "false")

    val mysqlURL = "jdbc:mysql://localhost:3306/sakila?serverTimezone=EST"

    var df = spark.read.jdbc(mysqlURL, "actor", props)

    df = df.orderBy(df.col("last_name"))

    // Displays the dataframe and some of its metadata
    df.show(5)
    df.printSchema()
    println(s"The dataframe contains ${df.count} record(s).")

    spark.stop
  }

}
