package net.jgp.books.spark.ch08.lab320_ingestion_partitioning

import java.util.Properties
import org.apache.spark.sql.SparkSession

/**
 * MySQL injection to Spark, using the Sakila sample database.
 *
 * @author rambabu.posa
 */
object MySQLToDatasetWithoutPartitionScalaApp {

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
      .appName("MySQL to Dataframe using JDBC without partioning")
      .master("local[*]")
      .getOrCreate

    // Using properties
    val props = new Properties
    props.put("user", "root")
    props.put("password", "Spark<3Java")
    props.put("useSSL", "false")
    props.put("serverTimezone", "EST")

    val mySQLURL = "jdbc:mysql://localhost:3306/sakila"
    // Let's look for all movies matching the query
    val df = spark.read.jdbc(mySQLURL, "film", props)

    // Displays the dataframe and some of its metadata
    df.show(5)
    df.printSchema()
    println(s"The dataframe contains ${df.count} record(s).")
    println(s"The dataframe is split over ${df.rdd.getNumPartitions} partition(s).")

    spark.stop
  }

}
