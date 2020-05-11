package net.jgp.books.spark.ch08.lab200_informix_dialect

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.jdbc.JdbcDialects

/**
 * MySQL injection to Spark, using the Sakila sample database.
 *
 * @author rambabu.posa
 */
object InformixToDatasetScalaApp {

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
      .appName("Informix to Dataframe using a JDBC Connection")
      .master("local[*]")
      .getOrCreate

    // Specific Informix dialect
    val dialect = new InformixJdbcDialectScala
    JdbcDialects.registerDialect(dialect)

    val informixURL = "jdbc:informix-sqli://[::1]:33378/stores_demo:IFXHOST=lo_informix1210;DELIMIDENT=Y"
    // Using properties
    val df = spark.read
      .format("jdbc")
      .option("url", informixURL)
      .option("dbtable", "customer")
      .option("user", "informix")
      .option("password", "in4mix")
      .load

    // Displays the dataframe and some of its metadata
    df.show(5)
    df.printSchema()
    println("The dataframe contains " + df.count + " record(s).")

  }

}
