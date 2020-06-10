package net.jgp.books.spark.ch08.lab311_sql_joins

import java.util.Properties
import org.apache.spark.sql.SparkSession

/**
 * MySQL injection to Spark, using the Sakila sample database.
 *
 * @author rambabu.posa
 */
object MySQLWithJoinToDatasetScalaApp {

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
      .appName("MySQL with join to Dataframe using JDBC")
      .master("local[*]")
      .getOrCreate

    // Using properties
    val props = new Properties
    props.put("user", "root")
    props.put("password", "Spark<3Java")
    props.put("useSSL", "false")
    props.put("serverTimezone", "EST")

    // Builds the SQL query doing the join operation
    val sqlQuery = "select * " +
      "from actor, film_actor, film " +
      "where actor.actor_id = film_actor.actor_id " +
      "and film_actor.film_id = film.film_id"

    val mySQLURL = "jdbc:mysql://localhost:3306/sakila"
    val df = spark.read
      .jdbc(mySQLURL, "(" + sqlQuery + ") actor_film_alias", props)

    // Displays the dataframe and some of its metadata
    df.show(5)
    df.printSchema()
    println("The dataframe contains " + df.count + " record(s).")

    spark.stop
  }

}
