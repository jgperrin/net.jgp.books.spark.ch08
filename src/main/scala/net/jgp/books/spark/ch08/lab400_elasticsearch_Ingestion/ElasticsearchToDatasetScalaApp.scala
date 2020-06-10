package net.jgp.books.spark.ch08.lab400_elasticsearch_Ingestion

import org.apache.spark.sql.SparkSession

/**
 * Connects Spark and Elasticsearch, ingests data from Elasticsearch in Spark.
 *
 * @author rambabu.posa
 */
object ElasticsearchToDatasetScalaApp {

  /**
   * Our main...
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    /**
     * The real processing starts.
     */
    val t0: Long = System.currentTimeMillis

    val spark: SparkSession = SparkSession.builder
      .appName("Elasticsearch to Dataframe")
      .master("local[*]")
      .getOrCreate

    val t1 = System.currentTimeMillis
    println(s"Getting a session took: ${t1 - t0} ms")

    val df = spark.read
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes", "localhost")
      .option("es.port", "9200")
      .option("es.query", "?q=*")
      .option("es.read.field.as.array.include", "Inspection_Date")
      .load("nyc_restaurants")

    val t2 = System.currentTimeMillis
    println(s"Init communication and starting to get some results took: ${t2 - t1} ms")

    // Shows only a few records as they are pretty long
    df.show(10)

    val t3 = System.currentTimeMillis
    println(s"Showing a few records took: ${t3 - t2} ms")

    df.printSchema()
    val t4 = System.currentTimeMillis
    println(s"Displaying the schema took: ${t4 - t3} ms")

    println(s"The dataframe contains ${df.count} record(s).")
    val t5 = System.currentTimeMillis
    println(s"Counting the number of records took: ${t5 - t4} ms")

    println(s"The dataframe is split over ${df.rdd.getNumPartitions} partition(s).")
    val t6 = System.currentTimeMillis
    println(s"Counting the # of partitions took: ${t6 - t5} ms")

    spark.stop
  }

}
