package net.jgp.books.spark.ch08.lab400_elasticsearch_Ingestion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Connects Spark and Elasticsearch, ingests data from Elasticsearch in
 * Spark.
 * 
 * @author jgp
 */
public class ElasticsearchToDatasetApp {

  /**
   * Our main...
   * 
   * @param args
   */
  public static void main(String[] args) {
    ElasticsearchToDatasetApp app =
        new ElasticsearchToDatasetApp();
    app.start();
  }

  /**
   * The real processing starts.
   */
  private void start() {
    long t0 = System.currentTimeMillis();

    SparkSession spark = SparkSession.builder()
        .appName("Elasticsearch to Dataframe")
        .master("local")
        .getOrCreate();

    long t1 = System.currentTimeMillis();
    System.out.println("Getting a session took: " + (t1 - t0) + " ms");

    Dataset<Row> df = spark
        .read()
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes", "localhost")
        .option("es.port", "9200")
        .option("es.query", "?q=*")
        .option("es.read.field.as.array.include", "Inspection_Date")
        .load("nyc_restaurants");

    long t2 = System.currentTimeMillis();
    System.out.println(
        "Init communication and starting to get some results took: "
            + (t2 - t1) + " ms");

    // Shows only a few records as they are pretty long
    df.show(10);

    long t3 = System.currentTimeMillis();
    System.out.println("Showing a few records took: " + (t3 - t2) + " ms");

    df.printSchema();
    long t4 = System.currentTimeMillis();
    System.out.println("Displaying the schema took: " + (t4 - t3) + " ms");

    System.out.println("The dataframe contains " +
        df.count() + " record(s).");
    long t5 = System.currentTimeMillis();
    System.out.println("Counting the number of records took: " + (t5 - t4)
        + " ms");

    System.out.println("The dataframe is split over " + df.rdd()
        .getPartitions().length + " partition(s).");
    long t6 = System.currentTimeMillis();
    System.out.println("Counting the # of partitions took: " + (t6 - t5)
        + " ms");
  }
}
