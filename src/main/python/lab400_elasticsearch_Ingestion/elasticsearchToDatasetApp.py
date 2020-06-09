"""
 Connects PySpark and Elasticsearch, ingests data from Elasticsearch in PySpark.

 @author rambabu.posa
"""
import time
import logging
from pyspark.sql import SparkSession

t0 = int(round(time.time() * 1000))

# Creates a session on a local master
spark = SparkSession.builder.appName("Elasticsearch to Dataframe") \
    .master("local[*]").getOrCreate()

t1 = int(round(time.time() * 1000))

logging.info("Getting a session took: {} ms".format((t1 - t0)))

df = spark.read.format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.query", "?q=*") \
    .option("es.read.field.as.array.include", "Inspection_Date") \
    .load("nyc_restaurants")

t2 = int(round(time.time() * 1000))

logging.info("Init communication and starting to get some results took: {} ms".format((t2 - t1)))

# Shows only a few records as they are pretty long
df.show(10)

t3 = int(round(time.time() * 1000))

logging.info("Showing a few records took: {} ms".format((t3 - t2)))

df.printSchema()
t4 = int(round(time.time() * 1000))

logging.info("Displaying the schema took: {} ms".format((t4 - t3)))

logging.info("The dataframe contains ${df.count} record(s).")

t5 = int(round(time.time() * 1000))
logging.info("Counting the number of records took: {} ms".format((t5 - t4)))

logging.info("The dataframe is split over {} partition(s).".format(df.rdd.getNumPartitions()))

t6 = int(round(time.time() * 1000))

logging.info("Counting the # of partitions took: {} ms".format((t6 - t5)))

spark.stop()