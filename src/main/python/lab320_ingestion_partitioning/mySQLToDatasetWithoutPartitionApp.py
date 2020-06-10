"""
 MySQL injection to Spark, using the Sakila sample database.

 @author rambabu.posa
"""
import logging
from pyspark.sql import SparkSession

# Creates a session on a local master
spark = SparkSession.builder.appName("MySQL to Dataframe using JDBC without partitioning") \
    .master("local[*]").getOrCreate()

user = "root"
password = "Spark<3Java"
use_ssl="false"
mysql_url = "jdbc:mysql://localhost:3306/sakila?serverTimezone=EST"
dbtable = "actor"

df = spark.read.format("jdbc") \
    .option("url", mysql_url) \
    .option("user", user) \
    .option("dbtable", dbtable) \
    .option("password", password) \
    .load()

df = df.table("film")

# Displays the dataframe and some of its metadata
df.show(5)
df.printSchema()

logging.info("The dataframe contains {} record(s).".format(df.count()))
logging.info("The dataframe is split over ${} partition(s).".format(df.rdd.getNumPartitions()))

spark.stop()