"""
 MySQL injection to Spark, using the Sakila sample database.

 @author rambabu.posa
"""
import logging
from pyspark.sql import SparkSession

# Creates a session on a local master
spark = SparkSession.builder.appName("MySQL with where clause to Dataframe using a JDBC Connection") \
    .master("local[*]").getOrCreate()

user = "root"
password = "Spark<3Java"
use_ssl = "false"
mysql_url = "jdbc:mysql://localhost:3306/sakila?serverTimezone=EST"
dbtable = "actor"


sqlQuery = """
    select * from film where 
    (title like \"%ALIEN%\" or title like \"%victory%\" 
    or title like \"%agent%\" or description like \"%action%\") 
    and rental_rate>1 
    and (rating=\"G\" or rating=\"PG\")
"""

df = spark.read.format("jdbc") \
    .option("url", mysql_url) \
    .option("user", user) \
    .option("dbtable", dbtable) \
    .option("password", password) \
    .load()

df = df.select(sqlQuery)

# Displays the dataframe and some of its metadata
df.show(5)
df.printSchema()

logging.info("The dataframe contains {} record(s).".format(df.count()))

spark.stop()