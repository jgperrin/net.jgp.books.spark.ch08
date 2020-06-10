"""
 MySQL injection to Spark, using the Sakila sample database.

 @author rambabu.posa
"""
import logging
from pyspark.sql import SparkSession

# Creates a session on a local master
spark = SparkSession.builder.appName("MySQL with join to Dataframe using JDBC") \
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


# Builds the SQL query doing the join operation
sqlQuery = """
    select actor.first_name, actor.last_name, film.title, 
    film.description 
    from actor, film_actor, film 
    where actor.actor_id = film_actor.actor_id 
    and film_actor.film_id = film.film_id
"""

df = df.select(sqlQuery)

# Displays the dataframe and some of its metadata
df.show(5)
df.printSchema()

logging.info("The dataframe contains {} record(s).".format(df.count()))

spark.stop()