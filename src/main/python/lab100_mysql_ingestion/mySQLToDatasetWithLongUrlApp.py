"""
 MySQL injection to Spark, using the Sakila sample database.

 @author rambabu.posa
"""
from pyspark.sql import SparkSession

# Creates a session on a local master
spark = SparkSession.builder.appName("MySQL to Dataframe using a JDBC Connection") \
    .master("local[*]").getOrCreate()

mysql_long_url = """
    jdbc:mysql://localhost:3306/sakila?user=root
    &password=Spark<3Java
    &useSSL=false
    &serverTimezone=EST
"""

user = "root"
password = "Spark<3Java"

dbtable = "actor"

df = spark.read.format("jdbc") \
    .option("url", mysql_long_url) \
    .option("dbtable", dbtable) \
    .load()

df = df.orderBy(df.col("last_name"))

# Displays the dataframe and some of its metadata
df.show(5)
df.printSchema()
print("The dataframe contains {} record(s).".format(df.count()))

spark.stop()