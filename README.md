The examples in this repository are support to the **[Spark in Action, 2nd edition](http://jgp.net/sia)** book by Jean Georges Perrin and published by Manning. Find out more about the book on [Manning's website](http://jgp.net/sia).

# Spark in Action, 2nd edition - chapter 8

Welcome to Spark with Java, chapter 8. This chapter covers data ingestion from databases.

This code is designed to work with Apache Spark v3.0.0.

## Labs

### Lab \#100

The `MySQLToDatasetApp` application does the following:

1.	It acquires a session (a `SparkSession`).
2.	It connects to MySQL Database.
3.	Spark reads data from MySQL Database into dataframe, then perform some operations on dataframe.

### Lab \#101

Same as lab #100, but uses environment variables.

### Lab \#200

Example of building a dialect, here connect IBM Informix and Apache Spark.

### Lab \#300

Advanced queries.

### Lab \#310

Ingestion from SQL joins.

### Lab \#311

Similar to lab #310.

### Lab \#320

Repartitionning after ingestion.

### Lab \#400

Ingestion from Elasticsearch.

Note that, as of now, Elasticsearch does not support Apache Spark v3.x, as the connector is limited to Scala v2.11. To run this example, use `pom-elastic-scale2.11.xml` instead of the default `pom.xml`. 


## Running the lab in Java

For information on running the Java lab, see chapter 1 in [Spark in Action, 2nd edition](http://jgp.net/sia).

## Running the lab using PySpark

Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer Appendix P - 'Spark in production: installation and a few tips').

1. Clone this project

```
git clone https://github.com/jgperrin/net.jgp.books.spark.ch08
```

2. Go to the lab in the Python directory

```
cd net.jgp.books.spark.ch08/src/main/python/lab100_mysql_ingestion/
```

3. Execute the following spark-submit command to run any MySQL database related application

 ```
spark-submit --driver-class-path /temp/jars/mysql_mysql-connector-java-8.0.12.jar --packages mysql:mysql-connector-java:8.0.12 mySQLToDatasetApp.py 
 ```

NOTE:- 
Here we assume that user have MySQL jar file at '/tmp/jars' folder (Please use any folder you like based on your OS.).
You can also use maven or ivy repository path.

4. Execute the following spark-submit command to run any ElasticSearch related application

```
cd net.jgp.books.spark.ch08/src/main/python/lab400_elasticsearch_Ingestion/
```

 ```
spark-submit --driver-class-path /tmp/jars/org.elasticsearch_elasticsearch-hadoop-6.2.3.jar --packages org.elasticsearch:elasticsearch-hadoop:6.2.3 elasticsearchToDatasetApp.py
 ```
NOTE:- 
Here we assume that user have ElasticSearch jar file at '/tmp/jars' folder (Please use any folder you like based on your OS.).
You can also use maven or ivy repository path.

## Running the lab in Scala

Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer Appendix P - 'Spark in production: installation and a few tips').

1. Clone this project

```
git clone https://github.com/jgperrin/net.jgp.books.spark.ch08
```

2. cd net.jgp.books.spark.ch08

3. Package application using sbt command

```
sbt clean assembly
```

4. Run Spark/Scala application using spark-submit command as shown below:

```
spark-submit --class net.jgp.books.spark.ch08.lab100_mysql_ingestion.MySQLToDatasetScalaApp target/scala-2.12/SparkInAction2-Chapter08-assembly-1.0.0.jar
```

## Notes
 1. [Java] Due to renaming the packages to match more closely Java standards, this project is not in sync with the book's MEAP prior to v10 (published in April 2019).
 2. [Scala, Python] As of MEAP v14, we have introduced Scala and Python examples (published in October 2019).
 
---

Follow me on Twitter to get updates about the book and Apache Spark: [@jgperrin](https://twitter.com/jgperrin). Join the book's community on [Facebook](https://facebook.com/sparkinaction/) or in [Manning's live site](https://forums.manning.com/forums/spark-in-action-second-edition?a_aid=jgp).
