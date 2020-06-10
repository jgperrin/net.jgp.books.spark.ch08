name := "SparkInAction2-Chapter08"

version := "1.0.0"

scalaVersion := "2.12.10"

val sparkVersion                = "3.0.0-preview2"
val mysqlVersion                = "8.0.19"
val informixJdbcVersion         = "4.10.8.1"
val elasticSearchHadoopVersion  = "6.2.3"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark"    %% "spark-core"           % sparkVersion,
  "org.apache.spark"    %% "spark-sql"            % sparkVersion,
  "mysql"               % "mysql-connector-java"  % mysqlVersion,
  "com.ibm.informix"    % "jdbc"                  % informixJdbcVersion,
  "org.elasticsearch"   % "elasticsearch-hadoop"  % elasticSearchHadoopVersion
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
