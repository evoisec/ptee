name := "pt"

version := "0.1"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"

// https://mvnrepository.com/artifact/org.postgresql/postgresql
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.5"


//libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0"

//libraryDependencies += "org.apache.hadoop" % "hadoop-hive" % "1.2.1"