import java.nio.file.{Files, Paths}
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset, Row, SparkSession}

import scala.io.Source
import org.apache.spark.sql.functions._

object RandomDSSplitter {

  //########################## Get the Parameters of the Job ##########################################
  // Assuming that application.properties is in the root folder of the spark job

  val properties: Properties = new Properties()

  if (Files.exists(Paths.get("./dssplitter.properties"))) {
    val source = Source.fromURL("file:./dssplitter.properties")
    properties.load(source.bufferedReader())
  }
  else if (Files.exists(Paths.get("dssplitter.properties"))) {
    val source = Source.fromURL("file:dssplitter.properties")
    properties.load(source.bufferedReader())
  }
  else {
    println("no properties file, exiting")
    println(System.getProperty("user.dir"))
    System.exit(0)
  }

  val master = properties.getProperty("master")
  println(master)
  val fileName = properties.getProperty("input.file.name")
  println(fileName)
  val outFileName = properties.getProperty("output.file.name")
  println(outFileName)
  val hiveSupport = properties.getProperty("hive.support").toBoolean
  println(hiveSupport)
  val parThreads = properties.getProperty("spark.parallel.threads").toInt
  println(parThreads)
  val fileFormat = properties.getProperty("format")
  println(fileFormat)
  val partitioned = properties.getProperty("partitioned").toBoolean
  println(partitioned)
  val partitionName = properties.getProperty("partition.field.name")
  println(partitionName)
  val stageNumber = properties.getProperty("stage.number").toInt
  println(stageNumber)



}
