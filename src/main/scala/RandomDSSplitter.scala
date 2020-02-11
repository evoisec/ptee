import java.nio.file.{Files, Paths}
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset, Row, SparkSession}

import scala.io.Source
import org.apache.spark.sql.functions._

object RandomDSSplitter {

  def main(args: Array[String]): Unit = {

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

    val splitRatio = properties.getProperty("split.ratio").toDouble
    println(splitRatio)

    val datePresent = properties.getProperty("date.present").toBoolean
    println(datePresent)

    val dwhLocation = properties.getProperty("dwh.location")
    println(dwhLocation)

    val storage = properties.getProperty("storage")
    println(storage)
    val indbName = properties.getProperty("input.db.name")
    println(indbName)
    val intableName = properties.getProperty("input.table.name")
    println(intableName)
    val outdbName = properties.getProperty("output.db.name")
    println(outdbName)
    val outtableName = properties.getProperty("output.table.name")
    println(outtableName)
    val format = properties.getProperty("format")
    println(format)


    val sparkT = SparkSession.builder
      .master(master)
      .appName("random-ds-splitter")
      //put here any required param controlling Vectorization (see all available params listed at the beginning)
      .config("spark.sql.parquet.enableVectorizedReader ", "true")


    if (hiveSupport) {

      sparkT.config("spark.sql.warehouse.dir", dwhLocation)
      sparkT.enableHiveSupport()

    }

    val spark = sparkT.getOrCreate()

    var mainpartDF: DataFrame = null

    if (fileFormat.equalsIgnoreCase("csv") && storage.equalsIgnoreCase("file")) {

      mainpartDF = spark.read.format("csv")
        .option("sep", ",")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(fileName)
        .repartition(parThreads)

    }

    if (fileFormat.equalsIgnoreCase("parquet") && storage.equalsIgnoreCase("file")) {

      mainpartDF = spark.read.format("parquet")
        //.option("sep", ",")
        //.option("inferSchema", "true")
        .option("header", "true")
        .load(fileName)
        .repartition(parThreads)

    }

    if (storage.equalsIgnoreCase("db")){

      mainpartDF = spark.sql("SELECT * FROM " + indbName + "." + intableName)

    }

    mainpartDF.printSchema()
    mainpartDF.show()


    var splits: Array[DataFrame] = mainpartDF.randomSplit(Array(1-splitRatio, splitRatio))
    var trainingData = splits(0)
    println("Number of training feature vectors = " + trainingData.count())
    var testData = splits(1)
    println("Number of test feature vectors = " + testData.count())
    testData.show(100)

    //if (datePresent)
      //testData = testData.withColumn("DATE", col("DATE").cast("date"))

    if(storage.equalsIgnoreCase("file")) {

      if (partitioned) {

        testData.write
          .mode("overwrite")
          .format(format)
          .option("header", "true")
          .partitionBy(partitionName)
          .save(fileName)

      }
      else {

        testData.write
          .mode("overwrite")
          .format(format)
          .option("header", "true")
          .save(fileName)

      }


    }

    if(storage.equalsIgnoreCase("db")) {

      if (partitioned) {

        testData.write
          .mode("overwrite")
          .format(format)
          .partitionBy(partitionName)
          .saveAsTable(outdbName + "." + outtableName)

      }
      else{

        testData.write
          .mode("overwrite")
          .format(format)
          .saveAsTable(outdbName + "." + outtableName)

      }

    }


  }

}
