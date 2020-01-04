import java.nio.file.{Files, Paths}
import java.util.Properties

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.io.Source
import org.apache.spark.sql.functions._

object StageMeasurement {


  def main(args: Array[String]): Unit = {


    //########################## Get the Parameters of the Job ##########################################
    // Assuming that application.properties is in the root folder of the spark job

    val properties: Properties = new Properties()

    if (Files.exists(Paths.get("./dfsread.properties"))) {
      val source = Source.fromURL("file:./dfsread.properties")
      properties.load(source.bufferedReader())
    }
    else if (Files.exists(Paths.get("dfsread.properties"))) {
      val source = Source.fromURL("file:dfsread.properties")
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
    val hiveSupport = properties.getProperty("hive.support").toBoolean
    println(hiveSupport)
    val parThreads = properties.getProperty("spark.parallel.threads").toInt
    println(parThreads)
    val fileFormat = properties.getProperty("format")
    println(fileFormat)


    //System.exit(0)

    val sparkT = SparkSession.builder
      .master(master)
      .appName("dfs-read")
      //put here any required param controlling Vectorization (see all available params listed at the beginning)
      .config("spark.sql.parquet.enableVectorizedReader ", "true")


    if (hiveSupport) {

      sparkT.config("spark.sql.warehouse.dir", "/opt/dwh")
      sparkT.enableHiveSupport()

    }

    val spark = sparkT.getOrCreate()

    import spark.implicits._

    var mainpartDF : DataFrame = null

    if (fileFormat.equalsIgnoreCase("csv")) {

      mainpartDF = spark.read.format("csv")
        .option("sep", ",")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(fileName)
        .repartition(parThreads)

    }

    if (fileFormat.equalsIgnoreCase("parquet")) {

      mainpartDF = spark.read.format("parquet")
        //.option("sep", ",")
        //.option("inferSchema", "true")
        .option("header", "true")
        .load(fileName)
        .repartition(parThreads)

    }

    mainpartDF.printSchema()

    //************************************************************************************************************************
    //perform some lightweight data processing operations to simulate reading of the dataset but without heavy data processing
    //************************************************************************************************************************

    //mainpartDF.show()

    /* val s = mainpartDF
       .select("NIN","BENEFITS")
       .as[(Int, Int)]
       .map { case (n, b) => (n+1, b+1) } */


    mainpartDF = mainpartDF.withColumn("NIN", col("NIN") +1)

    //mainpartDF.show()
    //mainpartDF.printSchema()

    mainpartDF.createOrReplaceTempView("perftest")

    val result = spark.sql("SELECT * FROM perftest")

    //result.show()
    //result.printSchema()

    println(result.count())

  }


}
