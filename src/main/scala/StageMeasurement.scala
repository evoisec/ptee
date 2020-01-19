import java.nio.file.{Files, Paths}
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset, Row, SparkSession}

import scala.io.Source
import org.apache.spark.sql.functions._

/**********************************************************************************************************************
 *
 * Spark Job for performance measurement of various Data Shuffling Scenarios, between Job Stages and when running at scale
 * Features controlled chaining of successive Spark Job Stages, each implementing different Data Shuffling Scenario, enabling the measurement
 * of their resulting data volume (ie during shuffling), data throughput and resource consumption - Network IO, RAM and Disk I/O
 *
 * @author  Evo Eftimov
 **********************************************************************************************************************/

object StageMeasurement {

  var master: String = null
  var fileName: String = null
  var fileNameJoin: String = null
  var outFileName: String = null
  var outFileNameJoin: String = null
  var hiveSupport: Boolean = true
  var parThreads: Integer = 2
  var fileFormat: String = null
  var partitioned: Boolean = false
  var partitionName: String = null
  var stageFlow: String = null

  def stageRunner(stage: String, field: String, df1: DataFrame, df2: DataFrame): Unit = {

    if (stage.equalsIgnoreCase("GROUP")){

      println("Executing Stage: " + stage)

      println(stage)
      println(field)

      df1.groupBy(field).sum("BENEFITS").count()

    }

    if (stage.equalsIgnoreCase("JOIN")){

      println("Executing Stage: " + stage)

      println(stage)
      println(field)

      //simulate Join based Shuffling and Stage Boundary
      df1.join(df2, field).show()

    }

    if (stage.equalsIgnoreCase("JOIN-W")){

      println("Executing Stage: " + stage)

      println(stage)
      println(field)

      //simulate Join based Shuffling and Stage Boundary
      df1.join(df2, field).show()

      if (partitioned) {

        df1.write
          .mode("overwrite")
          .format(fileFormat)
          .option("header", "true")
          .partitionBy(partitionName)
          .save(outFileNameJoin)

      }
      else {

        df1.write
          .mode("overwrite")
          .format(fileFormat)
          .option("header", "true")
          .save(outFileNameJoin)

      }

    }

    if (stage.equalsIgnoreCase("CUBE")){

      println("Executing Stage: " + stage)

      println(stage)
      println(field)

      var i = df1.cube(col(field), col("ADDRESS")).agg(Map(
        "BENEFITS" -> "avg",
        "BALANCE" -> "max"
      ))
      i.show()

    }

    if (stage.equalsIgnoreCase("WRITE")){

      println("Executing Stage: " + stage)

      println(stage)
      println(field)

      if (partitioned) {

        df1.write
          .mode("overwrite")
          .format(fileFormat)
          .option("header", "true")
          .partitionBy(partitionName)
          .save(outFileName)

      }
      else {

        df1.write
          .mode("overwrite")
          .format(fileFormat)
          .option("header", "true")
          .save(outFileName)

      }

    }

  }


  def main(args: Array[String]): Unit = {


    //########################## Get the Parameters of the Job ##########################################
    // Assuming that application.properties is in the root folder of the spark job

    val properties: Properties = new Properties()

    if (Files.exists(Paths.get("./stagemeasurements.properties"))) {
      val source = Source.fromURL("file:./stagemeasurements.properties")
      properties.load(source.bufferedReader())
    }
    else if (Files.exists(Paths.get("stagemeasurements.properties"))) {
      val source = Source.fromURL("file:stagemeasurements.properties")
      properties.load(source.bufferedReader())
    }
    else {
      println("no properties file, exiting")
      println(System.getProperty("user.dir"))
      System.exit(0)
    }

    master = properties.getProperty("master")
    println(master)
    fileName = properties.getProperty("input.file.name")
    println(fileName)
    fileNameJoin = properties.getProperty("input.file.name-join")
    println(fileNameJoin)
    outFileName = properties.getProperty("output.file.name")
    println(outFileName)
    outFileNameJoin = properties.getProperty("output.file.name-join")
    println(outFileNameJoin)
    hiveSupport = properties.getProperty("hive.support").toBoolean
    println(hiveSupport)
    parThreads = properties.getProperty("spark.parallel.threads").toInt
    println(parThreads)
    fileFormat = properties.getProperty("format")
    println(fileFormat)
    partitioned = properties.getProperty("partitioned").toBoolean
    println(partitioned)
    partitionName = properties.getProperty("partition.field.name")
    println(partitionName)
    stageFlow = properties.getProperty("stage.flow")
    println(stageFlow)


    //System.exit(0)

    val sparkT = SparkSession.builder
      .master(master)
      .appName("stage-measurement")
      //put here any required param controlling Vectorization (see all available params listed at the beginning)
      .config("spark.sql.parquet.enableVectorizedReader ", "true")


    if (hiveSupport) {

      sparkT.config("spark.sql.warehouse.dir", "/opt/dwh")
      sparkT.enableHiveSupport()

    }

    val spark = sparkT.getOrCreate()

    import spark.implicits._

    var mainpartDF : DataFrame = null
    var mainpartDF2 : DataFrame = null

    if (fileFormat.equalsIgnoreCase("csv")) {

      mainpartDF = spark.read.format("csv")
        .option("sep", ",")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(fileName)
        .repartition(parThreads)

      if(fileNameJoin != null)
        mainpartDF2 = spark.read.format("csv")
          .option("sep", ",")
          .option("inferSchema", "true")
          .option("header", "true")
          .load(fileNameJoin)
          .repartition(parThreads)

    }

    if (fileFormat.equalsIgnoreCase("parquet")) {

      mainpartDF = spark.read.format("parquet")
        //.option("sep", ",")
        //.option("inferSchema", "true")
        .option("header", "true")
        .load(fileName)
        .repartition(parThreads)

      if(fileNameJoin != null)
        mainpartDF2 = spark.read.format("parquet")
          //.option("sep", ",")
          //.option("inferSchema", "true")
          .option("header", "true")
          .load(fileNameJoin)
          .repartition(parThreads)

    }

    mainpartDF.printSchema()
    mainpartDF.show()

    //************************************************************************************************************************
    // Begin controlled chaining of successive Spark Job Stages, each implementing different Data Shuffling Scenario, enabling the measurement
    // of their resulting data volume (ie during shuffling), data throughput and resource consumption - Network IO, RAM and Disk I/O
    //************************************************************************************************************************

    //mainpartDF.show()

    /* val s = mainpartDF
       .select("NIN","BENEFITS")
       .as[(Int, Int)]
       .map { case (n, b) => (n+1, b+1) } */


    //Perform a simulated Task Pipeline Operation (no Stages / Shuffling)
    mainpartDF = mainpartDF.withColumn("NIN", col("NIN") +1)

    //mainpartDF.show()
    //mainpartDF.printSchema()


    println(mainpartDF.count())


    //************************************************************************************

    stageFlow = stageFlow.replaceAll("\\s", "")
    println(stageFlow)
    val stages = stageFlow.split(",")

    for (i <- stages){

      //println(i)
      val s = i.split(":")

      if (s.length > 1)
        stageRunner(s(0), s(1), mainpartDF, mainpartDF2)
      else
        stageRunner(s(0), "", mainpartDF, mainpartDF2)

    }



    //**************************************************************************************



  }


}
