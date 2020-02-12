import java.io.FileInputStream
import java.nio.file.{Files, Paths}
import java.util.Properties
import java.util.UUID.randomUUID

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
  var hiveSupport: Boolean = true
  var parThreads: Integer = 2
  var fileFormat: String = null
  var partitioned: Boolean = false
  var partitionName: String = null
  var stageFlow: String = null
  var datePresent: Boolean = true

  def stageRunner(stage: String, field: String, df1: DataFrame, df2: DataFrame): DataFrame = {

    var dataFrameRes: DataFrame = null

    if (stage.equalsIgnoreCase("GROUP")){

      println("Executing Stage: " + stage)

      println(stage)
      println(field)

      dataFrameRes = df1.groupBy(field).sum("BENEFITS")
      dataFrameRes.show()
      dataFrameRes.count()

    }

    if (stage.equalsIgnoreCase("JOIN")){

      println("Executing Stage: " + stage)

      println(stage)
      println(field)

      //simulate Join based Shuffling and Stage Boundary

      dataFrameRes = df1.join(df2, field)

      val newColumns = Seq("newCol1","newCol2","newCol3","newCol4","newCol5","newCol6","newCol7","newCol8","newCol9","newCol10","newCol11","newCol12","newCol13","newCol14","newCol15", "newcol16", "newcol17")
      dataFrameRes = dataFrameRes.toDF(newColumns:_*)

      dataFrameRes.show()
      dataFrameRes.count()

    }


    if (stage.equalsIgnoreCase("CUBE")){

      println("Executing Stage: " + stage)

      println(stage)
      println(field)


      dataFrameRes = df1.cube(col(field), col("ADDRESS")).agg(Map(
        "BENEFITS" -> "avg",
        "BALANCE" -> "max"
      ))
      dataFrameRes.show()
      dataFrameRes.count()

    }

    if (stage.equalsIgnoreCase("WRITE")){

      println("Executing Stage: " + stage)

      println(stage)
      println(field)


      if (dataFrameRes == null) {

        dataFrameRes = df1

      }

      if (partitioned) {

        dataFrameRes.write
          .mode("overwrite")
          .format(fileFormat)
          .option("header", "true")
          .partitionBy(partitionName)
          .save(outFileName)

      }
      else {

        dataFrameRes.write
          .mode("overwrite")
          .format(fileFormat)
          .option("header", "true")
          .save(outFileName)

      }



    }

    return dataFrameRes

  }


  def main(args: Array[String]): Unit = {


    //########################## Get the Parameters of the Job ##########################################

    val cfgFile = args(1)
    val properties: Properties = new Properties()
    println(cfgFile)

    if (Files.exists(Paths.get(cfgFile)))
      properties.load(new FileInputStream(cfgFile))
    else{
      println("no properties file, exiting")
      println(System.getProperty("user.dir"))
      System.exit(0)
    }

    master = properties.getProperty("master")
    println(master)
    fileName = properties.getProperty("input.file.name")
    println(fileName)
    fileNameJoin = properties.getProperty("input.file.name.join")
    println(fileNameJoin)
    outFileName = properties.getProperty("output.file.name")
    println(outFileName)
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
    datePresent = properties.getProperty("date.present").toBoolean
    println(datePresent)

    val uuidSufix = properties.getProperty("uuid.filename.sufix").toBoolean
    println(uuidSufix)

    //System.exit(0)

    if(uuidSufix){

      outFileName = outFileName + "_" + randomUUID().toString.replace("-", "_")

    }

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

      mainpartDF2 = spark.read.format("parquet")
        //.option("sep", ",")
        //.option("inferSchema", "true")
        .option("header", "true")
        .load(fileNameJoin)
        .repartition(parThreads)


    }

    /*

    if (datePresent) {
      mainpartDF = mainpartDF.withColumn("DATE", col("DATE").cast("date"))

      if(mainpartDF2 != null)
        mainpartDF2 = mainpartDF2.withColumn("DATE", col("DATE").cast("date"))
    }

     */

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
    mainpartDF = mainpartDF.withColumn("BENEFITS", col("BENEFITS") +1)

    //mainpartDF.show()
    //mainpartDF.printSchema()


    println(mainpartDF.count())


    //************************************************************************************

    stageFlow = stageFlow.replaceAll("\\s", "")
    println(stageFlow)
    val stages = stageFlow.split(",")

    var storageDataFrame: DataFrame = mainpartDF

    for (i <- stages){

      //println(i)
      val s = i.split(":")

      if (s.length > 1)
        storageDataFrame = stageRunner(s(0), s(1), mainpartDF, mainpartDF2)
      else
        storageDataFrame = stageRunner(s(0), "", storageDataFrame, mainpartDF2)

    }



    //**************************************************************************************



  }


}
