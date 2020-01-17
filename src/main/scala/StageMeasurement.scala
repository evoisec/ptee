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


    mainpartDF = mainpartDF.withColumn("NIN", col("NIN") +1)

    //mainpartDF.show()
    //mainpartDF.printSchema()

    mainpartDF.createOrReplaceTempView("perftest")

    var result = spark.sql("SELECT * FROM perftest")

    //result.show()
    //result.printSchema()

    println(result.count())


    var result1 : RelationalGroupedDataset = null

    if (stageNumber == 0){
      //do nothing - this is a direct input to output pipeline
    }
    else{

      var sss : RDD[(Any, Iterable[Row])] = null

      if (stageNumber >= 1){

        println("Starting Stage 1")

        //result1 = result.groupBy("NIN")
        //println(result1.count().show())

        result.groupBy("NIN").count().show()

        sss = result.rdd.groupBy(x => x(0))
        sss.count()


        var o = result.groupBy("NIN").agg(sum(mainpartDF.col("BENEFITS")))
        o.show()
        o = mainpartDF.groupBy("NIN").count().sort($"count".desc)
        o.show()
        o = mainpartDF.groupBy("NIN").sum("BENEFITS")
        o.show()

        var i = mainpartDF.cube($"NIN", $"ADDRESS").agg(Map(
          "BENEFITS" -> "avg",
          "BALANCE" -> "max"
        ))
        i.show()

        mainpartDF.stat.freqItems(Seq("NIN")).show(100)

        mainpartDF.withColumn("new_column", lit(10)).show()


      }
      if (stageNumber >= 2){

        println("Starting Stage 2")
        result.rdd.groupBy(x => x(4)).count()

      }
      if (stageNumber >= 3){

        println("Starting Stage 3")
        result = result.repartition(parThreads)

      }

    }


    if (partitioned) {

      result.write
        .mode("overwrite")
        .format(fileFormat)
        .option("header", "true")
        .partitionBy(partitionName)
        .save(outFileName)

    }
    else {

      result.write
        .mode("overwrite")
        .format(fileFormat)
        .option("header", "true")
        .save(outFileName)

    }

  }


}
