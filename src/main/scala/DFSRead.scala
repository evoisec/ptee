import java.io.FileInputStream
import java.nio.file.{Files, Paths}
import java.util.Properties

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.io.Source
import org.apache.spark.sql.functions._

/**********************************************************************************************************************
 *
 * Spark Job for simulation of parallel, high throughput Data Read operations on synthetic datasets with specific
 * realistic, business schemas
 *
 * @author  Evo Eftimov
 **********************************************************************************************************************/

object DFSRead {

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
    val dwhLocation = properties.getProperty("dwh.location")
    println(dwhLocation)
    val storage = properties.getProperty("storage")
    println(storage)
    val dbName = properties.getProperty("db.name")
    println(dbName)
    val tableName = properties.getProperty("table.name")
    println(tableName)


    //System.exit(0)

    val sparkT = SparkSession.builder
      .master(master)
      .appName("dfs-read")
      //put here any required param controlling Vectorization (see all available params listed at the beginning)
      .config("spark.sql.parquet.enableVectorizedReader ", "true")


    if (hiveSupport) {

      sparkT.config("spark.sql.warehouse.dir", dwhLocation)
      sparkT.enableHiveSupport()

    }


    val spark = sparkT.getOrCreate()

    import spark.implicits._

    var mainpartDF : DataFrame = null

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

      mainpartDF = spark.sql("SELECT * FROM " + dbName + "." + tableName)

    }

    mainpartDF.printSchema()
    mainpartDF.show()

    //************************************************************************************************************************
    //perform some lightweight data processing operations to simulate reading of the dataset but without heavy data processing
    //************************************************************************************************************************

    //mainpartDF.show()

   /* val s = mainpartDF
      .select("NIN","BENEFITS")
      .as[(Int, Int)]
      .map { case (n, b) => (n+1, b+1) } */


    mainpartDF = mainpartDF.withColumn("BENEFITS", col("BENEFITS") +1)

    mainpartDF.show()

    //mainpartDF.show()
    //mainpartDF.printSchema()

    mainpartDF.createOrReplaceTempView("perftest")

    val result = spark.sql("SELECT * FROM perftest")

    //result.show()
    //result.printSchema()

    println(result.count())

  }

}
