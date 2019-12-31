import java.lang.System._
import sys.process._

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.io.FileNotFoundException
import java.util.Properties

import scala.io.Source
import scala.io.Source.fromURL

/**********************************************************************************************************************
 *
 * Spark Job for scalable deletion of Individual Records from large Parquet datasets. Required for maintaining GDPR
 * compliance for Citizen PII. Takes advantage of Spark Vectorization.
 * The Citizen records requested for deletion are put in an Input Table. Then the Spark Job reads the Input Table and for
 * each Citizen record there, it deletes its matching counterpart in the main Parquet dataset and then writes a new
 * Parquet dataset, without the Citizens Records requested for deletion
 *
 * @author  Evo Eftimov
 *
 *
 *          citizens.json (the main table from which the spark job deletes individual records)
 *
 *          {"nin":30234324, "name":"Andy", "dob":"1960"}
 *          {"nin":30234323, "name":"Peter", "dob":"1950"}
 *          {"nin":30234333, "name":"Justin", "dob":"1940"}
 *          {"nin":30234328, "name":"Rose", "dob":"1945"}
 *          {"nin":30234329, "name":"Mary", "dob":"1940"}
 *          {"nin":30234332, "name":"Sam", "dob":"1940"}
 *          {"nin":30231324, "name":"Peter", "dob":"1960"}
 *          {"nin":30238323, "name":"Jessy", "dob":"1950"}
 *          {"nin":30239333, "name":"John", "dob":"1940"}
 *          {"nin":32234328, "name":"Terry", "dob":"1945"}
 *          {"nin":30334329, "name":"Gordon", "dob":"1940"}
 *          {"nin":30244332, "name":"Lisa", "dob":"1940"}
 *
 *
 *          citizens-del.json (the input table (input parameter for the spark job) containing records representing GDPR requests to delete from the main dataset)
 *
 *          {"nin":30234324, "name":"Andy", "dob":"1960"}
 *          {"nin":30234323, "name":"Peter", "dob":"1950"}
 *
 *
 *          The state of the main table after deleting the records specified in the input table
 *
 *          {"nin":30234333, "name":"Justin", "dob":"1940"}
 *          {"nin":30234328, "name":"Rose", "dob":"1945"}
 *          {"nin":30234329, "name":"Mary", "dob":"1940"}
 *          {"nin":30234332, "name":"Sam", "dob":"1940"}
 *          {"nin":30231324, "name":"Peter", "dob":"1960"}
 *          {"nin":30238323, "name":"Jessy", "dob":"1950"}
 *          {"nin":30239333, "name":"John", "dob":"1940"}
 *          {"nin":32234328, "name":"Terry", "dob":"1945"}
 *          {"nin":30334329, "name":"Gordon", "dob":"1940"}
 *          {"nin":30244332, "name":"Lisa", "dob":"1940"}
 *
 *
 *
 **********************************************************************************************************************/

//parameter list for optimal Parquet quering and processing including taking advantage of Vectorization - spark default values
//spark.sql.parquet.enableVectorizedReader - true
//spark.sql.inMemoryColumnarStorage.enableVectorizedReader - true
//spark.sql.inMemoryColumnarStorage.compressed - true
//spark.sql.inMemoryColumnarStorage.batchSize - 10000
//spark.sql.parquet.filterPushdown - true
//spark.sql.parquet.columnarReaderBatchSize - 4K
//spark.sql.parquet.recordLevelFilter.enabled - false



object PII {

  def main(args: Array[String]): Unit = {

    //supress all spark status messages sent to the driver console
    //Logger.getLogger("org").setLevel(Level.OFF)
    //Logger.getLogger("akka").setLevel(Level.OFF)

    //get the Scala version we are running on
    println(scala.util.Properties.scalaPropOrElse("version.number", "unknown"))
    //for python
    //print(sys.version)
    //print(sys.version_info)
    //exit(0)

    //########################## Get the Parameters of the Job ##########################################
    // Assuming that application.properties is in the root folder of your applicationval url = getClass.getResource("application.properties")
    val url = getClass.getResource("application.properties.prop")
    val properties: Properties = new Properties()

    if (url != null) {
      val source = Source.fromURL(url)
      properties.load(source.bufferedReader())
    }
    else {
      //logger.error("properties file cannot be loaded at path " +path)
      throw new FileNotFoundException("Properties file cannot be loaded")
    }

    val genDS = properties.getProperty("generate.synthetic.datasets").toBoolean
    println(genDS)
    val mainTable = properties.getProperty("main.table")
    println(mainTable)
    val gdprTable = properties.getProperty("gdpr.table")
    println(gdprTable)
    val tmpDir = properties.getProperty("tmp.dir")
    println(tmpDir)
    val mvCommand = properties.getProperty("mv.command")
    println(mvCommand)
    val partitionCol = properties.getProperty("partitioning.column")
    println(partitionCol)
    val master = properties.getProperty("master")
    println(master)
    val dwhLocation = properties.getProperty("dwh.location")
    println(dwhLocation)
    val controledExec = properties.getProperty("controled.exec").toBoolean
    println(controledExec)


    //#####################################################################################################

    val spark = SparkSession.builder
      //.master(master)
      .master("yarn")
      .appName("oltp-gdpr")
      //put here any required param controlling Vectorization (see all available params listed at the beginning)
      .config("spark.sql.parquet.enableVectorizedReader ", "true")
      .config("spark.sql.warehouse.dir", dwhLocation)
      .enableHiveSupport()
      .getOrCreate()

    //create synthetic Parquet datasets, if they dont already exist- subsequently used to demonstrate the working of this prototype
    //var createParquet = true
    if(genDS) {

      //the main table where some individual records need to be deleted
      var citizensDF = spark.read.json("/opt/citizens.json")
      citizensDF.write
        .mode("overwrite")
        .format("parquet")
        .partitionBy("dob")
        .saveAsTable("citizens_partitioned")

      //the input table containing records for deletion
      citizensDF = spark.read.json("/opt/citizens-del.json")
      citizensDF.write
        .mode("overwrite")
        .format("parquet")
        .partitionBy("dob")
        .saveAsTable("citizens_del_partitioned")

    }

    var dfTable = spark.sql("SELECT * FROM " + mainTable) //this is how to get the paritions of the Input table at runtime
    dfTable .show()

    dfTable = spark.sql("SELECT * FROM " + gdprTable) //this is how to get the paritions of the Input table at runtime
    dfTable .show()

    if(genDS)
      exit(0)

    //the table partitions containing the records to be deleted (will be infered automatically by the spark job from the paritions of
    // the input table. the sample input table used here, has two paritions dob=1950 and dob=1960)
    val dfPartitions = spark.sql("SHOW PARTITIONS " + gdprTable) //this is how to get the paritions of the Input table at runtime
    dfPartitions .show()

    var dfLocation = spark.sql("DESCRIBE EXTENDED " + mainTable)
    dfLocation.show(dfLocation.count().toInt,false)
    var tvDF = dfLocation.createOrReplaceTempView("tablemetadata")
    dfLocation = spark.sql("SELECT * FROM tablemetadata WHERE col_name = 'Location'")
    dfLocation.show(dfLocation.count().toInt,false)
    val filepath:String = dfLocation.toLocalIterator().next().toSeq(1).toString.substring(5)
    println(filepath)

    if(controledExec) {
      println("Will move the Target Partions. Press ENTER to continue")
      scala.io.StdIn.readLine() //waits for any key to be pressed
    }

    //move the table partitions to be updated to tmp folder, thus creating a tmp table containing only
    //the target partitions from the main table with records to be updated
    //in the final production version, check the exit code to make sure the folder move operation completed successfuly

    //var exitCode = ("mv /opt/dwh/citizens_partitioned/" + p1 + " /opt/tmp").!
    //exitCode = ("mv /opt/dwh/citizens_partitioned/" + p2 + " /opt/tmp").!

    //the following derives and processes Target partiions automatically
    if(mvCommand.equalsIgnoreCase("standalone")) {
      dfPartitions.foreach { row =>
        row.toSeq.foreach { col => ("mv " + filepath + "/" + col + " " + tmpDir).! }
      }
    }
    else{
      dfPartitions.foreach { row =>
        row.toSeq.foreach { col => ("hadoop dfs -mv " + filepath + "/" + col + " " + tmpDir).! }
      }
    }

    //read the tmp table containing onlly the target partitions of the main table (note there is no need to register the table
    //definition with Hive Metastore - Spark automatically recovers table columns from partition folders
    var mainpartDF = spark.read.parquet(tmpDir)
    mainpartDF.createOrReplaceTempView("mainpartitions")
    var citizensDF = spark.sql("SELECT * FROM mainpartitions")
    citizensDF.show()

    //deletion against Citizen records in an input dataset/table which represent a new batch of Citizen PII Records requested for deletion
    var citizensUpdatedDF = spark.sql("SELECT * FROM mainpartitions WHERE nin NOT IN (SELECT nin FROM " + gdprTable + ")")
    citizensUpdatedDF.show()

    if(controledExec) {
      println("Will create the updated main table. Press ENTER to continue")
      scala.io.StdIn.readLine() //waits for any key to be pressed
    }


    //add and write the updated main table partitijns to the hive managed main table (now with the requested records deleted from the specified partitions)
    citizensUpdatedDF.write
      .mode("append")
      .format("parquet")
      .partitionBy(partitionCol)
      .saveAsTable(mainTable)

    //show the end result from the record deletion operation by re-reading and showing the full main table
    var citizensUpdatedFullDF = spark.sql("SELECT * FROM " + mainTable)
    citizensUpdatedFullDF.show()

    spark.stop()

    //remove the temporary partitions
    if(mvCommand.equalsIgnoreCase("standalone")) {
      var exitCode = Seq("/bin/sh", "-c", "rm -Rf " + tmpDir + "/*").!
      println(exitCode)
    }
    else{
      var exitCode = Seq("/bin/sh", "-c", "hadoop dfs -rm -R -f " + tmpDir + "/*").!
      println(exitCode)
    }

  }

}
