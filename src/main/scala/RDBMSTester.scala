import java.io.FileInputStream
import java.nio.file.{Files, Paths}
import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.io.Source

/**********************************************************************************************************************
 *
 * Spark Job for performance measurement of dataset ingestion from RDBMS, when running at scale
 *
 * @author  Evo Eftimov
 **********************************************************************************************************************/

object RDBMSTester {

  def main(args: Array[String]): Unit = {


    //########################## Get the Parameters of the Job ##########################################
    // Assuming that application.properties is in the root folder of the spark job

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

    val outFileName = properties.getProperty("output.file.name")
    println(outFileName)
    val format = properties.getProperty("format")
    println(format)

    val parThreads = properties.getProperty("spark.parallel.threads").toInt
    println(parThreads)
    val fileFormat = properties.getProperty("format")
    println(fileFormat)
    val dbMode = properties.getProperty("db.mode")
    println(dbMode)

    val dbServer = properties.getProperty("db.server.jdbc.url")
    println(dbServer)
    val dbTableName = properties.getProperty("db.table.name")
    println(dbTableName)
    val dbUser = properties.getProperty("db.username")
    println(dbUser)
    val dbPassword = properties.getProperty("db.password")
    println(dbPassword)

    val sparkT = SparkSession.builder
      .master(master)
      .appName("rdbms-tester")



    val spark = sparkT.getOrCreate()

    if(dbMode.equalsIgnoreCase("readtest")) {

      var jdbcDF = spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql:" + dbServer)
        .option("dbtable", dbTableName)
        .option("user", dbUser)
        .option("password", dbPassword)
        .load().repartition(parThreads)


      //perform-simulate some simple data processing with the db query result set
      jdbcDF = jdbcDF.withColumn("BENEFITS", col("BENEFITS") + 1)

      println(jdbcDF.count())
      jdbcDF.show()

      jdbcDF.write
        .mode("overwrite")
        .format(format)
        .option("header", "true")
        .save(outFileName)

    }

    /*
    val connectionProperties = new Properties()
    connectionProperties.put("user", "username")
    connectionProperties.put("password", "password")

    val jdbcDF2 = spark.read
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
    // Specifying the custom data types of the read schema
    connectionProperties.put("customSchema", "id DECIMAL(38, 0), name STRING")
    val jdbcDF3 = spark.read
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
    */


    if(dbMode.equalsIgnoreCase("writetest")) {

      // Saving data to a JDBC source
     /* var jdbcDF
      .write
        .format("jdbc")
        .option("url", "jdbc:postgresql:dbserver")
        .option("dbtable", "schema.tablename")
        .option("user", "username")
        .option("password", "password")
        .save()
      */

    }

    /*
    jdbcDF2.write
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

    // Specifying create table column data types on write
    jdbcDF.write
      .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

     */

  }
}
