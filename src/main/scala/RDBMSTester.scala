import java.nio.file.{Files, Paths}
import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.io.Source

object RDBMSTester {

  def main(args: Array[String]): Unit = {


    //########################## Get the Parameters of the Job ##########################################
    // Assuming that application.properties is in the root folder of the spark job

    val properties: Properties = new Properties()

    if (Files.exists(Paths.get("./rdbmstester.properties"))) {
      val source = Source.fromURL("file:./rdbmstester.properties")
      properties.load(source.bufferedReader())
    }
    else if (Files.exists(Paths.get("rdbmstester.properties"))) {
      val source = Source.fromURL("file:rdbmstester.properties")
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


    if (hiveSupport) {

      sparkT.config("spark.sql.warehouse.dir", "/opt/dwh")
      sparkT.enableHiveSupport()

    }

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
      jdbcDF = jdbcDF.withColumn("NIN", col("NIN") + 1)

      println(jdbcDF.count())
      jdbcDF.show()

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
