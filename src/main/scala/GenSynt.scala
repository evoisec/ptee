import java.io.FileNotFoundException
import java.nio.file.{Files, Paths}
import java.sql.Date
import java.util.Properties

import PII.getClass
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DateType, DecimalType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

import scala.io.Source
import scala.util.Random
import java.util.UUID.randomUUID
import java.time.LocalDate
import java.time.temporal.ChronoUnit.DAYS

import scala.util.Random

/**********************************************************************************************************************
 *
 * Spark Job for scalable generation (leverages the parallel compute power of the entire Hadoop Cluster)
 * of synthetic big datasets with configurable data schema and dataset size
 *
 * @author  Evo Eftimov
 **********************************************************************************************************************/

object GenSynt {

  def random(from: LocalDate, to: LocalDate): LocalDate = {
    val diff = DAYS.between(from, to)
    val random = new Random(System.nanoTime) // You may want a different seed
    from.plusDays(random.nextInt(diff.toInt))
  }

  def main(args: Array[String]): Unit = {


    //########################## Get the Parameters of the Job ##########################################
    // Assuming that application.properties is in the root folder of the spark job

    val properties: Properties = new Properties()

    if (Files.exists(Paths.get("./dsgen.properties"))){
      val source = Source.fromURL("file:./dsgen.properties")
      properties.load(source.bufferedReader())
    }
    else if (Files.exists(Paths.get("dsgen.properties"))) {
      val source = Source.fromURL("file:dsgen.properties")
      properties.load(source.bufferedReader())
    }
    else{
      println("no properties file, exiting")
      println(System.getProperty("user.dir"))
      System.exit(0)
    }

    val master = properties.getProperty("master")
    println(master)
    val dbName = properties.getProperty("db.name")
    println(dbName)
    val tableName = properties.getProperty("table.name")
    println(tableName)
    val fileName = properties.getProperty("file.name")
    println(fileName)
    val partitioned = properties.getProperty("partitioned").toBoolean
    println(partitioned)
    val partitionName = properties.getProperty("partition.field.name")
    println(partitionName)

    val hiveSupport = properties.getProperty("hive.support").toBoolean
    println(hiveSupport)
    val parThreads = properties.getProperty("spark.parallel.threads").toInt
    println(parThreads)
    val storage = properties.getProperty("storage")
    println(storage)
    val format = properties.getProperty("format")
    println(format)

    val nameStr = properties.getProperty("name.string.len").toInt
    println(nameStr)
    val addressStr = properties.getProperty("address.string.len").toInt
    println(addressStr)
    val ninInt = properties.getProperty("nin.int.len").toInt
    println(ninInt)
    val benInt = properties.getProperty("benefits.int.len").toInt
    println(benInt)
    val accNameStr = properties.getProperty("accname.int.len").toInt
    println(accNameStr)

    val initRec1 = properties.getProperty("initial.record.count1").toInt
    println(initRec1)
    val outerIter1 = properties.getProperty("outer.iterations1").toInt
    println(outerIter1)
    val innerIter1 = properties.getProperty("inner.iterations1").toInt
    println(innerIter1)

    val outerIter2 = properties.getProperty("outer.iterations2").toInt
    println(outerIter2)
    val innerIter2 = properties.getProperty("inner.iterations2").toInt
    println(innerIter2)

    //System.exit(0)

    val sparkT = SparkSession.builder
      .master(master)
      .appName("synt-gen")
      //put here any required param controlling Vectorization (see all available params listed at the beginning)
      .config("spark.sql.parquet.enableVectorizedReader ", "true")


     if (hiveSupport) {

       sparkT.config("spark.sql.warehouse.dir", "/opt/dwh")
       sparkT.enableHiveSupport()

     }

    val spark = sparkT.getOrCreate()

    import spark.implicits._

    //********************* Generate Synthetic Data from within the Spark Job *******************************

    def randomStringFromCharList(length: Int, chars: Seq[Char]): String = {
      val sb = new StringBuilder
      for (i <- 1 to length) {
        val randomNum = util.Random.nextInt(chars.length)
        sb.append(chars(randomNum))
      }
      sb.toString
    }

    def randomAlpha(length: Int): String = {
      val chars = ('a' to 'z') ++ ('A' to 'Z')
      randomStringFromCharList(length, chars)
    }

    var dd = (1 to 1)
    println(dd)
    var ggg = dd.flatMap( x => (1 to initRec1).map(_ => x) )
    println(ggg)
    for (b <- (1 to outerIter1)){

      ggg = ggg.flatMap( x => (1 to innerIter1).map(_ => x) )
      println(ggg)

    }
    // num of internal iteration on the power of external iter times inital element count = total
    println(ggg)
    println(ggg.length)
    val kkk = ggg.map(x => Row(1))
    println(kkk)
    println(kkk.length)

    //System.exit(0)

    var schemaTyped = new StructType()

    //************* The Schema of the Synthetic Dataset **************************************

    schemaTyped = schemaTyped.add("NIN", LongType, true)
    //schemaTyped = schemaTyped.add("NIN", IntegerType, true)
    //schemaTyped = schemaTyped.add("NIN", "String", true)
    schemaTyped = schemaTyped.add("NAME", "String", true)
    schemaTyped = schemaTyped.add("BENEFITS", DoubleType, true)
    schemaTyped = schemaTyped.add("ADDRESS", "String", true)
    schemaTyped = schemaTyped.add("BALANCE", DoubleType, true)
    schemaTyped = schemaTyped.add("ACC_NAME", "String", true)
    schemaTyped = schemaTyped.add("DATE", DateType, true)

    var rdd = spark.sparkContext.parallelize(kkk)

    var dataRow = rdd.repartition(parThreads)

    for( i <- (1 to outerIter2) )
    {
      dataRow = dataRow.flatMap( x => (1 to innerIter2).map(_ => x) )
    }


    val from = LocalDate.of(2018, 1, 1)
    val to = LocalDate.of(2020, 1, 1)

    //Several Data Row Schemas available for tests
    //dataRow = dataRow.map(x => Row(Random.nextInt(ninInt), randomUUID().toString, Random.nextDouble(), randomAlpha(addressStr), Random.nextDouble(), randomAlpha(accNameStr), Date.valueOf(random(from, to).toString)))
    //dataRow = d1.map(x => Row(randomUUID().toString, randomUUID().toString, Random.nextDouble(), randomAlpha(addressStr), Random.nextDouble(), randomAlpha(accNameStr), Date.valueOf(random(from, to).toString)))
    dataRow = dataRow.map(x => Row(  randomUUID().getLeastSignificantBits().abs,  randomUUID().toString, Random.nextDouble(), randomAlpha(addressStr), Random.nextDouble(), randomAlpha(accNameStr), Date.valueOf(random(from, to).toString)))

    //println(d1.collect().toList)


    val df = spark.sqlContext.createDataFrame(dataRow, schemaTyped)

    df.printSchema()
    df.show()

    println(df.count())

    //System.exit(0)

    //**************** Persist the Synthetic Data ****************************************

    if(storage.equalsIgnoreCase("file")) {

      if (partitioned) {

        df.write
          .mode("overwrite")
          .format(format)
          .option("header", "true")
          .partitionBy(partitionName)
          .save(fileName)

      }
      else {

        df.write
          .mode("overwrite")
          .format(format)
          .option("header", "true")
          .save(fileName)

      }

    }

    if(storage.equalsIgnoreCase("db")) {

      if (partitioned) {

        df.write
          .mode("overwrite")
          .format(format)
          .partitionBy(partitionName)
          .saveAsTable(dbName + "." + tableName)

      }
      else{

        df.write
          .mode("overwrite")
          .format(format)
          .saveAsTable(dbName + "." + tableName)

      }

    }

  }

}
