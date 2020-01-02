import java.io.FileNotFoundException
import java.nio.file.{Files, Paths}
import java.util.Properties

import PII.getClass
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType, IntegerType, StructField, StructType}

import scala.io.Source
import scala.util.Random

object GenSynt {

  def main(args: Array[String]): Unit = {


    //########################## Get the Parameters of the Job ##########################################
    // Assuming that application.properties is in the root folder of your applicationval url = getClass.getResource("application.properties")

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

    val dbName = properties.getProperty("db.name")
    println(dbName)
    val initRec = properties.getProperty("initial.record.count").toInt
    println(initRec)
    val outerIter = properties.getProperty("outer.iterations").toInt
    println(outerIter)
    val innerIter = properties.getProperty("inner.iterations").toInt
    println(innerIter)
    val master = properties.getProperty("master")
    println(master)

    //System.exit(0)

    val spark = SparkSession.builder
      .master(master)
      .appName("oltp-gdpr")
      //put here any required param controlling Vectorization (see all available params listed at the beginning)
      .config("spark.sql.parquet.enableVectorizedReader ", "true")
      //.config("spark.sql.warehouse.dir", "/opt/dwh")
      //.enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

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
    var ggg = dd.flatMap( x => (1 to initRec).map(_ => x) )
    println(ggg)
    for (b <- (1 to outerIter)){

      ggg = ggg.flatMap( x => (1 to innerIter).map(_ => x) )
      println(ggg)

    }
    // num of internal iteration on the pwer of external iter times inital element count = total
    println(ggg)
    println(ggg.length)
    val kkk = ggg.map(x => Row(randomAlpha(4), Random.nextInt(1000)))
    println(kkk)
    println(kkk.length)

    //System.exit(0)


    var schemaTyped = new StructType()

    schemaTyped = schemaTyped.add("NIN", IntegerType, true)
    schemaTyped = schemaTyped.add("NAME", "String", true)
    schemaTyped = schemaTyped.add("BENEFITS", IntegerType, true)
    schemaTyped = schemaTyped.add("ADDRESS", "String", true)

    var rdd = spark.sparkContext.parallelize(kkk)

    var d1 = rdd.repartition(2)

    for( i <- (0 to 1) )
    {
      d1 = d1.flatMap( x => (1 to 2).map(_ => x) )
    }

    d1 = d1.map(x => Row(Random.nextInt(1000), randomAlpha(4), Random.nextInt(1000), randomAlpha(8)))

    //println(d1.collect().toList)


    val df = spark.sqlContext.createDataFrame(d1, schemaTyped)


    df.printSchema()
    df.show()

    println(df.count())

    //System.exit(0)

    df.write
      .mode("overwrite")
      .format("csv")
      //.partitionBy("dob")
      //.saveAsTable(dbName + ".synt")
      .save("/opt/synt.csv")

    //val dff = spark.sql("SELECT * FROM " + dbName + ".synt")
    //dff.show()

  }

}
