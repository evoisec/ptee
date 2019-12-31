import java.io.FileNotFoundException
import java.util.Properties

import PII.getClass
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, DoubleType, StructField, StructType}

import scala.io.Source
import scala.util.Random

object GenSynt {

  def main(args: Array[String]): Unit = {


    //########################## Get the Parameters of the Job ##########################################
    // Assuming that application.properties is in the root folder of your applicationval url = getClass.getResource("application.properties")

    val properties: Properties = new Properties()

    val source = Source.fromURL("file:./dsgen.properties")
    properties.load(source.bufferedReader())

    val dbName = properties.getProperty("db.name")
    println(dbName)
    val outerIter = properties.getProperty("outer.iterations").toInt
    println(outerIter)
    val innerIter = properties.getProperty("inner.iterations").toInt
    println(innerIter)
    val master = properties.getProperty("master")
    println(master)

    val spark = SparkSession.builder
      .master(master)
      .appName("oltp-gdpr")
      //put here any required param controlling Vectorization (see all available params listed at the beginning)
      .config("spark.sql.parquet.enableVectorizedReader ", "true")
      .config("spark.sql.warehouse.dir", "/opt/dwh")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val data = (1 to 4).map(_ => Seq.fill(4)(Random.nextDouble).toList ::: List("waddas sfdfsdfjksdhfsdkjhf dsfjidsfhksdhfsdjfjsdhgf"))

    println(data)

    //System.exit(0)

    val k = data.flatMap( x => (1 to innerIter).map( _ => x ) )

    println(k)

    //System.exit(0)

    val colNames = (1 to 4).mkString(",")
    var sch = StructType(colNames.split(",").map(fieldName => StructField(fieldName, DoubleType, true)))
    sch = sch.add("NIN", "String", true)
    var rdd = spark.sparkContext.parallelize(data.map(x => Row(x:_*)))

    var d1 = rdd.flatMap(x => (1 to innerIter).map(_ => x))

    d1 = d1.repartition(10)

    for( i <- (0 to outerIter) )
    {
      d1 = d1.flatMap( x => (1 to innerIter).map(_ => x) )
    }


    val df = spark.sqlContext.createDataFrame(d1, sch)

    df.printSchema()
    df.show()

    df.write
      .mode("overwrite")
      .format("csv")
      //.partitionBy("dob")
      //.saveAsTable(dbName + ".synt")
      .save(dbName)

    //val dff = spark.sql("SELECT * FROM " + dbName + ".synt")
    //dff.show()

  }

}
