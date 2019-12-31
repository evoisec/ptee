/*

object GenSyntDS {

  bin/spark-shell --driver-class-path postgresql-9.4.1207.jar --jars postgresql-9.4.1207.jar

  val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
  recordsDF.createOrReplaceTempView("records")

  // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
  // Loading data from a JDBC source
  val jdbcDF = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql:dbserver")
    .option("dbtable", "schema.tablename")
    .option("user", "username")
    .option("password", "password")
    .load()

  val connectionProperties = new Properties()
  connectionProperties.put("user", "username")
  connectionProperties.put("password", "password")
  val jdbcDF2 = spark.read
    .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
  // Specifying the custom data types of the read schema
  connectionProperties.put("customSchema", "id DECIMAL(38, 0), name STRING")
  val jdbcDF3 = spark.read
    .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

  // Saving data to a JDBC source
  jdbcDF.write
    .format("jdbc")
    .option("url", "jdbc:postgresql:dbserver")
    .option("dbtable", "schema.tablename")
    .option("user", "username")
    .option("password", "password")
    .save()

  jdbcDF2.write
    .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

  // Specifying create table column data types on write
  jdbcDF.write
    .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
    .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

}

examples/src/main/scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala" in the Spark repo.


  import org.apache.spark.sql.Row
  import spark.implicits._

  val data = (0 to rows).map(_ => Seq.fill(columns)(Random.nextDouble))
  val rdd = sc.parallelize(data)
  val df = rdd.map(s => Row.fromSeq(s)).toDF()





  import org.apache.spark.sql.{Row, SparkSession}
  import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

  def start(rows: Int, cols: Int, col: String, spark: SparkSession): Unit = {

  val data = (1 to rows).map(_ => Seq.fill(cols)(Random.nextDouble))

  val colNames = (1 to cols).mkString(",")
  val sch = StructType(colNames.split(",").map(fieldName => StructField(fieldName, DoubleType, true)))

  val rdd = spark.sparkContext.parallelize(data.map(x => Row(x:_*)))
  val df = spark.sqlContext.createDataFrame(rdd, sch)

  df.printSchema()

  spark.stop()
  }

*/