import java.io.FileNotFoundException
import java.util.Properties

import scala.io.Source
import scala.io.Source.fromURL

object SYSTst {

  def main(args: Array[String]): Unit = {

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

      val table = properties.getProperty("hbase_table_name")
      val zquorum = properties.getProperty("localhost")
      val port = properties.getProperty("2181")

      println(table)

  }

}
