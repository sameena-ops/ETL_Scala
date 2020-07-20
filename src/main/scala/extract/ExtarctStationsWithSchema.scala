package extract

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

class ExtarctStationsWithSchema {
def extractStationsData(filepath: String, sparkSession: SparkSession): DataFrame = {
    val stationRDD = sparkSession.sparkContext.textFile(filepath).map { line =>
      val sid = line.substring(0, 11)
      val lat = line.substring(12, 20).toDouble
      val lon = line.substring(21, 30).toDouble
      val name = line.substring(41, 71)
      Row(sid, lat, lon, name)
    }
    val sschema = StructType(Array(StructField("sid", StringType), StructField("lat", DoubleType),
      StructField("lon", DoubleType), StructField("name", StringType)))
    return sparkSession.createDataFrame(stationRDD, sschema).cache()
  }
}
