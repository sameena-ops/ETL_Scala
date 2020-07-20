package extract

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}

class ExtractTemperaturesWithSchema {

  def extractTempsData(filepath: String, sparkSession: SparkSession): DataFrame = {
    val tschema = StructType(Array(StructField("sid", StringType),
      StructField("date", DateType),
      StructField("mtype", StringType),
      StructField("value", DoubleType)))
    return sparkSession.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv(filepath)
    //df2017.schema.printTreeString()
  }

}
