package builder
import org.apache.spark.sql.SparkSession
class SessionBuilder {

  def sparkSession(): SparkSession ={

    val session = SparkSession.builder().master("local[*]").appName("Annual data").getOrCreate()
    return session
  }

}
