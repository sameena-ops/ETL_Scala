package load

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

class LoadDataToSQLServer {
  def loadDataToDb(sparkSession: SparkSession, dataFrame: DataFrame): Unit = {
    val sqlppt = new Properties()
    sqlppt.put("user", "sa")
    sqlppt.put("password", "vodqa@123")
    sqlppt.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    dataFrame.write.mode("append").jdbc(url = "jdbc:sqlserver://192.168.0.107:1401;databaseName=vodqa",
      table = "temperatures", connectionProperties = sqlppt)
  }
}
