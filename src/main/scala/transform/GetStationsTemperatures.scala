package transform

import org.apache.spark.sql.{DataFrame, SparkSession}

class GetStationsTemperatures {
  def transformDataStationsTempData(sparkSession: SparkSession, tempDataFrame: DataFrame,stationsDataFrame: DataFrame): DataFrame = {
    //df2017.createOrReplaceTempView("tempdata2017")
    // df.createOrReplaceTempView("praveen")
    tempDataFrame.createOrReplaceTempView("tempdata2017")
    val sqltemp = sparkSession.sql(
      """ select sid,date,(tmax+tmin)/20*1.8+32 as tavg from
                              (select sid,date,value as tmax from tempdata2017 where mtype ="TMAX" limit 1000)
                                join (select sid,date,value as tmin from tempdata2017 where mtype ="TMIN" limit 1000) using (sid,date)""")
    sqltemp.show

    sqltemp.createOrReplaceTempView("sqltemp")
    val sqlstntempdata = sparkSession.sql("""select sid,avg(tavg) as tavg from sqltemp group by sid limit 1000""")
    sqlstntempdata.show()

    sqlstntempdata.createOrReplaceTempView("avgtempperstn")
    stationsDataFrame.createOrReplaceTempView("stndataframe")
    val finaltable = sparkSession.sql(
      """select sid,lat,lon,name,tavg from (select * from stndataframe limit 1000) join (select * from avgtempperstn limit 1000) using (sid)""")
    return finaltable
  }
}
