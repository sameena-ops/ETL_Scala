import builder.SessionBuilder
import extract.{ExtarctStationsWithSchema, ExtractTemperaturesWithSchema}
import transform.GetStationsTemperatures

object ExtractTransformJob {
  def main(args: Array[String]){
    val session = new SessionBuilder().sparkSession()
  // SparkSession.builder().master("local[*]").appName("Annual data").getOrCreate()
  val tempsfile = args(0);
  val stationsfile = args(1);
  session.sparkContext.setLogLevel("WARN")

  val df2017 = new ExtractTemperaturesWithSchema().extractTempsData(tempsfile, session)
  df2017.show()
  df2017.schema.printTreeString()

  val sdf = new ExtarctStationsWithSchema().extractStationsData(stationsfile, session)
  sdf.show()
  val finaltable = new GetStationsTemperatures().transformDataStationsTempData(session,df2017,sdf)
  finaltable.count()
  finaltable.show()
  println("successfully transformed the data")
}
}
