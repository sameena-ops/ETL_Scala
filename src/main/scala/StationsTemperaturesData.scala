import builder.SessionBuilder
import extract.{ExtarctStationsWithSchema, ExtractTemperaturesWithSchema}
import load.LoadDataToSQLServer
import transform.GetStationsTemperatures

object StationsTemperaturesData extends App{

  val session = new SessionBuilder().sparkSession()
  // SparkSession.builder().master("local[*]").appName("Annual data").getOrCreate()
  val tempsfile = "./src/data/2017.csv"
  val stationsfile = "./src/data/ghcnd-stations.txt"
  session.sparkContext.setLogLevel("WARN")

  val df2017 = new ExtractTemperaturesWithSchema().extractTempsData(tempsfile, session)
  df2017.show()
  df2017.schema.printTreeString()

  val sdf = new ExtarctStationsWithSchema().extractStationsData(stationsfile, session)
  sdf.show()
  val finaltable = new GetStationsTemperatures().transformDataStationsTempData(session,df2017,sdf)
  finaltable.count()
  finaltable.show()

  new LoadDataToSQLServer().loadDataToDb(session, finaltable)

  println("Temperatures Database has been loaded with the stationsTemperatures data")
}
