import builder.SessionBuilder
import load.LoadDataToSQLServer
import transform.GetStationsTemperatures
import extract.ExtarctStationsWithSchema
import extract.ExtractTemperaturesWithSchema
object LoadJob {
    def main(args: Array[String]){
        val session = new SessionBuilder().sparkSession()
        val tempsfile = args(0);
        val stationsfile = args(1);

        session.sparkContext.setLogLevel("WARN")

        val df2017 = new ExtractTemperaturesWithSchema().extractTempsData(tempsfile, session)
        val sdf = new ExtarctStationsWithSchema().extractStationsData(stationsfile, session)
        val finaltable = new GetStationsTemperatures().transformDataStationsTempData(session,df2017,sdf)

        new LoadDataToSQLServer().loadDataToDb(session, finaltable)
        println("Temperatures Database has been loaded with the stationsTemperatures data")
        session.close()
}
}
