package transformvalidations

import builder.SessionBuilder
import extract.{ExtarctStationsWithSchema, ExtractTemperaturesWithSchema}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import transform.GetStationsTemperatures

class ValidateLatLonMappedCorrectlyForStations extends AnyFunSuite with BeforeAndAfter {

  test("throw an exception if there is any mismatch in the station details in the transformed data"){

    val session = new SessionBuilder().sparkSession()
    val tempsfile = "./src/testdata/temperatures.csv";
    val stationsfile = "./src/testdata/dummy.txt";
    val df2017 = new ExtractTemperaturesWithSchema().extractTempsData(tempsfile, session)
    val sdf = new ExtarctStationsWithSchema().extractStationsData(stationsfile, session)
    val finaltable = new GetStationsTemperatures().transformDataStationsTempData(session,df2017,sdf)
    sdf.createOrReplaceTempView("stationsdata")
    finaltable.createOrReplaceTempView("transformeddata")
    val expectedLatitude = session.sql("""select lat from stationsdata where sid ="AEM00041217"""").toDF("lat").collectAsList()
    val exp = expectedLatitude.get(0);
    var actualLatitude = session.sql("""select lat from transformeddata where sid ="AEM00041217"""").toDF("lat").collectAsList()
    val act = actualLatitude.get(0)

    assert(exp == act)

}

  test("validates the transformed data from stationsfile to transformed data"){
    val session = new SessionBuilder().sparkSession()
    val tempsfile = "./src/testdata/temperatures.csv";
    val stationsfile = "./src/testdata/dummy.txt";
    val df2017 = new ExtractTemperaturesWithSchema().extractTempsData(tempsfile, session)
    val sdf = new ExtarctStationsWithSchema().extractStationsData(stationsfile, session)
    val finaltable = new GetStationsTemperatures().transformDataStationsTempData(session,df2017,sdf)
    finaltable.show()
    sdf.show()
    sdf.createOrReplaceTempView("stationsdata")
    finaltable.createOrReplaceTempView("transformeddata")
    val latitudeDataFrame = session.sql(
      """select stationsdata.lat as latexp,transformeddata.lat as latact from stationsdata,
        |transformeddata where stationsdata.sid = transformeddata.sid""".stripMargin)
    latitudeDataFrame.show()
   assert(latitudeDataFrame.select("latexp").collectAsList()==latitudeDataFrame.select("latact").collectAsList())
  }
}
