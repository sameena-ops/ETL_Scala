package extract
import builder.SessionBuilder
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class ValidateNoDuplicateStationEntries extends AnyFunSuite with BeforeAndAfter{

  test("throw an exception if there is any duplicate entry for a station"){
    val spark = new SessionBuilder().sparkSession()
    val df = new ExtarctStationsWithSchema().extractStationsData("./src/testdata/dummy.txt",spark)
    df.createOrReplaceTempView("statndata")
    val stationCount = spark.sql("""select * from statndata where sid ="AEM00041217"""").count()
    assert(stationCount ==1)
  }

}
