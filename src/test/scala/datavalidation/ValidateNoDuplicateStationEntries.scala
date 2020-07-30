package datavalidation

import builder.SessionBuilder
import extract.ExtarctStationsWithSchema
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite


class ValidateNoDuplicateStationEntries extends AnyFunSuite with BeforeAndAfter{

  test("throws an exception if there is any duplicate entry for a station"){
    val sc = new SessionBuilder().sparkSession()
    val df = new ExtarctStationsWithSchema().extractStationsData("./src/testdata/dummy.txt",sc)
    df.createOrReplaceTempView("stationData")
   val df1= df.select("sid").distinct()
    val df2= df.select("sid")
    df1.show()
    df2.show()
    val count =df.select("sid").distinct().count()
    val count2 = df.select("sid").count()
    //val count =session.sql("""select * from stationData where sid="ABC"""").toDF().count()
    assert(count == count2)

  }

}


