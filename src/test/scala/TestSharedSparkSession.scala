import Task3.{processTaxiDS, readCSVDF, readParquetDF}
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{Row, SQLContext, SQLImplicits, SparkSession}
import org.scalatest.funsuite.AnyFunSuite



class TestSharedSparkSession extends SharedSparkSession {

  import testImplicits._

  test("join - join using") {
    val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
    val df2 = Seq(1, 2, 3).map(i => (i, (i + 1).toString)).toDF("int", "str")

    checkAnswer(
      df.join(df2, "int"),
      Row(1, "1", "2") :: Row(2, "2", "3") :: Row(3, "3", "4") :: Nil)
  }

  test("join - processTaxiData") {
    val taxiZonesDF2 = readCSVDF("src/main/resources/data/taxi_zones.csv")
    val taxiDF2 = readParquetDF("src/main/resources/data/yellow_taxi_jan_25_2018")

    val actualDistribution = processTaxiDS(taxiZonesDF2, taxiDF2)

    checkAnswer(
      actualDistribution,
      Row("Bronx", 211, 3732.15, 17.69, 2.99, 0.0, 20.09) ::
        Row("Brooklyn", 3037, 51594.12, 16.99, 3.28, 0.0, 27.37) ::
        Row("EWR", 19, 1571.61, 82.72, 3.46, 0.0, 17.3) ::
        Row("Manhattan", 304266, 4298380.49, 14.13, 2.23, 0.0, 66.0) ::
        Row("Queens", 17712, 806004.27, 45.51, 11.14, 0.0, 53.5) ::
        Row("Staten Island", 4,  191.72, 47.93, 0.2, 0.0, 0.5) ::
        Row("Unknown", 6644, 114453.23, 17.23, 2.34, 0.0, 42.8) :: Nil
    )
  }

}
