import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._




object Task3 extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Introduction to DataSet")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def readParquetDF(path: String)(implicit spark: SparkSession): DataFrame = spark.read.load(path)

  def readCSVDF(path: String)(implicit spark: SparkSession):DataFrame =
  spark.read.format("csv")
    .option("sep", ",")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(path)

  val taxiFactTableDF = readParquetDF("src/main/resources/data/yellow_taxi_jan_25_2018")(spark)

  val taxiDistrictDF = readCSVDF("src/main/resources/data/taxi_zones.csv")(spark)


  def processTaxiDS(taxiFactTableDF: DataFrame, taxiDistrictDF: DataFrame) = {
    taxiDistrictDF
      .join(taxiFactTableDF, col("LocationID") === col("PULocationID"))
      .groupBy(col("Borough"))
      .agg(
        count("*").as("total_trips")
        , round(sum(col("total_amount")), 2).as("total_amount")
        , round(avg(col("total_amount")), 2).as("avg_amount")
        , round(avg(col("trip_distance")), 2).as("avg_trip_distance")
        , min(col("trip_distance")).as("min_trip_distance")
        , max(col("trip_distance")).as("max_trip_distance")
      )
      .orderBy(col("Borough"))
  }

  val taskresult = processTaxiDS(taxiFactTableDF, taxiDistrictDF)

  taskresult.show(30)


}

