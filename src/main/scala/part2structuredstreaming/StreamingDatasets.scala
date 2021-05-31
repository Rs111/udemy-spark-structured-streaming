package part2structuredstreaming

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import common._ // import pre-built schemas
/*
  Streaming datasets are TypeSafe Structured Streams
  - note sure if the point below is still true
  - note: lambda is opaque to spark: https://www.protechtraining.com/blog/post/6-steps-to-get-top-performance-from-the-changes-in-apache-spark-20-909
 */
object StreamingDatasets {

  val spark = SparkSession.builder()
    .appName("Streaming Datasets")
    .master("local[2]")
    .getOrCreate()

  // include encoders for DF -> DS transformations
  import spark.implicits._

  def readCars(): Dataset[Car] = {
    // useful for DF -> DS transformations
    val carEncoder = Encoders.product[Car] // no need for this, as imported spark implicits

    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // DF with single string column "value"
      .select(from_json(col("value"), carsSchema).as("car")) // composite column (struct)
      .selectExpr("car.*") // DF with multiple columns
      .as[Car] // encoder can be passed implicitly with spark.implicits
  }

  def showCarNames() = {
    val carsDS: Dataset[Car] = readCars()

    // transformations here
    val carNamesDF: DataFrame = carsDS.select(col("Name")) // DF

    // collection transformations maintain type info
    val carNamesAlt: Dataset[String] = carsDS.map(_.Name)

    carNamesAlt.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  /**
    * Exercises
    *
    * 1) Count how many POWERFUL cars we have in the DS (HP > 140)
    * 2) Average HP for the entire dataset
    *   (use the complete output mode)
    * 3) Count the cars by origin
    */

  def ex1() = {
    val carsDS = readCars()
    carsDS.filter(_.Horsepower.getOrElse(0L) > 140)
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def ex2() = {
    val carsDS = readCars()

    carsDS.select(avg(col("Horsepower")))
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def ex3() = {
    val carsDS = readCars()

    val carCountByOrigin = carsDS.groupBy(col("Origin")).count() // option 1
    val carCountByOriginAlt = carsDS.groupByKey(car => car.Origin).count() // option 2 with the Dataset API

    carCountByOriginAlt
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    ex3()
  }
}
