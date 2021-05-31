package part2structuredstreaming

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
/*
  Takeaways
    - Streaming DFs has pretty much the same APIs as non-streaming DFs
    - note:
      - aggregations work at a micobatch level
      - the append output mode not supported for aggregations without watermarks
      - aggregation doesn't support some aggregations
        - distinct methods
        - sorting or chained aggregations (i.e. multiple aggregations in the same structure)
 */
object StreamingAggregations {

  val spark = SparkSession.builder()
    .appName("Streaming Aggregations")
    .master("local[2]")
    .getOrCreate()

  def streamingCount() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val lineCount: DataFrame = lines.selectExpr("count(*) as lineCount")

    // aggregations with distinct are not supported
    // in order to fetch distinct values, spark would need to keep the entire state of the entire stream (unreasonable as data is unbounded)
    // otherwise Spark will need to keep track of EVERYTHING

    // complete re-writes the entire stream (watermark, which gives us more options, is a more advanced concept)
    lineCount.writeStream
      .format("console")
      .outputMode("complete") // append and update not supported on aggregations without watermark*
      .start()
      .awaitTermination()
  }

//  def main(args: Array[String]): Unit = {
//    streamingCount()
//  }

  def numericalAggregations(aggFunction: Column => Column): Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // aggregate here
    val numbers = lines.select(col("value").cast("integer").as("number"))
    val aggregationDF = numbers.select(aggFunction(col("number")).as("agg_so_far"))

    aggregationDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

//  def main(args: Array[String]): Unit = {
//    numericalAggregations(sum)
//  }

  def groupNames(): Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // counting occurrences of the "name" value
    val names = lines
      .select(col("value").as("name"))
      .groupBy(col("name")) // RelationalGroupedDataset
      .count()

    names.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    groupNames()
  }
}
