package part6advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/*
  - Event Time: moment when the record was generated
  - set by the data generation system
  - usually a column in the dataset
  - different from processing time (processing time is the time the record arrives at spark engine)
    - there two times are usually different (sometimes very different)
  - can do aggregations on time-based groups
  - Essential concepts:
    - window durations
    - window sliding intervals
  - As opposed to DStreams
    - records are not necessarily tken between "now" and a certain past date"
    - we can control output modes
  - E.g. assume we have a count by window in "complete" mode
    - note: update happens at end of batch window
    - batch time = 10 minutes
    - window duration = 20 minutes (the magnitude of the aggregation, 4:50 - 5:10 = 20 min)
    - window sliding interval = 10 minutes (the difference between the start time of every agg key; 4:50 - 5:00 = 10min)
    - looks different in append mode; basically each batch creates it's own independant table
    -      5:04                                                                                      5:22
           5:01        |         5:11          |                    |         5:19           |       5:18        |
      ---------------------------------------------------------------------------------------------------------------
    - "complete" mode:
      window  | count      window  | count         window  | count        window  | count        window  | count
      4:50-5:10    2       4:50-5:10    2          4:50-5:10    2         4:50-5:10    2         4:50-5:10    2
      5:00-5:20    2       5:00-5:20    3          5:00-5:20    3         5:00-5:20    4         5:00-5:20    5
                           5:10-5:30    1          5:10-5:30    1         5:10-5:30    2         5:10-5:30    4
                                                                                                 5:20-5:40    1

    - "append" mode (only add new records in every batch trigger):
      window  | count      window  | count         window  | count        window  | count        window  | count
      4:50-5:10    2       5:00-5:20    1                                 5:00-5:20    1         5:00-5:20    1
      5:00-5:20    2       5:10-5:30    1                                 5:10-5:30    1         5:10-5:30    2
                                                                                                 5:20-5:40    1

    - "update" mode (only have the window for which the count has been updated, incl new windows)
      window  | count      window  | count         window  | count        window  | count        window  | count
      4:50-5:10    2       5:00-5:20    3                                 5:00-5:20    4         5:00-5:20    5
      5:00-5:20    2       5:10-5:30    1                                 5:10-5:30    2         5:10-5:30    4
                                                                                                 5:20-5:40    1


 */
object EventTimeWindows {

  val spark = SparkSession.builder()
    .appName("Event Time Windows")
    .master("local[2]")
    .getOrCreate()

  val onlinePurchaseSchema = StructType(Array(
    StructField("id", StringType),
    StructField("time", TimestampType),
    StructField("item", StringType),
    StructField("quantity", IntegerType)
  ))

  def readPurchasesFromSocket() = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()
    .select(from_json(col("value"), onlinePurchaseSchema).as("purchase"))
    .selectExpr("purchase.*")

  def readPurchasesFromFile() = spark.readStream
    .schema(onlinePurchaseSchema)
    .json("src/main/resources/data/purchases")

  def aggregatePurchasesByTumblingWindow() = {
    val purchasesDF = readPurchasesFromSocket()

    val windowByDay = purchasesDF
      // window(col, windowDuration, sliding interval
     // .groupBy(window(col("time"), "1 day", "1 hour").as("time"))
      // note: window function key will be a struct field/column {start,end}
      // column `time` has to be of type TimeStamp
      .groupBy(window(col("time"), "1 day").as("time")) // tumbling window (no two windows overlap): sliding duration == window duration
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def aggregatePurchasesBySlidingWindow() = {
    val purchasesDF = readPurchasesFromSocket()

    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day", "1 hour").as("time")) // struct column: has fields {start, end}
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
    * Exercises
    * 1) Show the best selling product of every day, +quantity sold.
    * 2) Show the best selling product of every 24 hours, updated every hour.
    */

  def bestSellingProductPerDay() = {
    val purchasesDF = readPurchasesFromFile()

    val bestSelling = purchasesDF
      .groupBy(col("item"), window(col("time"), "1 day").as("day"))
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("day").getField("start").as("start"),
        col("day").getField("end").as("end"),
        col("item"),
        col("totalQuantity")
      )
      .orderBy(col("day"), col("totalQuantity").desc)

    bestSelling.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def bestSellingProductEvery24h() = {
    val purchasesDF = readPurchasesFromFile()

    val bestSelling = purchasesDF
      .groupBy(col("item"), window(col("time"), "1 day", "1 hour").as("time"))
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("item"),
        col("totalQuantity")
      )
      .orderBy(col("start"), col("totalQuantity").desc)

    bestSelling.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /*
    For window functions, windows start at Jan 1 1970, 12 AM GMT
      - e.g. if your computer is based in GMT +2 and you do a daily window, your days will start at GMT +2
      - if you do hourly, the first window will be computed based on your first observation
   */

  def main(args: Array[String]): Unit = {
    bestSellingProductEvery24h()
  }
}
