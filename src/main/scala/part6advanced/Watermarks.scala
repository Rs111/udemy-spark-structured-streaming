package part6advanced

import java.io.PrintStream
import java.net.ServerSocket
import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import scala.concurrent.duration._

/*
  - watermarks let us handle late data in time-based aggregations
  - going to refer to event time windows example again
  - with every batch, spark will:
    - update the max time ever recorded
    - update the watermark as (max time - watermark duration)
  - Guarantees
    - in every batch, all records with time > watermark will be considered
    - if using window functions, a window will be updated until the watermark surpasses the window
      - a window-based group will be kept and updated until the watermark position surpasses the end of the window (at which point spark will include it in the next batch)
  - No Guarantees
    - records whose time < watermark will not necessarily be dropped
    - the intention is to drop them, but due to the nature of distributed systems and time percision they might not
    - however the older the records are, the more likely it is for them to be dropped
  - Aggregations & Joins in append mode need watermarks
    - a watermark allows spark to drop old records from state management

  - E.g. assume we have a count by window in "complete" mode
    - note: update happens at end of batch window
    - watermark = 10 minutes (how far back we still consider records before dropping them)
      - i.e. spark will only consider records that are at most 10 minutes old (with respect to max time that spark has recorded so far)
      - e.g. for first batch, spark will record "Max" = 5:04, and "Watermark" = 4:54 because 4:54 is 10 minutes before max bs of batch
      - goes based on previous batch:
        - if in previous batch the max event time was "X", then watermark used in next batch will be "X" - watermark_time_input
    - batch time = 10 minutes
    - window duration = 20 minutes (the magnitude of the aggregation, 4:50 - 5:10 = 20 min)
    - window sliding interval = 10 minutes (the difference between the start time of every agg key; 4:50 - 5:00 = 10min)
    - note: watermarks calculated using CURRENT batch data are used in NEXT batch
    -      5:04                                                                                      5:22
           5:01        |         5:11          |                    |         5:19           |       5:08 (OLDer than previous WM of 5:09)     |
      ---------------------------------------------------------------------------------------------------------------
    - "complete" mode:
      window  | count      window  | count         window  | count        window  | count        window  | count
      4:50-5:10    2       4:50-5:10    2          4:50-5:10    2         4:50-5:10    2         4:50-5:10    2
      5:00-5:20    2       5:00-5:20    3          5:00-5:20    3         5:00-5:20    4         5:00-5:20    4
      Max=5:04             5:10-5:30    1          5:10-5:30    1         5:10-5:30    2         5:10-5:30    3
      WM=4:54              Max=5:11                Max=5:11               Max=5:19               5:20-5:40    1
                           WM=5:01                 WM=5:01                WM=5:09                Max=5:22
                                                                                                 WM=5:12
 */

object Watermarks {
  val spark = SparkSession.builder()
    .appName("Late Data with Watermarks")
    .master("local[2]")
    .getOrCreate()

  // 3000,blue

  import spark.implicits._

  // will print stuff to consol asynchronously
  // very cool; will print to consol in between the batches; shows that spark processes records in between writing out the mini batches
  // e.g. we can see updated watermark, min/max record timestamp, etc, in between the batches
  def debugQuery(query: StreamingQuery) = {
    // useful skill for debugging
    new Thread(() => {
      (1 to 100).foreach { i =>
        Thread.sleep(1000)
        val queryEventTime =
          if (query.lastProgress == null) "[]"
          else query.lastProgress.eventTime.toString

        println(s"$i: $queryEventTime")
      }
    }).start()
  }

  def testWatermark() = {
    val dataDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        val timestamp = new Timestamp(tokens(0).toLong)
        val data = tokens(1)

        (timestamp, data)
      }
      .toDF("created", "color")

    val watermarkedDF = dataDF
      // args: column + amount of time you're willing to wait
      .withWatermark("created", "2 seconds") // adding a 2 second watermark
      .groupBy(window(col("created"), "2 seconds"), col("color"))
      .count()
      .selectExpr("window.*", "color", "count")

    /*
      A 2 second watermark means
      - a window will only be considered until the watermark surpasses the window end
        - i.e. if you have a window from 20 sec ago, it will never be updated again: late arriving data that would update it as dropped due to watermark
        - with (at least) append mode, spark only outputs data that it knows is complete (i.e. once late-arriving threshold is done and cannot update it any more)
        - i.e. until watermark is updated past that window limit
      - an element/a row/a record will be considered if AFTER the watermark
     */

    val query = watermarkedDF.writeStream
      .format("console")
      // with append, new info is only printed once complete (i.e. once watermark passes)
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds)) // when new batches are being selected
      .start()

    debugQuery(query)
    query.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    testWatermark()
  }
}

// sending data "manually" through socket to be as deterministic as possible
object DataSender {
  val serverSocket = new ServerSocket(12345)
  val socket = serverSocket.accept() // blocking call for another application to connect to 12345 (i.e. nothing below will run until our spark app connects)
  val printer = new PrintStream(socket.getOutputStream) // using this to send data through the socket

  println("socket accepted")

  def example1() = {
    Thread.sleep(7000) // 7 seconds after 1970 (start of unix time)
    printer.println("7000,blue")
    Thread.sleep(1000)
    printer.println("8000,green")
    Thread.sleep(4000)
    printer.println("14000,blue")
    Thread.sleep(1000)
    printer.println("9000,red") // discarded: older than the watermark
    Thread.sleep(3000)
    printer.println("15000,red")
    printer.println("8000,blue") // discarded: older than the watermark
    Thread.sleep(1000)
    printer.println("13000,green")
    Thread.sleep(500)
    printer.println("21000,green")
    Thread.sleep(3000)
    printer.println("4000,purple") // expect to be dropped - it's older than the watermark
    Thread.sleep(2000)
    printer.println("17000,green")
  }

  def example2() = {
    printer.println("5000,red")
    printer.println("5000,green")
    printer.println("4000,blue")

    Thread.sleep(7000)
    printer.println("1000,yellow")
    printer.println("2000,cyan")
    printer.println("3000,magenta")
    printer.println("5000,black")

    Thread.sleep(3000)
    printer.println("10000,pink")
  }

  def example3() = {
    Thread.sleep(2000)
    printer.println("9000,blue")
    Thread.sleep(3000)
    printer.println("2000,green")
    printer.println("1000,blue")
    printer.println("8000,red")
    Thread.sleep(2000)
    printer.println("5000,red") // discarded
    printer.println("18000,blue")
    Thread.sleep(1000)
    printer.println("2000,green") // discarded
    Thread.sleep(2000)
    printer.println("30000,purple")
    printer.println("10000,green")
  }

  def main(args: Array[String]): Unit = {
    example3()
  }
}

