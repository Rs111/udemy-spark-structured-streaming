package part2structuredstreaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import common._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

/*
  Spark Architecture
    - apps      : Streaming, ML, GraphX, Other libraries
    - High-level: DataFrames, Datasets, Spark SQL
    - Low Level: DStreams, RDDs, Distributed variables

   - Why Spark streaming? Data is not static; need continous big data processing
   - Stream processing:
    - once we have insights computed from data, then new incoming data will have us re-compute our data insights
    - no bound on when data is going to end
    - in process, stream + batc inter-operate:
      - incoming data joined with fixed data
      - output of streaming job periodically queried by a batch job
      - consistency between batch/streaming jobs
    - Pros of Streaming:
      - lower latency than batch
      - greater performance/efficiency (especially with incremental data)
    - Difficulties:
      - maintaining state and order for incoming data (will do this with event time processing)
      - exactly-once processing in the context of machine failures (overcome by fault tolerance, feature of spark)
      - responding to events at low latency
      - transactional data at runtime
      - updating business logic at runtime
      - last 3 are basically making sure offset doesn't get huge; efficient apps
    - Spark Streaming Design Principals
      -1. Declaritive API
        - write "what" needs to be computed, let the library/spark decide "how"
        - e.g. spark engine decides how the data is split, how the data travels between the nodes in cluster, etc
        - Alternative to declaritive: RaaT (record-at-a-time)
          - a set of APIs to process each incoming element as it arrives
          - this is a very low-level API but need to be careful of everything you do (e.g. maintaining state, resource usage is your responsibility)
          - due to the point above, libraries that went down this path had limited success (hard to develop with them)
      -2. Event time vs Processing time API
        - event time = when the event was produced
        - processing time =  when the event arrives
        - event time is critical: allows detection of late data points
      -3. Continous vs micro-batch execution
        - continous = include each data point as it arrives (lower latency)
        - micro-batch = wait for a few data points, process them all in the new result (higher throughput)
        - spark streaming currently operates on microbatches
        - supports continuous execution as an experimental feature
      -4. Low Level API (DStreams) vs High-Level API (Structured Streaming)
        - Structured Streaming
          - benefits
            - easier to develop
            - interoperate with other Spark APIs
            - benefits from the auto-optimizations that the spark engine already provides
          - lazily evaluated
          - transformations vs actions
          - input sources
            - kakfa, flume, distributed file systems, sockets
          - output sinks
            - distributed file systems (S3, HDFS), databases, kafka, testing sinks (e.g. consol memory)
          - streaming I/O modes (dictate how data will be written to the sink)
            - append = only add new records
            - update = modify records in place (note: if query has no aggregations, equivalent with append)
            - complete = re-write everything
            - note: not all queries and sinks support all output modes (e.g. aggregations and append mode)
          - Trigger: when new data is written to sink
            - default: write as soon as the current microbatch has been processed (i.e. as soon as new batch is ready)
            - once: write a single microbatch and stop
            - processing-time: look for new data at fixed intervals
            - continous (currently experimental); process each record individually
    Docs
      - executed on Spark SQL Engine (get optimizations)
      - provides end-to-end  exactly-once fault-tolerance guarantees through checkpointing and Write-Ahead logs
      - microbatch by default (100 mill latency); introduced continous processing (lowest 1 mill, at least-once guarantees)

 */

object StreamingDataFrames {

  val spark = SparkSession.builder()
    .appName("Our first streams")
    .master("local[2]") // allocate at least 2 threads for this local application
    .getOrCreate()

  // run this before running the function (or else get connection refused): nc -lk 12345
  // note: socket seems to retain data a little bit (e.g. if send data to socket, then wait 5 sec, then run spark app, it will get the msg)
  // i.e. spark attempts to read values in the buffer for the socket
  /* created a dataframe by connecting spark to a socket at localhost:12345,
    and started an action on the DF by outputing each line to the consol in outputmode append,
    and we've started a streaming query that will run until terminated
   */
  def readFromSocket() = {
    // reading strings from socket
    // reading a DF
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // can add transformations between reading-and-consuming DFs
    val shortLines: DataFrame = lines.filter(length(col("value")) <= 5)

    // tell between a static vs a streaming DF
    println(shortLines.isStreaming)

    // consuming a Streaming DF
    // spark creates a dependency graph of various transformations written above
    val query = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start() // need to start a streaming query; it's equivalent of an action
      // start: () => StreamingQuery
      // start command is ASYNC; if therefore if you don't put in awaitTermination command, program will end

    // query needs to be waited-for
    // wait for the stream to finish
    query.awaitTermination()
  }

//    def main(args: Array[String]): Unit = {
//      readFromSocket()
//    }

  // note: appending extra data to a file after run does not cause it to read extra value; it has already read that file
  // BUT: any new files will be read
  // e.g. if you add another file to stocks folder, spark will monitor that and add that to streaming dataframe
  def readFromFiles() = {
    val stocksDF: DataFrame = spark.readStream
      .format("csv")
      .option("header", "false") // none of these files have headers
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema) // as opposed to reading static DFs, on streaming DFs you must specify schema for file reader (by default)
      .load("src/main/resources/data/stocks")

    stocksDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

//      def main(args: Array[String]): Unit = {
//        readFromFiles()
//      }

  // how to operate with triggers; triggers determine when new data is appended or added to streamingDF
  def demoTriggers() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .option("kafka.ssl.keystore.location", null)
      .option("kafka.ssl.truststore.password", "")
      .option("kafka.ssl.truststore.location", "")
      .load()
        .select(col("values") + lit("s"))

    // write the lines DF at a certain trigger
    lines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(
        // every 2 seconds the "lines" dataframe is checked for new data; if it has any new data, execute transformations as a new batch
        // if dataframe/source doesn't have new data, it will simply wait for a new trigger (i.e. doesn't create a new batch)
        // Trigger.ProcessingTime(2.seconds) // every 2 seconds run the query

        // create a single batch from whatever data there is in the source and then terminate
        // Trigger.Once() // single batch, then terminate

        // spark will create a batch every 2 seconds regardless of whether source has data or not
        Trigger.Continuous(2.seconds) // experimental, every 2 seconds create a batch with whatever you have
      )
    //  .start()
    //  .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    demoTriggers()
  }
}
