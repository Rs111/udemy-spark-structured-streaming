package part3lowlevel

import java.io.{File, FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import common._

/*
  Discretized Streams
  - DStream is a never-ending sequence of RDDs
    - each batch is an RDD of one or more partitions
    - batches are triggered at the same time in the cluster (possible because nodes' clocks are synchronized with a network time protocol)
    - i.e. a batch is across executors; each batch is all the partition blocks across executors between batch intervals
      - e.g. first batch has 3 partitions; 2 in executor 1 and 1 in executor 2
    - T0=steam starts, T1= first batch, T2 second batch, [] are partition blocks that are added to stream
    - MX are the block intervals
    - batchInterval: interval betwen TX, TX+1 (basically a poll duration)
    - block interval: the interval inbetween which the partitions of the RDD will be created
      - i.e. how often do we take our data and create a new partition block within the same microbatch
      - batch interval must be a multiple of the block interval
    - as new data comes in we keep on getting more partitions and batches
    - the whole thing below is known as a DStream
     T0        T1          T2
     |          |          |
    ----------------------------------------------
    | []    []     []                             | executor 1
    ----------------------------------------------
    ----------------------------------------------
    | []           []    []                       | executor 2
    ----------------------------------------------
     |    |     |      |   |
     M0   M1    M2     M3  M4

   - Essentially a distributed collection of elements of the same type
    - functional operators (e.g. map, flatMap, reduce)
    - access to each RDD which constitutes the stream
    - some advanced operators
   - DStream needs to be coupled with a receiver to perform computations
    - one receiver per DStream
    - receiver does the following:
      - fetches data from the source
      - sends data to Spark engine for processing
      - creates blocks and trigger the time barriers above to create batches and partitions
    - receiver is managed by the StreamingContext on the driver
    - occupies one core on the machine! (very important to allocate enough cores on machines we run streams on)
      - e.g. local[2]
 */

object DStreams {

  val spark = SparkSession.builder()
    .appName("DStreams")
    .master("local[2]")
    .getOrCreate()

  /*
    Spark Streaming Context = entry point to the DStreams API
    - needs the spark context
    - a duration = batch interval; basically a poll duration
   */
  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  /*
    - define input sources by creating DStreams (can create one or more)
    - define transformations on DStreams
    - call an action on DStreams
    - start ALL computations with ssc.start()
      - no more computations can be added
    - await termination, or stop the computation
      - you cannot restart the ssc
   */

  def readFromSocket() = {
    // distributed never ending collection of strings
    val socketStream: DStream[String] = ssc.socketTextStream("localhost", 12345)

    // transformation = lazy
    val wordsStream: DStream[String] = socketStream.flatMap(line => line.split(" "))

    // action; note: actions not triggered until we do ssc.start()
    // wordsStream.print()
    wordsStream.saveAsTextFiles("src/main/resources/data/words/") // each folder = RDD = batch, each file = a partition of the RDD

    // all dstreams that we define as dependant on ssc
    ssc.start()
    ssc.awaitTermination()
  }

  def createNewFile() = {
    new Thread(() => {
      Thread.sleep(5000)

      val path = "src/main/resources/data/stocks"
      val dir = new File(path) // directory where I will store a new file
      val nFiles = dir.listFiles().length
      val newFile = new File(s"$path/newStocks$nFiles.csv")
      newFile.createNewFile()

      val writer = new FileWriter(newFile)
      writer.write(
        """
          |AAPL,Sep 1 2000,12.88
          |AAPL,Oct 1 2000,9.78
          |AAPL,Nov 1 2000,8.25
          |AAPL,Dec 1 2000,7.44
          |AAPL,Jan 1 2001,10.81
          |AAPL,Feb 1 2001,9.12
        """.stripMargin.trim)

      writer.close() // flush away whatever buffers it may have
    }).start()
  }

  def readFromFile() = {
    createNewFile() // operates on another thread

    // defined DStream
    val stocksFilePath = "src/main/resources/data/stocks"

    /*
      ssc.textFileStream monitors a directory for NEW FILES; won't read what's already there before you start app
      - creating `createNewFile` to help us test
     */
    val textStream: DStream[String] = ssc.textFileStream(stocksFilePath)

    // transformations
    val dateFormat = new SimpleDateFormat("MMM d yyyy")

    val stocksStream: DStream[Stock] = textStream.map { line =>
      val tokens = line.split(",")
      val company = tokens(0)
      val date = new Date(dateFormat.parse(tokens(1)).getTime)
      val price = tokens(2).toDouble

      Stock(company, date, price)
    }

    // action
    stocksStream.print()

    // start the computations
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readFromSocket()
  }
}
