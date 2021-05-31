package part4integrations

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.{Dataset, ForeachWriter, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra._ // cassandra connector library
import common._

/*
 - integrate Cassandra NoSQL distributed store
 - TWO WAYs to write to cassandra below
  - 1. write to cassandra is same idea as postgres write; write in batches of static DFs
  - 2. more optimized way
 */

object IntegratingCassandra {

  val spark = SparkSession.builder()
    .appName("Integrating Cassandra")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  /*
    Before running this
    - start up cassandra container
    - run cql.sh to go into container
    - run commands in the prompt
      - create keyspace public with replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 };
      - create table public.cars("Name" text primary key, "Horsepower" int);
        - Schema must have exact same name as column from Cars dataset
      - select * from public.cars
    - can run app after this is there
    - truncate public.cars (after you're done)
   */
  def writeStreamToCassandraInBatches() = {
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS
      .writeStream
      // _ is batchId; not used
      .foreachBatch { (batch: Dataset[Car], _: Long) =>
        // save this batch to Cassandra in a single table write
        batch
          .select(col("Name"), col("Horsepower"))
          .write
          .cassandraFormat("cars", "public") // type enrichment
          .mode(SaveMode.Append)
          .save()
      }
      .start()
      .awaitTermination()
  }

  // ForEachWrite is an abstract class with some unimplemented methods
  class CarCassandraForeachWriter extends ForeachWriter[Car] {

    /*
      - on every batch, on every partition `partitionId`:
        - on every "epoch" (chunk of data):
          - call the open method; if false, skip this chunk
          - for each entry in this chunk, call the process method
            - insert records into cassandra on a record-by-record basis
          - call the close method either at the end of the chunk or with an error if it was thrown
     */

    val keyspace = "public"
    val table = "cars"
    // need to import from sbt
    val connector = CassandraConnector(spark.sparkContext.getConf)

    // don't need to do anything; connection is opened by cassandra connecter
    override def open(partitionId: Long, epochId: Long): Boolean = {
      println("Open connection")
      true
    }

    // process record
    override def process(car: Car): Unit = {
      connector.withSessionDo { session =>
        session.execute(
          s"""
             |insert into $keyspace.$table("Name", "Horsepower")
             |values ('${car.Name}', ${car.Horsepower.orNull})
           """.stripMargin)
      }
    }

    // this close method is used for sanitizing resources if error was thrown; we don't need to do anything
    override def close(errorOrNull: Throwable): Unit = println("Closing connection")

  }

  def writeStreamToCassandra() = {
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS
      .writeStream
      // this foreach will take a `ForeachWriter[Car]` as arguement
      // ForEachWriter[Car] will have logic to write every single record to cassandra, but in a more optimized fashion
      .foreach(new CarCassandraForeachWriter)
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    writeStreamToCassandra()
  }
}
