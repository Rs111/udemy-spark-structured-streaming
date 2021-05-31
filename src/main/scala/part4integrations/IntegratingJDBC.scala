package part4integrations

import org.apache.spark.sql.{Dataset, SparkSession}
import common._
/*
  - You can't read streams from JDBC
    - JDBC has transactions as their fundamental feature
    - You have to read the data all at once or not at all
  - You can't write to JDBC in a streaming fashion for the same reason
  - BUT you can write data in batches (using foreachBatch)
 */
object IntegratingJDBC {

  val spark = SparkSession.builder()
    .appName("Integrating JDBC")
    .master("local[2]")
    .getOrCreate()

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  import spark.implicits._

  def writeStreamToPostgres() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsDS = carsDF.as[Car]

    carsDS.writeStream
      // second is batchId; we don't need it
      .foreachBatch { (batch: Dataset[Car], _: Long) =>
        // each executor can control the batch
        // batch is a STATIC Dataset/DataFrame

        // need savemode to write it again( i.e. append, overwrite)
        batch.write
          .format("jdbc")
          .option("driver", driver)
          .option("url", url)
          .option("user", user)
          .option("password", password)
          .option("dbtable", "public.cars") // automatically creates table if it does not exist
          .save()
      }
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    writeStreamToPostgres()
  }
}
