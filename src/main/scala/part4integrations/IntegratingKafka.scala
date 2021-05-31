package part4integrations

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro.functions.from_avro
import common._
/*
  Sources
  - http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#overview
  - Kafka Int
    - https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    - https://docs.databricks.com/spark/latest/structured-streaming/kafka.html#using-ssl
 */

object IntegratingKafka {

  val spark = SparkSession.builder()
    .appName("Integrating Kafka")
    .master("local[2]")
    .getOrCreate()

  /* start kafka before running
    - cd to project directory on 3-4 terminals
    - run docker-compose up in one
    - in a second terminal, hit docker container ps to see running containers; should see rockthejvm-sparkstreaming-kafka container
    - docker exec -it rockthejvm-sparkstreaming-kafka bash
    - cd /opt.kafka_2.12-2.5.0
      - in bin folder, we can execute executables that we have installed on the docker container
    - bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic rockthejvm
    - bin/kafka-console-producer.sh --broker-list localhost:9092 --topic rockthejvm
      - this is the equivalent of openning the socket
    - can run spark app after taking these steps
   */
  def readFromKafka() = {
    val kafkaDF: DataFrame =
      spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092") // can pass in multiple servers
      .option("subscribe", "rockthejvm") // topics we want to subscribe to
      // more options available for how to read from topic
      // https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
      .load()

    kafkaDF
      // value initial comes out in binary; need to select it and cast as string so we see the actual string
      .select(col("topic"), expr("cast(value as string) as actualValue"))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

//    def main(args: Array[String]): Unit = {
//      readFromKafka()
//    }

  /*
    run docker exec -it rockthejvm-sparkstreaming-kafka bash
    cd /opt.kafka_2.12-2.5.0
    bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic rockthejvm
   */
  def writeToKafka() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    // need to transform it to a key, value DF
    val carsKafkaDF = carsDF.selectExpr("upper(Name) as key", "Name as value")

    carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      // checkpoints directory is useful because it marks the data that has already been sent to kafka (so that you don't re-send data in event the pipeline goes down)
      // therefore if you want to re-run application and re-send same data to kafka, you will need to delete the checkpoints directory (root of project)
      .option("checkpointLocation", "checkpoints") // without checkpoints the writing to Kafka will fail
      .start()
      .awaitTermination()
  }

//    def main(args: Array[String]): Unit = {
//      writeToKafka()
//    }
  /**
    * Exercise: write the whole cars data structures to Kafka as JSON.
    * Use struct columns an the to_json function.
    */
  def writeCarsToKafka() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsJsonKafkaDF = carsDF.select(
      col("Name").as("key"),
      to_json(struct(col("Name"), col("Horsepower"), col("Origin"))).cast("String").as("value")
    )

    carsJsonKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeCarsToKafka()
  }
}
