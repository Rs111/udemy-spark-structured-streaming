package part4integrations

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object IntegratingKafkaDStreams {

  val spark = SparkSession.builder()
    .appName("Spark DStreams + Kafka")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  val kafkaParams: Map[String, Object] = Map(
    "bootstrap.servers" -> "localhost:9092",
    // expecting
    "key.serializer" -> classOf[StringSerializer], // send data to kafka; serialize bytes from spark to kafka
    "value.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer], // receiving data from kafka
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest", // when spark crashes, the offset from a given topic will reset to latest (if it can't find offset in kafka cluster)
    "enable.auto.commit" -> false.asInstanceOf[Object]
  )

  val kafkaTopic = "rockthejvm"

  def readFromKafka() = {
    val topics = Array(kafkaTopic)
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      /*
       LocationStrategies.PreferConsistent distributes the partitions evenly across the Spark cluster.
       Alternatives:
       - PreferBrokers if the brokers and executors are in the same cluster
       - PreferFixed
      */
      // type parameters are the type of the key and the type of the value in the kafka data
      // go hand-in-hand with serializers/deseriaizers; expecting values to be strings
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1"))
      /*
        Alternative
        - SubscribePattern allows subscribing to topics matching a pattern
        - Assign - advanced; allows specifying offsets and partitions per topic
       */
    )

    val processedStream = kafkaDStream.map(record => (record.key(), record.value()))
    processedStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  // read data from socket and write to kafka topic
  def writeToKafka() = {
    // DStream[String]
    val inputData = ssc.socketTextStream("localhost", 12345)

    // transform data
    val processedData = inputData.map(_.toUpperCase())

    processedData.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        // inside this lambda, the code is run by a single executor

        val kafkaHashMap = new util.HashMap[String, Object]()
        kafkaParams.foreach { pair =>
          kafkaHashMap.put(pair._1, pair._2)
        }

        // producer can insert records into the Kafka topics
        // available on this executor
        // note: the code block below MUST BE in the rdd.foreachPartition code block
        // i.e. why should we create a a producer for each partition? why can't we re-use? Answer is no
        // the rdd.foreachPartition lambda is run by a single executor, and the single executor would need to refer to the producer
        // if we had producer outside the code block, the driver program would need to send the producer to each executor (which is impossible)
        val producer = new KafkaProducer[String, String](kafkaHashMap)

        partition.foreach { value =>
          // cheap operation to create the simple data structure
          val message = new ProducerRecord[String, String](kafkaTopic, null, value)
          // feed the message into the Kafka topic
          producer.send(message)
        }
        // after we've sent all records, close the producer
        producer.close()
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeToKafka()
  }

}
