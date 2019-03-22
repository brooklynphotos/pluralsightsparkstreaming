package photos.brooklyn.pluralsight.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object FraudDetection {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[3]")
      .appName("Fraud Detector")
      .config("spark.driver.memory", "2g")
      .enableHiveSupport
      .getOrCreate()

    // 1 second batch duration
    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(1))
    // this is the kafka connection
    val kafkaStream = KafkaUtils.createStream(streamingContext, "localhost:2181",
      "test-group", Map("test"->1))

    kafkaStream.print
    streamingContext.start
    streamingContext.awaitTermination
  }
}
