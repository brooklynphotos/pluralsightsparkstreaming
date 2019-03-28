package photos.brooklyn.pluralsight.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.streaming.{Seconds, StreamingContext, _}

import scala.collection.mutable

object SparkShellRun {
  val sc: SparkContext = SparkContext.getOrCreate()

  def examineQueue(): Unit ={
    val ssc = new StreamingContext(sc, Seconds(5))
    val rddStream = (1 to 10).map(x => sc.makeRDD(List(x%4)))
    val testStream = ssc.queueStream(mutable.Queue(rddStream: _*))
    testStream.print
    ssc.remember(Seconds(60)) // remember last 60 seconds so they don't automatically get deleted, trailing input needed for interactive mode here
    val curMillis = System.currentTimeMillis()
    val startTime = Time(curMillis - (curMillis % 5000))
    ssc.start
    // wait for at least three things have appeared
    val slicedRDDs = testStream.slice(startTime, startTime + Seconds(10))
    // ascertains that we have the rdds captured during these past 10 seconds
    slicedRDDs.foreach(rdd => rdd.foreach(println))
    // stop
    ssc.stop()
  }

  def examineQueueShorter(oneAtATime: Boolean): Unit ={
    // shorter at 2 sec
    val ssc = new StreamingContext(sc, Seconds(2))
    val rddStream = (1 to 10).map(x => sc.makeRDD(List(x%4)))
    val testStream = ssc.queueStream(mutable.Queue(rddStream: _*), oneAtATime)
    // count by batch if oneAtATime, or count all in the batch
    testStream.countByValue().print
    ssc.start
    // stop after some observations
    ssc.stop()
  }

  def viewOtherStreamApi(): Unit = {
    val ssc = new StreamingContext(sc, Seconds(5))
    val rddStream = (1 to 10) map (x=>sc.makeRDD(List(x % 4)))
    val q = mutable.Queue(rddStream: _*)
    // creating a stream mostly for testing or learning how a stream api works
    val testStream = ssc.queueStream(q)
    testStream.print
    ssc.remember(Seconds(60))
    val currMillis = System.currentTimeMillis
    val startTime = Time(currMillis - (currMillis % 5000))
    ssc.start()
    // wait for at least three batches have run
    // a slice of the results during first 10 seconds
    // only works if we are still running and the RDDs are still in memory
    val slicedRDDs = testStream.slice(startTime, startTime + Seconds(10))
    slicedRDDs.foreach(rdd=>rdd.foreach(println))
    // stop the streaming without stopping the context
    ssc.stop(false)
  }

  def countBatch(): Unit = {
    val ssc = new StreamingContext(sc, Seconds(2))
    val rddStream = (1 to 10) map (x=>sc.makeRDD(List(x % 4)))
    val testStream = ssc.queueStream(mutable.Queue(rddStream: _*))
//    val testStream = ssc.queueStream(mutable.Queue(rddStream: _*), false) setting false will print the stuff out in one shot
//    testStream.countByValue().print this will show also the batch id
    testStream.count().print
    ssc.start
  }

  def structuredWindowedStreaming(spark: SparkSession): Unit = {
    import org.apache.spark.sql.functions.window
    import spark.implicits._

    val socketDS = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .option("includeTimestamp", true) // includes timestamp in the output
      .load
    val windowedCount = socketDS
      .withWatermark("timestamp", "10 seconds")
      .groupBy(window($"timestamp", "5 seconds"), $"value")
      .count
    val query = windowedCount
      .writeStream.format("console")
      .option("truncate", false)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      // append because we are using watermark
      .outputMode("append").start
  }

  def structuredStreaming(spark: SparkSession): Unit = {
    val socketDS = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load
    val socketCount = socketDS.groupBy().count
    // complete because we just want the aggregation
    val query = socketCount.writeStream.format("console").outputMode("complete").start

  }
}
