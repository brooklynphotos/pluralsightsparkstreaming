package photos.brooklyn.pluralsight.spark

import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import org.apache.spark.streaming._

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
}
