package photos.brooklyn.pluralsight.spark

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

import scala.util.Try

object StatefulDetection {
  import org.apache.spark.sql.functions._

  def main(args: Array[String]): Unit = {
    // spark setup
    val spark = SparkSession.builder
      .master("local[3]")
      .appName("Fraud Detector")
      .config("spark.driver.memory", "2g")
      .enableHiveSupport
      .getOrCreate()

    // preparing dummy data

    val checkPointPath = "file:///tmp/checkpoint"
    // all the stream creation logic in the lambda function
    val streamingContext = StreamingContext.getOrCreate(checkPointPath, () => {
      val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
      ssc.checkpoint(checkPointPath)
      val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "transaction-group", Map("transactions" -> 1))
      transform(kafkaStream).print
      ssc
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def transform(kafkaStream: ReceiverInputDStream[(String, String)]): ReceiverInputDStream[_]= {
    kafkaStream.map(keyVal => tryConversionToSimpleTransaction(keyVal._2))
      .flatMap(_.right.toOption)
      .map(simpleTx => (simpleTx.account_number, simpleTx))
      .mapValues(tx => List(tx))
      // window and slide duration
      .reduceByKeyAndWindow((txs, otherTxs) => txs ++ otherTxs, (txs, oldTxs) => txs diff oldTxs, Seconds(10), Seconds(10))
      .mapWithState(
        StateSpec.function((accNum: String, newTxsOpt: Option[List[SimpleTransaction]], aggData: State[AggregateData]) => {

        }))
  }


  def tryConversionToSimpleTransaction(logLine: String): Either[UnparsableTransaction, SimpleTransaction] = {
    import java.text.SimpleDateFormat
    import scala.util.control.NonFatal
    logLine.split(',') match {
      case Array(id, date, acctNum, amt, desc) =>
        var parsedId: Option[Long] = None
        try {
          parsedId = Some(id.toLong)
          Right(SimpleTransaction(parsedId.get, acctNum, amt.toDouble,
            new java.sql.Date((new SimpleDateFormat("MM/dd/yyyy")).parse(date).getTime), desc))
        } catch {
          case NonFatal(exception) => Left(UnparsableTransaction(parsedId, logLine, exception))
        }
      case _ => Left(UnparsableTransaction(None, logLine,
        new Exception("Log split on comma does not result in a 5 element array.")))
    }

  }

  def transformData(spark: SparkSession): Unit ={
    import spark.implicits._
    val dataPath = "/Users/gzhong/projects/pluralsight/sparkStreaming/06/module 5/section 9/FraudDetector/Data/finances-small.json"
    // might have to add file:// before
    val financesDS = spark.read.json(dataPath)
      .withColumn("date", to_date(unix_timestamp($"Date", "MM/dd/yyyy")
        .cast("timestamp"))).as[Transaction]

    val accountNumberPrevious4WindowSpec = Window.partitionBy($"AccountNumber")
      .orderBy($"Date").rowsBetween(-4,0)
    val rollingAvgForPrevious4PerAccount = avg($"Amount").over(accountNumberPrevious4WindowSpec)

    financesDS
      .na.drop("all", Seq("ID","Account","Amount","Description","Date"))
      .na.fill("Unknown", Seq("Description")).as[Transaction]
      //.filter(tx=>(tx.amount != 0 || tx.description == "Unknown"))
      .where($"Amount" =!= 0 || $"Description" === "Unknown")
      .select($"Account.Number".as("AccountNumber").as[String], $"Amount".as[Double],
        $"Date".as[java.sql.Date](Encoders.DATE), $"Description".as[String])
      .withColumn("RollingAverage", rollingAvgForPrevious4PerAccount)
      .write.mode(SaveMode.Overwrite).parquet("Output/finances-small")

    if(Try(financesDS("_corrupt_record")).isSuccess) {
      financesDS.where($"_corrupt_record".isNotNull).select($"_corrupt_record")
        .write.mode(SaveMode.Overwrite).text("Output/corrupt_finances")
    }

    financesDS
      .map(tx => (s"${tx.account.firstName} ${tx.account.lastName}", tx.account.number))
      .distinct
      .toDF("FullName", "AccountNumber")
      .coalesce(5)
      .write.mode(SaveMode.Overwrite).json("Output/finances-small-accounts")

    financesDS
      .select($"account.number".as("accountNumber").as[String], $"amount".as[Double],
        $"description".as[String],
        $"date".as[java.sql.Date](Encoders.DATE)).as[TransactionForAverage]
      .groupBy($"AccountNumber")
      .agg(avg($"Amount").as("average_transaction"), sum($"Amount").as("total_transactions"),
        count($"Amount").as("number_of_transactions"), max($"Amount").as("max_transaction"),
        min($"Amount").as("min_transaction"), stddev($"Amount").as("standard_deviation_amount"),
        collect_set($"Description").as("unique_transaction_descriptions"))
      .withColumnRenamed("accountNumber", "account_number")
      .coalesce(5)
      .write
      .mode(SaveMode.Overwrite)
      .json("account_aggregates")

  }
}

case class TransactionForAverage(accountNumber: String, amount: Double, description: String, date: java.sql.Date)

case class UnparsableTransaction(id: Option[Long], originalMessage: String, exception: Throwable)
case class SimpleTransaction(id: Long, account_number: String, amount: Double,
                             date: java.sql.Date, description: String)

case class AggregateData(totalSpending: Double, numTx: Int, windowSpendingAvg: Double) {
  val averageTx = if(numTx > 0) totalSpending / numTx else 0
}
case class Account(number: String, firstName: String, lastName: String)
case class Transaction(id: Long, account: Account, date: java.sql.Date, amount: Double,  description: String)

