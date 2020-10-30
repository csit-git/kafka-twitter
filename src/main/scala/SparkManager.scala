import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkManager {

  def fetchSparkSession():SparkSession={
    SparkSession
      .builder
      .appName("KafkaTwitterManager")
      .getOrCreate()
  }

  def fetchSparkStreamingContext(sparkSession: SparkSession):StreamingContext={
    new StreamingContext(sparkSession.sparkContext, Seconds(10))
  }

}
