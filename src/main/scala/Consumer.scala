import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

object Consumer {

  private val conf = ConfigFactory.load()
  private val spark = SparkManager.fetchSparkSession()

  def main(args: Array[String]) {
    spark.sparkContext.setLogLevel("ERROR")
    readFromKafkaAndWriteToHDFS(conf, spark)
  }

  def readFromKafkaAndWriteToHDFS(conf: Config, spark: SparkSession) {
    case class output(key: String, value: String)
    val query = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getString("kafka.brokers"))
      .option("subscribe", conf.getString("kafka.topic"))
      .option("failOnDataLoss", false)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .repartition(50)
      .writeStream
      .format("parquet")
      .option("format", "append")
      .option("path", conf.getString("consumer.output"))
      .option("checkpointLocation", conf.getString("consumer.checkpoint"))
      .start()

    query.awaitTermination()
  }
}