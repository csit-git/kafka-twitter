import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder


object Producer {

  private val conf = ConfigFactory.load()
  private val spark = SparkManager.fetchSparkSession()

  import spark.implicits._

  def main(args: Array[String]) {
    spark.sparkContext.setLogLevel("ERROR")
    val ssc = SparkManager.fetchSparkStreamingContext(spark)
    val tweets = getTweets(ssc)
    val SGTweets = filterTweets(conf.getString("twitter.hashtag"), tweets)
    val twitterStream = SGTweets.map(toTweet)
    twitterStream.foreachRDD(rdd => if (!rdd.isEmpty()) sendToKafka(rdd))
    ssc.start()
    ssc.awaitTermination()
  }

  def toTweet(s: Status): Tweet = {
    Tweet(s.getCreatedAt.toString, s.getText)
  }

  def sendToKafka(tweetRdd: RDD[Tweet]) {
    val df = spark.createDataset(tweetRdd).toDF("key", "value")
    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getString("kafka.brokers"))
      .option("topic", conf.getString("kafka.topic"))
      .save()
  }

  def getTweets(context: StreamingContext): ReceiverInputDStream[Status] = {
    val twitterConf = new ConfigurationBuilder()
      .setOAuthConsumerKey(conf.getString("twitter.consumerKey"))
      .setOAuthConsumerSecret(conf.getString("twitter.consumerSecret"))
      .setOAuthAccessToken(conf.getString("twitter.accessToken"))
      .setOAuthAccessTokenSecret(conf.getString("twitter.accessTokenSecret"))
    val auth = new OAuthAuthorization(twitterConf.build)
    TwitterUtils.createStream(context, Some(auth))
  }

  def filterTweets(keyword: String, tweets: ReceiverInputDStream[Status]): DStream[Status] = {
    tweets.filter(_.getText.contains(keyword))
  }

}