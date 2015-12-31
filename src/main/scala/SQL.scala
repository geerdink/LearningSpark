import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

object SQL {
  def Test(sc: SparkContext) = {
    val hc = new HiveContext(sc)

    val input = hc.read.json("data//testweet.json")

    input.registerTempTable("Tweets")

    val topTweets = hc.sql("SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10")

    val topTweetsText = topTweets.rdd.map(row => row.getString(0))

    topTweetsText.foreach(println)

    input.printSchema()
  }
}
