import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{StreamingContext, Seconds}

object Streaming {
  val conf = new SparkConf().setAppName("Streaming").setMaster("local[2]")
  val sc = new SparkContext(conf)

  def Stream() = {
    val ssc = new StreamingContext(conf, Seconds(1)) // batch interval = 1 sec
    val lines = ssc.socketTextStream("localhost", 7777) // create a DStream
    val errorLines = lines.filter(_.contains("error"))
    errorLines.print()

    //val txt = ssc.textFileStream("~//data//test")
    //txt.countByWindow(10 seconds, 2 seconds)

    sc.stop()
  }
}