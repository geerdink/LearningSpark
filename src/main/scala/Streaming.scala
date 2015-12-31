import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{StreamingContext, Seconds}

object Streaming {
  def Stream(sc: SparkContext) = {
    val conf = new SparkConf().setAppName("Streaming").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1)) // batch interval = 1 sec

    val lines = ssc.socketTextStream("localhost", 7777) // create a DStream
    val errorLines = lines.filter(_.contains("error"))
    errorLines.print()

    ssc.start()  // it's necessary to explicitly tell the StreamingContext to start receiving data
    ssc.awaitTermination()  // wait for the job to finish

    // To test the application by typing input lines:  nc localhost 7777

    //val txt = ssc.textFileStream("~//data//test")
    //txt.countByWindow(10 seconds, 2 seconds)

    ssc.stop(false, true)
  }
}
