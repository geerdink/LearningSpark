import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Learning Spark").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //WordCount.Count3(sc)
    //Pairs.Transform(sc)
    //SQL.Test(sc)
    //Streaming.Stream(sc)
    MachineLearning.Predict(sc)
    //Pi.Calculate(sc)
    //GraphX.Test()

    sc.stop()
  }
}
