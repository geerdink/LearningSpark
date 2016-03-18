import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Learning Spark").setMaster("local[2]")
    val sc = new SparkContext(conf)

    var input = ""
    do {
      println("Enter app to run or 'quit' to exit:")
      input = scala.io.StdIn.readLine().toLowerCase()

      input match {
        case "streaming" => Streaming.Stream(sc)
        case "mllib" => MachineLearning.Predict(sc)
        case "pairs" => Pairs.Transform(sc)
        case "sql" => SQL.Test(sc)
        case "pi" => Pi.Calculate(sc)
        case "graphx" => GraphX.Test(sc)
        case _ => Nil
      }
    }
    while (input != "quit")

    sc.stop()
  }
}
