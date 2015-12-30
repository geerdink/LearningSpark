import org.apache.spark.{SparkContext, SparkConf}

object Pairs {
  val conf = new SparkConf().setAppName("Pairs").setMaster("local[2]")
  val sc = new SparkContext(conf)


  val tf = sc.textFile("D:\\Programs\\Spark\\CHANGES.txt", 2).cache()

  def Transform() = {
    val pairs = tf.map(line => (line.split(" ")(0), line))

    val shortLines = pairs.filter{case (key, value) => value.length < 20 || key.startsWith("S")}

    shortLines.take(50).foreach{case (key, value) => println(key + " -> " + value)}

    //pairs.mapValues(startsWith("B")).take(10).foreach(println)
    sc.stop()
  }
}
