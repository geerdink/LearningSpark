import org.apache.spark.{SparkContext, SparkConf}

object Pairs {
  def Transform(sc: SparkContext) = {
    val tf = sc.textFile("D:\\Programs\\Spark\\CHANGES.txt", 2).cache()

    val pairs = tf.map(line => (line.split(" ")(0), line))

    val shortLines = pairs.filter{case (key, value) => value.length < 20 || key.startsWith("S")}

    shortLines.take(50).foreach{case (key, value) => println(key + " -> " + value)}

    //pairs.mapValues(startsWith("B")).take(10).foreach(println)
  }
}
