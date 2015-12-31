import org.apache.spark.{SparkContext, SparkConf}

object WordCount {
  def Count(sc: SparkContext) = {
    val tf = sc.textFile("D:\\Programs\\Spark\\CHANGES.txt", 2).cache()

    val words = tf.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }

    //val counts = tf.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

    println(counts)
    //counts.saveAsTextFile("D:\\Programs\\Spark\\wordcount.txt")
  }

  def Count2(sc: SparkContext) = {
    val tf = sc.textFile("D:\\Programs\\Spark\\CHANGES.txt", 2).cache()

    var input = ""
    do {
      println("Enter search term or 'quit' to exit:")
      input = scala.io.StdIn.readLine()

      if (input != "quit") {
        val start = java.util.Calendar.getInstance().getTimeInMillis
        val linesWithCopyright = tf.filter(line => line.contains(input)).count()
        val end = java.util.Calendar.getInstance().getTimeInMillis

        println("There are %s lines that contain the word '%s.' Processing took %s milliseconds.".format(linesWithCopyright, input, end - start))
      }

    } while (input != "quit")

    println("Quit...")
  }

  def Count3(sc: SparkContext) = {
    val tf = sc.textFile("D:\\Programs\\Spark\\CHANGES.txt", 2).cache()

    val counts = tf.flatMap(line => line.split(" ")).countByValue()
    counts.foreach{case (key, value) => println(value + " <- " + key)}
  }
}

