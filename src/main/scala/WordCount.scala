import org.apache.spark.{SparkContext, SparkConf}

object WordCount {
  val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
  val sc = new SparkContext(conf)

  val tf = sc.textFile("D:\\Programs\\Spark\\CHANGES.txt", 2).cache()

  def Count() = {
    val words = tf.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }

    //val counts = tf.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

    println(counts)
    //counts.saveAsTextFile("D:\\Programs\\Spark\\wordcount.txt")
    sc.stop()
  }

  def Count2() = {
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
    sc.stop()
  }

  def Count3() = {
    val counts = tf.flatMap(line => line.split(" ")).countByValue()
    counts.foreach{case (key, value) => println(value + " <- " + key)}

    sc.stop()
  }
}

