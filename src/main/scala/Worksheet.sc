import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark._
import org.apache.spark.SparkContext._

val conf = new SparkConf().setAppName("Worksheet").setMaster("local[2]")
//val sc = new SparkContext(conf)
val sc = SparkContext.getOrCreate()
val nrs = sc.parallelize(List(0,1,2,3,4,5,6,7,8,9,10))
val squares = nrs.map(i => i * i)
println(squares.collect().mkString(","))

