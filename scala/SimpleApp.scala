/**
  * Created by Talor Wu on 2017/5/16.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "src/data/sample.txt"
    val sc = new SparkContext("local", "Simple App", "/path/to/spark-1.0.1-incubating",
      List("target/scala-2.10/simple-project_2.10-1.0.jar"))
    val logData = sc.textFile(logFile, 2).cache()
    val numTHEs = logData.filter(line => line.contains("the")).count()
    println("Lines with the: %s".format(numTHEs))
  }
}