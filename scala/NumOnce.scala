import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Talor Wu on 2017/5/17.
  */
object NumOnce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Count Once")
    val sc = new SparkContext(conf)
    val data = sc.textFile(args(0))
    val result = data.mapPartitions(iter =>{
      var tmp = 0
      println(tmp+"!!")
      while (iter.hasNext){
        tmp = tmp ^ iter.next().toInt
        println(tmp +"!!")
      }
      Seq((1,tmp)).iterator
    }).reduceByKey(_ ^ _).collect()
    println("num appear once is :" + result(0))
  }
}
