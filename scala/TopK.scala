import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._
import SparkContext._
import java.io.PrintWriter
import java.io.File
/**
  * Created by Talor Wu on 2017/5/16.
  */
object TopK {
  def main(args: Array[String]): Unit = {
    if (args.length < 3){
      System.err.print("Usage:<inoutFile>,<K>,<outputFile>")
    }
    val conf = new SparkConf().setAppName("TopK")
    val sc = new SparkContext(conf)
    val lins = sc.textFile(args(0),1)
    val words = lins.flatMap(line => line.split("\\s+"))
    val pairs = words.map(word =>(word,1))
    val wordCount = pairs.reduceByKey(_ + _)
    val convert = wordCount.map{case(key,value) => (value,key)}.sortByKey(false,1)

    val topk = convert.top(args(1).toInt)
    topk.foreach(a => System.out.println("(" + a._2 + "," + a._1 + ")"))
    val writer = new PrintWriter(new File(args(2)))
    topk.foreach(a => writer.println("(" + a._2 + "," + a._1 + ")"))

    writer.close()

  }
}
