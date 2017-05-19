import org.apache.spark.{SparkConf, SparkContext}
import scala.util.control._
import scala.tools.ant.sabbus.Break

/**
  * Created by Talor Wu on 2017/5/16.
  */
object Median {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Median")
    val sc = new SparkContext(conf)
    //通过textfile读入的是字符串型，需进行转换
    val data = sc.textFile(args(0)).flatMap(x => x.split("\\s+")).map(x => x.toInt)
    //设置桶的宽度为4
    val mappeddata = data.map(x => (x/4,x)).sortByKey()
    //p_count为每个桶子中数的个数
    val p_count = data.map(x => (x/4,1)).reduceByKey(_ + _).sortByKey().collect()
    p_count.foreach(a => println(a._1,a._2))
    //sum_count是统计总的个数
    val sum_count = p_count.map(x => x._2).sum
    println(sum_count)
    var tmp = 0
    var index = 0
    var mid = (sum_count-1)/2
    val loop = new Breaks;
    loop.breakable {
      for (i <- 0 to p_count.length-1) {
        tmp = tmp + p_count(i)._2
        if (tmp >= mid+1) {
          index = i
          loop.break
        }

      }
    }
    println(mid+" "+index+" "+tmp)
    //中位数在桶中的偏移量
    var res = 0
    val offset =mid - (tmp - p_count(index)._2)
    println("offest is " + offset)
    //takeOrdered它默认可以将key从小到大排序后，获取rdd中的前n个元素
    val result =mappeddata.filter(x=>x._1==index).takeOrdered(offset+1)
    res = result(offset)._2
      //res = mappeddata.filter(nums => nums._1 == index).takeOrdered(offset)

    println("Median is "+ res)
  }
}
