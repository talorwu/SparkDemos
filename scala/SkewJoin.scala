import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Talor Wu on 2017/5/17.
  */
object SkewJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Skew Join")
    val sc = new SparkContext(conf)
    val card = sc.textFile(args(0)).map(x => (x, x))
    val info = sc.textFile(args(1)).map { line =>
      val field = line.split("\t")
      val card = field(0)
      val mid = field(1)
      val count = field(2)
      (card, (mid, count))
    }

    // 对产生数据倾斜的表进行抽样（这里假设只会出现一个数据倾斜的key）
    val infoSample = info.sample(false, 0.1, 9).map(x => (x._1, 1)).reduceByKey(_ + _)
    // 获得产生数据倾斜的key
    val maxRowKey = infoSample.sortByKey(false).take(1).toSeq(0)._1

    // 将产生数据倾斜的表进行分割，切分成2张表，一个表存的是发生数据倾斜的key的数据，还有个表是除了该key的数据
    val skewedTable = info.filter(_._1 == maxRowKey)
    val mainTable = info.filter(_._1 != maxRowKey)

    val rs = sc.union(card.join(skewedTable), card.join(mainTable)).map(line => {
      (line._1, line._2._2._1, line._2._2._2)
    })
    rs.saveAsTextFile(args(2))
  }
}
