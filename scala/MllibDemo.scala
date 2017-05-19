/**
  * Created by Talor Wu on 2017/5/19.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
object MllibDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MLlib Demos")
    val sc = new SparkContext(conf)
    //加载解析样例数据
    val data = sc.textFile(args(0))

    val parseData = data.map{line =>
      val parts = line.split(" ")
      //将向量转化为标记向量
      LabeledPoint(parts(0).toDouble,Vectors.dense(parts.tail.map(x => x.toDouble)))
    }
      //设置迭代次数
      val numIterations = 20
      //使用SVMWithSGD类训练模型，其中的优化方法是SGD
      val model = SVMWithSGD.train(parseData,numIterations)
      //使用训练好的模型进行分类预测
      val labelAndPreds = parseData.map { point =>
        val predition = model.predict(point.features)
        (point.label,predition)
      }
    //统计分类错误
    val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / parseData.count()
    println("trainErroe : "+trainErr)
  }
}
