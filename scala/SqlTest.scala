import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by Talor Wu on 2017/5/18.
  */
object SqlTest {
  case class Person(name:String , age:Int)
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("SQL test")
//    val sc = new SparkContext(conf)

    //这是一个过去的版本
    //val sqlContext = new SQLContext(sc)
    //这是最新的版本，SparkSession集成了SQLContext和HiveContext
    val sparkSession = SparkSession.builder().appName("SQL test").enableHiveSupport().getOrCreate()
    //引入所有的sparkSession下的方法
    import sparkSession._
    import sparkSession.implicits._

      /*
      下面的people是含有case类型数据的RDD,默认由scala的implict机制将RDD转换为SchemaRDD,SchemaRDD是SparkSQL的核心RDD
       */
     val people = sparkSession.sparkContext.textFile(args(0)).map(_.split(",")).map(attributes => Person(attributes(0), attributes(1).trim.toInt))
     people.toDF().createOrReplaceTempView("people")
      //在内存的元数据中注册表信息，这样spark Sql表就创建完了
      //people.createOrReplaceTempView("people")
      val teenagers = sparkSession.sql("SELECT name FROM People WHERE age >= 13 and age <= 19")
      teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
      sparkSession.stop()

  }
}
