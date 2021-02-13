import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

object Testt {

  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //自定义累加器的前提:
    val ordd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    val myAccumulatorV = new MyAccumulatorV2
    sc.register(myAccumulatorV)

    ordd.foreach(
      x => {
        println(x)
        myAccumulatorV.add(x.toString)
      }
    )

    println(myAccumulatorV.value)

  }

}

class MyAccumulatorV2 extends AccumulatorV2[String,util.ArrayList[String]] {

  private val strings = new util.ArrayList[String]

  override def isZero: Boolean = strings.isEmpty

  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = new MyAccumulatorV2

  override def reset(): Unit = strings.clear()

  override def add(v: String): Unit = strings.add(v)

  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = strings addAll(other.value)

  override def value: util.ArrayList[String] = strings
}
